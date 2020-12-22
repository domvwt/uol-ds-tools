#!/usr/bin/python3
import argparse
import configparser
import subprocess
import getpass
import sys


from datetime import datetime
from pathlib import Path
from pprint import pformat


_USER = getpass.getuser()


class RunConfig:
    def __init__(self, distance_metric, t1, t2, k_list):
        self.distance_metric = distance_metric
        self.t1 = t1
        self.t2 = t2
        self.k_list = k_list

    def __str__(self):
        return pformat(vars(self))

    def __repr__(self):
        return str(vars(self))


def run_command(label, command, interact):
    print(f"Running {label}...")
    print(f"Command:\n{command}")
    result = subprocess.run(
        command.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    output = str(result.stdout, encoding="utf-8")
    if interact and result.returncode:
        input(
            f"{output}\nCommand failed: {label}\nPress enter to continue or ctrl+c to abort.\n"
        )
    elif output:
        print(output)


def is_hdfs_file(filepath):
    result = subprocess.run(f"hdfs dfs -ls {filepath}/".split(), capture_output=True,)
    return not bool(result.returncode)


def make_directory(input_name):
    return f"hdfs dfs -mkdir {input_name}"


def copy_to_hdfs(input_name, input_dir):
    return f"hdfs dfs -put -f {input_dir} {input_name}/raw"


def make_seqfiles(input_name):
    return f"mahout seqdirectory -i {input_name}/raw -o {input_name}/seqfiles -c UTF-8 -chunk 5"


def make_sparse_vecs(input_name):
    return f"mahout seq2sparse -nv -i {input_name}/seqfiles -o {input_name}/vectors"


def clean_hdfs(input_name):
    return f"hdfs dfs -rm -r {input_name}"


def get_final_clusters(kmeans_output):
    result = subprocess.run(
        f"hdfs dfs -ls {kmeans_output}".split(), capture_output=True,
    )
    return str(result.stdout.split()[-1], encoding="utf-8")


def get_centroids(input_name, distance_metric, t1, t2, output):
    return f"""\
mahout canopy -i {input_name}/vectors/tfidf-vectors -ow \
-o {output} \
-dm org.apache.mahout.common.distance.{distance_metric} \
-t1 {t1} -t2 {t2}
"""


def run_kmeans_canopy(input_name, distance_metric, output):
    final_clusters = get_final_clusters(
        f"{input_name}/canopy-centroids-{distance_metric}"
    )
    return f"""\
mahout kmeans -i {input_name}/vectors/tfidf-vectors \
-c {final_clusters} \
-o hdfs://lena/user/{_USER}/{output} \
-dm org.apache.mahout.common.distance.{distance_metric} \
-cl -cd 0.1 -ow -x 20
"""


def run_kmeans(input_name, k, distance_metric, output):
    return f"""\
mahout kmeans -i {input_name}/vectors/tfidf-vectors \
-o hdfs://lena/user/{_USER}/{output} \
-dm org.apache.mahout.common.distance.{distance_metric} \
-cl -cd 0.1 -ow -x 20 -k {k}
"""


def get_cluster_info(
    input_name, k, distance_metric, kmeans_output, output_dir, output_format
):
    ext = output_format.replace("_", "").lower()
    ext = "txt" if ext == "text" else ext
    final_clusters = get_final_clusters(kmeans_output)
    return f"""\
mahout clusterdump \
-i {final_clusters} \
-d {input_name}/vectors/dictionary.file-0 \
-dt sequencefile \
-b 100 \
-n 20 \
-p {kmeans_output}/clusteredPoints \
-o {output_dir}/{input_name}-eval-{ext}/clusters-{k}-{distance_metric}.{ext} \
-dm org.apache.mahout.common.distance.{distance_metric} \
-of {output_format} \
--evaluate
"""


def main():
    time_start = datetime.now()

    argparser = argparse.ArgumentParser(
        description="Automatically run and manage mahout kmeans with simple user configuration files."
    )
    argparser.add_argument("conf", help="config file for choosing kmeans parameters")
    argparser.add_argument(
        "--force-upload",
        action="store_true",
        help="rerun and overwrite raw files that already exist",
    )
    argparser.add_argument(
        "--force-centroids",
        action="store_true",
        help="rerun and overwrite centroid files that already exist",
    )
    argparser.add_argument(
        "--force-canopy-eval",
        action="store_true",
        help="rerun and overwrite canopy cluster evaluation files that already exist",
    )
    argparser.add_argument(
        "--force-kmeans",
        action="store_true",
        help="rerun and overwrite cluster files that already exist",
    )
    argparser.add_argument(
        "--force-eval",
        action="store_true",
        help="rerun and overwrite evaluation files that already exist",
    )
    argparser.add_argument(
        "-i",
        "--interactive",
        dest="interact",
        action="store_true",
        help="user prompt on task failure",
    )

    try:
        args = argparser.parse_args()
    except:
        argparser.print_help()
        sys.exit(0)

    print(f"Start time: {time_start}")
    config = configparser.ConfigParser()

    with open(args.conf, "r") as f:
        config.read_file(f)

    run_configs = []

    for key in config.sections():
        rc = RunConfig(
            distance_metric=config[key].get("distance_metric").strip(),
            t1=config[key].get("t1"),
            t2=config[key].get("t2"),
            k_list=[x.strip() for x in config[key].get("k_list").split(",")],
        )
        run_configs.append(rc)

    input_dir = config["DEFAULT"]["input_dir"].strip()
    input_name = input_dir.split("/")[-1]

    output_dir = config["DEFAULT"]["output_dir"].strip()

    if not is_hdfs_file(f"{input_name}/vectors") or args.force_upload:
        run_command("clean_hdfs", clean_hdfs(input_name), False)
        run_command("make_directory", make_directory(input_name), args.interact)
        run_command("copy_to_hdfs", copy_to_hdfs(input_name, input_dir), args.interact)
        run_command("make_seqfiles", make_seqfiles(input_name), args.interact)
        run_command("make_sparse_vecs", make_sparse_vecs(input_name), args.interact)

    for conf in run_configs:
        centroid_output = f"{input_name}/canopy-centroids-{conf.distance_metric}"
        canopy_output = f"{input_name}/kmeans-clusters-canopy-{conf.distance_metric}"
        if not is_hdfs_file(centroid_output) or args.force_centroids:
            run_command(
                f"get_centroids, dm: {conf.distance_metric}",
                get_centroids(
                    input_name, conf.distance_metric, conf.t1, conf.t2, centroid_output
                ),
                args.interact,
            )
        if not is_hdfs_file(canopy_output) or args.force_centroids:
            run_command(
                f"run_kmeans_canopy, dm: {conf.distance_metric}",
                run_kmeans_canopy(input_name, conf.distance_metric, canopy_output),
                args.interact,
            )
        canopy_eval_output_csv = Path(
            f"{output_dir}/{input_name}-eval-csv/clusters-canopy-{conf.distance_metric}.csv"
        )
        if not canopy_eval_output_csv.is_file() or args.force_canopy_eval:
            canopy_eval_output_csv.parent.mkdir(parents=True, exist_ok=True)
            run_command(
                f"get_cluster_info: csv, dm: {conf.distance_metric}, k: canopy",
                get_cluster_info(
                    input_name,
                    "canopy",
                    conf.distance_metric,
                    canopy_output,
                    output_dir,
                    "CSV",
                ),
                args.interact,
            )
        canopy_eval_output_txt = Path(
            f"{output_dir}/{input_name}-eval-txt/clusters-canopy-{conf.distance_metric}.txt"
        )
        if not canopy_eval_output_txt.is_file() or args.force_canopy_eval:
            canopy_eval_output_txt.parent.mkdir(parents=True, exist_ok=True)
            run_command(
                f"get_cluster_info: txt, dm: {conf.distance_metric}, k: canopy",
                get_cluster_info(
                    input_name,
                    "canopy",
                    conf.distance_metric,
                    canopy_output,
                    output_dir,
                    "TEXT",
                ),
                args.interact,
            )

        for k in conf.k_list:
            kmeans_output = f"{input_name}/kmeans-clusters-k-{k}-{conf.distance_metric}"
            if not is_hdfs_file(kmeans_output) or args.force_kmeans:
                run_command(
                    f"run_kmeans, dm: {conf.distance_metric}, k: {k}",
                    run_kmeans(input_name, k, conf.distance_metric, kmeans_output),
                    args.interact,
                )

            eval_output_txt = Path(
                f"{output_dir}/{input_name}-eval-txt/clusters-{k}-{conf.distance_metric}.txt"
            )
            if not eval_output_txt.is_file() or args.force_eval:
                eval_output_txt.parent.mkdir(parents=True, exist_ok=True)
                run_command(
                    f"get_cluster_info: txt, dm: {conf.distance_metric}, k: {k}",
                    get_cluster_info(
                        input_name,
                        k,
                        conf.distance_metric,
                        kmeans_output,
                        output_dir,
                        "TEXT",
                    ),
                    args.interact,
                )

            eval_output_csv = Path(
                f"{output_dir}/{input_name}-eval-csv/clusters-{k}-{conf.distance_metric}.csv"
            )
            if not eval_output_csv.is_file() or args.force_eval:
                eval_output_csv.parent.mkdir(parents=True, exist_ok=True)
                run_command(
                    f"get_cluster_info: csv, dm: {conf.distance_metric}, k: {k}",
                    get_cluster_info(
                        input_name,
                        k,
                        conf.distance_metric,
                        kmeans_output,
                        output_dir,
                        "CSV",
                    ),
                    args.interact,
                )

    time_end = datetime.now()
    time_elapsed = time_end - time_start

    print(f"End time: {time_end}")
    print(f"Total job time: {time_elapsed}")


if __name__ == "__main__":
    main()
