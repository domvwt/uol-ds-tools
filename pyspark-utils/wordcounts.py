#! /opt/spark/bin/pyspark
import re
from pathlib import Path


INPUT_TXT = "~/uol-ds-tools/pyspark-utils/frankenstein.txt"


myfile = Path(INPUT_TXT).expanduser().absolute()
rdd_txt = sc.textFile(f"file:///{myfile}")

# Simple word counts splitting on whitespace
counts = (
    rdd_txt.flatMap(lambda line: line.split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda a: (a[1], a[0]))
)

res1 = counts.collect()[:20]
for i in res1:
    print(i)
print()

# Word counts splitting on non word elements
word_counts = (
    rdd_txt.flatMap(lambda line: re.split(r"\W+", line))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda a: (a[1], a[0]))
)

res2 = word_counts.collect()[:20]
for i in res2:
    print(i)
print()
