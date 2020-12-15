with open("logs.txt", "r") as f:
    mylogs = f.read()

app_id = mylogs.split("Submitted application ")[1].split("\n")[0].strip()
print(app_id)
