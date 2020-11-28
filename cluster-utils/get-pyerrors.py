#!/usr/bin/python
with open("nice_logs.txt") as f:
    logs = f.read()

pyerrors = logs.split("Traceback (most recent call last):", 1)[1].split("End of LogType:stderr", 1)[0]
print(pyerrors)
