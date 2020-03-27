import json

with open('sample-files/doe-a-deer.json') as f:
    data = json.load(f)
    print(data)