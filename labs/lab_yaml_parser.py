import yaml
import sys

with open('sample-files/doe-a-deer.yaml', 'r', newline='') as f:
    try:
        print(yaml.load(f))
    except yaml.YAMLError as e:
        print(e)
        sys.exit(1)
        