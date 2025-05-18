import yaml

def read_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)
