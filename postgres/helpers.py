import yaml

def read_yaml(file_path):
    cfg = None
    with open(file_path, 'r') as file:
        cfg = yaml.safe_load(file)
    return cfg