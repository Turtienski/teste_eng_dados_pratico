import yaml


def read_yml_config(file_path):
    with open(file_path) as file:

        config_list = yaml.load(file, Loader=yaml.FullLoader)

    return config_list
