import yaml

def read_yaml(filename):
    with open(f'{filename}.yaml','r') as f:
        output = yaml.safe_load(f)
    return output
    
def update_conf_run(conf_run, configurable_values):
    for key, value in configurable_values.items():
        if isinstance(value, dict):
            update_conf_run(conf_run[key], value)
        else:
            if value not in ['default_value', 'default_name', 'default_list']:
                conf_run[key] = value
                
if __name__ == "__main__":
    Config_base= read_yaml('config_example')
    Study= read_yaml('study')
    for run_key, configurable_values in Study.items():
        if run_key.startswith('run_'):
            conf_run = Config_base.copy()
            update_conf_run(conf_run, configurable_values)
            with open(f'conf_{run_key}.yaml', 'w') as f:
                yaml.safe_dump(conf_run, f)