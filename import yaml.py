import yaml
import yaml

# Load the scenario_run_01_1.yaml file
with open(r'C:/Users/szata/Codes/StoriesTeams/log_data/Study_1737032655/scenario_4.yaml', 'r') as file:
    scenario_data = yaml.safe_load(file)

# Load the dev/StoRIES_RefCase_Config_rev04.yaml file
with open(r'C:/Users/szata/Codes/StoriesTeams/log_data/Study_1737032655/test_bookChap_config.yaml', 'r') as file:
    refcase_data = yaml.safe_load(file)
# Compare the data
def compare_dicts(dict1, dict2, path=""):
    for key in dict1:
        if key in dict2:
            if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                compare_dicts(dict1[key], dict2[key], path + "." + key if path else key)
            elif dict1[key] != dict2[key]:
                print(f"Difference at {path + '.' + key if path else key}: {dict1[key]} != \n{dict2[key]}\n")
                
                
        else:
            print(f"Key {path + '.' + key if path else key} not found in second dictionary")
    for key in dict2:
        if key not in dict1:
            print(f"Key {path + '.' + key if path else key} not found in first dictionary")

compare_dicts(scenario_data, refcase_data)