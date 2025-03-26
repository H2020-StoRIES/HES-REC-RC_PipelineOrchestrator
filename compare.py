import yaml

# File paths
file1 = r"C:\Users\szata\Codes\StoriesTeams\log_data\Study_1742915299\scenario_run_04_1.yaml"
file2 = r"C:\Users\szata\Codes\StoriesTeams\t32-ref-case-dev\TP_Soria_CtrlSyst_day_0514_V1.yaml"

# Load YAML files
def load_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

data1 = load_yaml(file1)
data2 = load_yaml(file2)

# Recursive comparison of keys and values
def compare_dicts(dict1, dict2, path=""):
    differences = []
    keys = set(dict1.keys()).union(set(dict2.keys()))
    for key in keys:
        new_path = f"{path}/{key}" if path else key
        if key not in dict1:
            differences.append(f"Key '{new_path}' missing in File 1")
        elif key not in dict2:
            differences.append(f"Key '{new_path}' missing in File 2")
        else:
            if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                differences.extend(compare_dicts(dict1[key], dict2[key], new_path))
            elif dict1[key] != dict2[key]:
                differences.append(f"Value mismatch at '{new_path}': File 1 = {dict1[key]}, File 2 = {dict2[key]}")
    return differences

# Compare the two YAML files
differences = compare_dicts(data1, data2)

# Print differences
if differences:
    print("Differences found:")
    for diff in differences:
        print(diff)
else:
    print("The files are identical.")