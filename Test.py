# import yaml
# import os
# import yaml
# from pathlib import Path
# file_path = os.path.join( 'study_simple.yaml')
# print(f"Attempting to open file: {file_path}")
# with open(file_path, 'r') as f:
#     study_data = yaml.safe_load(f)

# script_parent_dir = Path(__file__).parent.parent
# file_path= os.path.join(f'{script_parent_dir}/t32-ref-case-dev', 'StoRIES_RefCase_Config_Simple.yaml')


# study_data= study_data

# Dict_param= {}
# for key1, value1 in study_data['study_param_range'].items():
#     if Dict_param.get(key1) is None:
#         Dict_param[key1]= value1
#     else:
#         Dict_param [key1].append(value1)
# Dict_runs= {}
# for key2, value2 in study_data['study_run_dicts'].items():
#     if isinstance(value2, dict):
#             if Dict_runs.get(key2) is None:
#                 Dict_runs[key2]= value2
#             else:
#                 Dict_runs [key2].append(value2)
#     else:
#         Dict_runs[key2]= value2

# Scenario_id= 0
# item_key= []
# for key, values in Dict_param.items():
#     if isinstance(values, dict):
#         for key1, value1 in values.items():
#             if isinstance(value1, list):
#                 for item in value1:
#                     item_key.append(key1)
#                     item_variable= key
# for keys in item_key:
#     item_key= keys   
#     for run_name, run_values in Dict_runs.items():
#         Scenario_id +=1
#         with open(file_path, 'r') as f:
#             config= yaml.safe_load(f)
#             config[item_variable][item_key]=item
#             for key2, value2 in run_values.items():
#                 if isinstance(value2, dict):
#                     for key3, value3 in value2.items():
#                         config[key2][key3]= value3
#                 else:
#                     config[key2]= value2
#             with open(f'scenario_{Scenario_id}.yaml', 'w') as f:
#                 yaml.safe_dump(config, f)
                        
from itertools import product

# Define the parameter ranges dynamically in a dictionary
study_param_range = {
    "CBD.Day": [13, 152, 244],  # Updateable list
    "CBD.Location": ["Soria", "Portici"],  # Updateable list
    "WG_PPMp.PNominal": [902.0, 1502.0, 222.0]  # Updateable list
}

# Define runs dynamically (can also be updated)
study_run_dicts = {
    "run_01": {"BAT_ESSm.PNominal": 902.0},
    "run_02": {"BAT_ESSm.PNominal": 1502.0},
    "run_03": {"BAT_ESSm.PNominal": 222.0}
}

# Extract keys and values from study_param_range for product
param_keys = list(study_param_range.keys())
param_values = list(study_param_range.values())

# Generate the combinations dynamically
scenarios = []
for run_name, run_values in study_run_dicts.items():
    for combination in product(*param_values):
        scenario = {
            "run_name": run_name,
            "run_values": run_values
        }
        # Add dynamic parameter combinations to the scenario
        scenario.update(dict(zip(param_keys, combination)))
        scenarios.append(scenario)

# Print the scenarios
for idx, scenario in enumerate(scenarios, start=1):
    print(f"Scenario {idx}: {scenario}")

# Total number of scenarios
print(f"Total scenarios generated: {len(scenarios)}")
