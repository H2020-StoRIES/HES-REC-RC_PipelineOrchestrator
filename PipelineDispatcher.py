import yaml
import subprocess
import os
from pathlib import Path
from time import time
from itertools import product
import json
import pandas as pd
import numpy as np
#Different from config:
# Cbue*100. Ask Marcos!
# WG and Cbu last data . but this code is correct
class PipelineDispatcher:
    def __init__(self, study_file_Nm):
        self.path_config= '../Config'
        self.study_file_Nm = study_file_Nm
        self.study_file = f'{self.path_config}/{study_file_Nm}.yaml'
        self.runs = []
        self.scenario_name = []
        self.path_simulation= '../t32-ref-case-dev'
        self.path_dispatcher= '../Pipeline-dispatcher'
        self.path_kpi_calculation= '../KPI_Evaluation/KPI_Evaluation_python'
        self.log_data = '../log_data'
        self.path_dispatch_optimisation= '../DispatchOptimisation'
        # self.INdir= f'{self.path_simulation}/test_bookChap_data/test_bookChap_config'
        self.INdir= self.path_config
    def xls_to_yaml(self):
        matlab_script = f"""
            clear all; restoredefaultpath %%%%%%%%%%%%%%%
            clearvars -except INstruct; restoredefaultpath
            cd('{self.path_simulation}');
            addpath(genpath('auxFunc'));
            t32_RefCase_ReadCfg_4xlscsv2yalm('{self.INfile}', '{self.path_config}', '{self.path_config}/{self.config_file_Nm}.yaml',1);
            """
        P1 = subprocess.Popen(
            ['matlab', '-batch', matlab_script], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            universal_newlines=True
        )

        stdout, stderr = P1.communicate()

        if P1.returncode == 0:
            print(stdout)
            print(f"Config file converted to YAML by MATLAB.")
            return True
        else:
            print(stderr)
            print("Error in xls_to_yaml conversion")
            return False

    def read_yaml(self, filename, path):
        file_path = os.path.join(path, f'{filename}.yaml')
        try:
            print(f"Attempting to open file: {file_path}")
            with open(file_path, 'r') as f:
                output = yaml.safe_load(f)
            return output
        except FileNotFoundError as e:
            print(f"File not found: {file_path}")
            raise e
    
    def load_study(self):
        self.study_data = self.read_yaml(self.study_file_Nm, self.path_config)


    def generate_combinations(self, param_ranges):
    # Flatten the nested dictionary into a flat dictionary with hierarchical keys
        flat_params = {}
        for outer_key, inner_dict in param_ranges.items():
            for inner_key, values in inner_dict.items():
                flat_params[f"{outer_key}.{inner_key}"] = values

        # Generate Cartesian product of all parameter value lists
        param_keys = list(flat_params.keys())
        param_values = list(flat_params.values())
        combinations = list(product(*param_values))

        # Convert combinations into a list of dictionaries with hierarchical keys
        scenarios = [dict(zip(param_keys, combo)) for combo in combinations]

        return scenarios
    
    def Read_data_from_csv (self, file_path, data_type, Day, interval, factor):
        H= 3600
        T= 24
        with open(f'{self.INdir}/{file_path}.csv', mode='r', newline='') as file:
            csv_reader = pd.read_csv(file, header=None)
            r = []
            if data_type =='Diary' or data_type== 'Short profile':
                day=1
            elif data_type== 'Anual':
                day= Day

            if data_type== 'Short profile':
                interval =interval
                for i in range(T):
                        id= i+(day-1)*T
                        period= H/interval
                        # Resample the interval-second dataset to 3600-second intervals by mean values, filling missing values with 0
                        df = csv_reader.loc[(csv_reader.iloc[:,0] >= id * H) & (csv_reader.iloc[:,0]  < (id + 1) * H)].fillna(0)
                        r.append([i * H, factor* float(df.iloc[:, 1].sum() / period)])

            else:
                interval =interval  
                for i in range(T):
                    id= i+(day-1)*T
                    period= H/interval
                    row = csv_reader.loc[(csv_reader.iloc[:,0] >= id * H) & (csv_reader.iloc[:,0]  < (id + 1) * H)].fillna(0)
                    r.append([i * H, factor*float(row.iloc[:, 1].sum() / period)])


        
        return r
    def replace_strings_with_csv_columns(self, yaml_content, outer_key):
        for key, value in yaml_content.items():
            if key == 'epzProfile_val':
                data_type= 'Anual'
                Day= self.config_copy ['CBD']['Day']
                factor= 1
                interval = 3600
                CSV_file= f'TP_{self.config_copy['CBD']['Location']}_elePrizes_2022_SZT'
                day=Day
                T=24
                H=3600
                r= []
                csv_reader = pd.read_csv(f'{self.INdir}/{CSV_file}.csv', header=None)
                for i in range(T):
                        id= i+(day-1)*T
                        period= H/interval
                        # Resample the interval-second dataset to 3600-second intervals by mean values, filling missing values with 0
                        df = csv_reader.loc[(csv_reader.iloc[:,0] >= id * H) & (csv_reader.iloc[:,0]  < (id + 1) * H)].fillna(0)
                        r.append([i * H, factor* float(df.iloc[:, 1].sum() / period), factor* float(df.iloc[:, 2].sum() / period)])
                self.config_copy[outer_key][key] = r
            if key == 'nuProfile_val':
                data_type= 'Anual'
                interval = 3600
                number1= self.config_copy[outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                number1="{:03d}".format(int(number1))
                number2= self.config_copy[outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                number2="{:03d}".format(int(number2))
                CSV_file= f'TP_{self.config_copy['CBD']['Location']}_RC_nu_{number1}_{number2}_SZT'
                Day= self.config_copy ['CBD']['Day']
                T=24
                H=3600
                r= []
                csv_reader = pd.read_csv(f'{self.INdir}/{CSV_file}.csv', header=None)
                for i in range(T):
                        id= i+(Day-1)*T
                        period= H/interval
                        factor= 1
                        # Resample the interval-second dataset to 3600-second intervals by mean values, filling missing values with 0
                        df = csv_reader.loc[(csv_reader.iloc[:,0] >= id * H) & (csv_reader.iloc[:,0]  < (id + 1) * H)].fillna(0)
                        r.append([i * H, factor* float(df.iloc[:, 1].sum() / period)])
                self.config_copy[outer_key][key] = r
            if key.endswith('P_baseThermalProfile_val'):
                factor= self.config_copy[outer_key]['PNominal'] /self.config_copy[outer_key]['PBase']
                if not isinstance( self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'], list):
                    number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                    number1="{:03d}".format(int(number1))
                    number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                    number2="{:03d}".format(int(number2))
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                    if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                        data_type= 'Anual'
                        Day= self.config_copy ['CBD']['Day']
                        if outer_key== 'CSP_MS_STPwtRK' or outer_key== 'TPS_MS_STPnoRK':
                            interval = 3600
                        else:
                            interval = 900
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day= Day, interval= interval, factor= factor)
                        
                        
                        self.config_copy[outer_key][key] =  [[abs(x) for x in sublist] for sublist in Data] #TODO: We used abs because th data sometimes is negative and we need csp to be always generating
                    elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                        data_type= 'Diary'
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day=1, interval= 3600, factor= factor)
                        
                        self.config_copy[outer_key][key] = Data
                    else:
                        print('Error')
                else:
                    Data1= []
                    for i in range(len(self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'])):
                        number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'][i]
                        number1="{:03d}".format(int(number1))
                        number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_'][i]
                        number2="{:03d}".format(int(number2))
                        CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                        if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                            data_type= 'Anual'
                            Day= self.config_copy ['CBD']['Day']
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval= 900, factor= factor)
                            
                            self.config_copy[outer_key][key] = Data
                        elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                            data_type= 'Diary'
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=1, interval= 3600, factor= factor)
                            
                            self.config_copy[outer_key][key] = Data
                        else:
                            print('Error')

                        if Data1== []:
                            Data1= Data
                        else:
                            Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                        self.config_copy[outer_key][key] = Data1
            if key.endswith('ElectricProfile_val'):
                factor= self.config_copy[outer_key]['PNominal'] /self.config_copy[outer_key]['PBase']
                #if there is just one component
                if not isinstance( self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'], list):
                    number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                    number1="{:03d}".format(int(number1))
                    number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                    number2="{:03d}".format(int(number2))
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseElectricProfile']}_{number1}_{number2}'
                    if self.config_copy [outer_key]['P_baseElectricProfileType']== 'Anual':
                        data_type= 'Anual'
                        Day= self.config_copy ['CBD']['Day']
                        if outer_key== 'CSP_MS_STPwtRK' or outer_key== 'TPS_MS_STPnoRK':
                            interval = 3600
                        else:
                            interval = 900
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day= Day, interval= interval, factor= factor)
                        
                        self.config_copy[outer_key][key] =  [[abs(x) for x in sublist] for sublist in Data]
                    elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Diary':
                        data_type= 'Diary'
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day=1, interval= 3600, factor= factor)
                        
                        self.config_copy[outer_key][key] = Data
                    elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Short profile':
                        print('Error: Short profile not allowed for ElectricProfile')
                    else:
                        print('Error')
                else:
                    #if there are multiple components
                    Data1 = []
                    for i in range(len(self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'])):
                        number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'][i]
                        number1="{:03d}".format(int(number1))
                        number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_'][i]
                        number2="{:03d}".format(int(number2))
                        CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseElectricProfile']}_{number1}_{number2}'
                        if self.config_copy [outer_key]['P_baseElectricProfileType']== 'Anual':
                            data_type= 'Anual'
                            Day= self.config_copy ['CBD']['Day']
                            if outer_key== 'CSP_MS_STPwtRK' or outer_key== 'TPS_MS_STPnoRK':
                                interval = 3600
                            else:
                                interval = 900
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval= interval, factor= factor)
                            if Data1== []:
                                Data1=  [[abs(x) for x in sublist] for sublist in Data]
                            else:
                                Data1= [Data1[i] + [abs(Data[i][1])] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Diary':
                            data_type= 'Diary'
                            Day= 1
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval= 3600, factor= factor)
                            if Data1== []:
                                Data1= Data
                            else:
                                # Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                                Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Short profile':
                            data_type= 'Short profile'
                            if outer_key== 'CEV_SCp':
                                interval = 900
                            else:
                                interval = 60
                            Data= self.Read_data_from_csv (CSV_file, data_type,  Day=1, interval= interval, factor= factor)
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        else:
                            print('Error')
            elif key.startswith('ThermalProfile_val'):
                factor= self.config_copy[outer_key]['PNominal'] /self.config_copy[outer_key]['PBase']
                if not isinstance( self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'], list):
                    number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                    number1="{:03d}".format(int(number1))
                    number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                    number2="{:03d}".format(int(number2))
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                    if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                        data_type= 'Anual'
                        Day= self.config_copy ['CBD']['Day']
                        if outer_key== 'CSP_MS_STPwtRK' or outer_key== 'TPS_MS_STPnoRK':
                            interval = 3600
                        else:
                            interval = 900
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day= Day, interval= interval, factor= factor)
                        
                        
                        self.config_copy[outer_key][key] =  [[abs(x) for x in sublist] for sublist in Data]
                    elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                        data_type= 'Diary'
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day=1, interval= 3600, factor= factor)
                        
                        self.config_copy[outer_key][key] = Data
                    else:
                        print('Error')
                else:
                    Data1= []
                    for i in range(len(self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'])):
                        number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'][i]
                        number1="{:03d}".format(int(number1))
                        number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_'][i]
                        number2="{:03d}".format(int(number2))
                        CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                        
                        if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                            data_type= 'Anual'
                            Day= self.config_copy ['CBD']['Day']
                            if outer_key== 'CSP_MS_STPwtRK' or outer_key== 'TPS_MS_STPnoRK':
                                interval = 3600
                            else:
                                interval = 900
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval=interval, factor= factor)
                            
                            self.config_copy[outer_key][key] =  [[abs(x) for x in sublist] for sublist in Data]
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                            data_type= 'Diary'
                            Day= 1
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval= 3600, factor= factor)
                            
                            self.config_copy[outer_key][key] = Data
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        else:
                            print('Error: Short profile not allowed for Thermal Profile')
            elif key.startswith('ctrl'):
                if key.endswith('tON'):
                    # outer_key= key
                    data_type= 'Short profile'
                    day_number = "{:03d}".format(int(self.config_copy['CBD']['Day']))
                    
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_{day_number}'
                    column= f'{outer_key}_tON'
                    path=f'{self.INdir}/{CSV_file}.csv'
                    with open(path, mode='r', newline='') as file:
                        csv_reader = pd.read_csv(file)
                        data_list = csv_reader[column].dropna().tolist() # eliminate NaN values
                        self.config_copy[outer_key][key] = data_list
                else:
                    # outer_key= key
                    data_type= 'Diary'
                    day_number = "{:03d}".format(int(self.config_copy['CBD']['Day']))
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_{day_number}'
                    column= f'{outer_key}_{key.split('_')[1]}'
                    path=f'{self.INdir}/{CSV_file}.csv'
                    with open(path, mode='r', newline='') as file:
                        csv_reader = pd.read_csv(file)
                        data_list = [[time1, data] for time1, data in zip(csv_reader['Time'], csv_reader[column])]
                        self.config_copy[outer_key][key] = data_list
            elif isinstance(value, dict):
                outer_key= key
                self.replace_strings_with_csv_columns(value, outer_key)
        
    def generate_scenarios(self):
        config= self.read_yaml(self.config_file_Nm, self.path_config)
        
        self.config_copy = config
        param_ranges = self.study_data['study_param_range']
        scenarios = self.generate_combinations(param_ranges)
        self.scenario_id = 0
        if 'study_run_dicts' in self.study_data:
            study_run_dicts= self.study_data['study_run_dicts']
            print(f'''\n
            Generating scenarios for the following study \n
            parameters: {param_ranges}, \n
            run dictionaries: {study_run_dicts}''')
            for run_name, run_values in study_run_dicts.items():
                for idx, scenario in enumerate(scenarios):
                    self.scenario_id += 1
                    # print(f"Generating scenario {scenario_id} for run {run_name}")
                    # print(f"Scenario: {scenario}, Run: {run_name}, Values: {run_values}")
                    self.config_copy= config.copy()
                    for outer_key, inner_dict in run_values.items():
                        for inner_key, value in inner_dict.items():
                            self.config_copy[outer_key][inner_key] = value
                    for key, value in scenario.items():
                        key_split = key.split('.')
                        self.config_copy[key_split[0]][key_split[1]] = value
                    
                    self.replace_strings_with_csv_columns(self.config_copy ,outer_key = '')
                    # with open(f'{self.Output_directory}/scenario_{self.scenario_id}.yaml', 'w') as f:
                    with open(f'{self.Output_directory}/scenario_{run_name}_{idx+1}.yaml', 'w') as f:
                        self.scenario_name.append(f'scenario_{run_name}_{idx+1}')
                        yaml.dump(self.config_copy, f)
                        if run_name == 'run_01':
                            if idx == 0:
                                self.base_case_NM= f'scenario_{run_name}_{idx+1}'
                                print(f'**************************, base case: {self.base_case_NM}')
                    print(f"Scenario {self.scenario_id} and {idx+1} generated for run {run_name}")
                    print(scenarios)
                    # Write scenario to JSON file
                    scenario_json = {
                        "scenario_id": self.scenario_id,
                        "run_name": run_name,
                        "run_id": idx+1,
                        "scenario": scenario
                    }
                    json_file_path = f'{self.Output_directory}/Execution_definition.json'
                    if os.path.exists(json_file_path):
                        with open(json_file_path, 'r') as json_file:
                            data = json.load(json_file)
                            if not isinstance(data, list):
                                data = [data]
                            data.append(scenario_json)
                        with open(json_file_path, 'w') as json_file:
                            json.dump(data, json_file, indent=4)
                    else:
                        with open(json_file_path, 'w') as json_file:
                            json.dump([scenario_json], json_file, indent=4)
                    print(f"Scenario {self.scenario_id} written to JSON file: {json_file_path}")
        else: #When there are no run dictionaries
            self.base_case_NM= f'scenario_1'
            for idx, scenario in enumerate(scenarios):
                self.scenario_id += 1
                self.config_copy= config.copy()
                for key, value in scenario.items():
                    key_split = key.split('.')
                    self.config_copy[key_split[0]][key_split[1]] = value
                self.replace_strings_with_csv_columns(self.config_copy ,outer_key = '')
                with open(f'{self.Output_directory}/scenario_{self.scenario_id}.yaml', 'w') as f:
                    self.scenario_name.append(f'scenario_{self.scenario_id}')
                    yaml.dump(self.config_copy, f)
                print(f"Scenario {self.scenario_id} generated")
                # Write scenario to JSON file
                scenario_json = {
                    "scenario_id": self.scenario_id,
                    "scenario": scenario
                }
                json_file_path = f'{self.Output_directory}/Execution_definition.json'
                if os.path.exists(json_file_path):
                    with open(json_file_path, 'r') as json_file:
                        data = json.load(json_file)
                        if not isinstance(data, list):
                            data = [data]
                        data.append(scenario_json)
                    with open(json_file_path, 'w') as json_file:
                        json.dump(data, json_file, indent=4)
                else:
                    with open(json_file_path, 'w') as json_file:
                        json.dump([scenario_json], json_file, indent=4)
                print(f"Scenario {self.scenario_id} written to JSON file: {json_file_path}")
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            print(f"Data: {data}")
            if not isinstance(data, list):
                data = [data]
            data.append(self.study_data)
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
            
    
    def execute_simulation(self, OUTyamlNmTxt, OUTfile):
        print(f"Executing simulation")
        matlab_script = f"""
            clear all; restoredefaultpath %%%%%%%%%%%%%%%
            clearvars -except INstruct; restoredefaultpath
            cd('{self.path_simulation}');
            addpath(genpath('dataSim')); CIEMAT_EDLC_SC_load
            addpath(genpath('auxFunc'))
            addpath(genpath('{self.Output_directory}'))
            %%
            UTfile  = '';
            plotON  = 1;
            cfgON   = 1; 
            MDLfile  = 'ElectricSys_CEDERsimple01';
            [out] = t32_RefCase_RunSlx_4yalm2out({OUTyamlNmTxt},{OUTfile},MDLfile,cfgON,plotON,'',1);

            """
        P2 = subprocess.Popen(
            ['matlab', '-batch', matlab_script], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            universal_newlines=True
        )

        stdout, stderr = P2.communicate()

        if P2.returncode != 0:
            print(f"MATLAB error: {stderr}")
        else:
            print(f"MATLAB output: {stdout}")
        return P2.returncode
    
    def calculate_kpis(self, run_id, OUTdir_study):
        print(f'KPI calculation is running for file: {run_id}')
        # script_parent_dir = Path(__file__).parent.parent
        # path = script_parent_dir / 'log_data'/ OUTdir_study
        path = f'{self.log_data}/{OUTdir_study}'
        print(f'path: {path}')
        kpi_script_path = os.path.join(self.path_kpi_calculation, 'KPI_evaluation.py')
        process= subprocess.Popen(['python', kpi_script_path, f'{run_id}',path, run_id])
        process.wait() 
    
    def run_keys(self):
        
            for run_key in self.study_data.keys():
                if run_key.startswith('run_'):
                    self.runs.append(run_key)
    def batch_kpi_calculation(self):
        KPI_summary= []
        for idx in self.scenario_name:
            path= f'{self.Output_directory}/KPI_outputs_{idx}.json'
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    data['scenario_name'] = idx
                    KPI_summary.append(data)
            except FileNotFoundError as e:
                print(f"File not found: {path}")
                raise e
        with open(f'{self.Output_directory}/KPI_summary.json', 'w') as f:
            json.dump(KPI_summary, f, indent=4)
        if KPI_summary:
            df = pd.DataFrame(KPI_summary)
            df.to_csv(f'{self.Output_directory}/KPI_summary.csv', index=False)
            print(f"KPI summary written to JSON file: {self.Output_directory}/KPI_summary.json")
        else:
            print("No KPI summary data to write.")
    def Translate_Dicts_Opt (self, Transdict_Path, Config_template_Path):
                for idx in self.scenario_name:
                    df= pd.read_excel (Transdict_Path)
                    with open(Config_template_Path, 'r') as f:
                        Opt_config= yaml.safe_load(f)
                    with open (f'{self.Output_directory}/{idx}.yaml') as f:
                        Sim_config= yaml.safe_load(f)
                        for _, row in df.iterrows():
                            Sim_Key1 = row['Sim_Key1']
                            Sim_Key2 = row['Sim_Key2']
                            Opt_Key1 = row ['Opt_Key1']
                            Opt_Key2 = row ['Opt_Key2']
                            Opt_Key3 = row ['Opt_Key3']
                            if pd.isna(Opt_Key1):
                                # Only two levels of nesting
                                Opt_config[Opt_Key2][Opt_Key3] = Sim_config[Sim_Key1][Sim_Key2]
                            else:
                                # Three levels of nesting
                                # Opt_config [Opt_Key1][0][Opt_Key2][Opt_Key3] = Sim_config[Sim_Key1][Sim_Key2]
                                        if isinstance(Opt_config.get(Opt_Key1), list):
                                            for item in Opt_config[Opt_Key1]:
                                                if Opt_Key2 in item:
                                                    item[Opt_Key2][Opt_Key3] = Sim_config[Sim_Key1][Sim_Key2]
                                                    break
                                        else:
                                            # Normal 3-stage nesting
                                            Opt_config.setdefault(Opt_Key1, {}).setdefault(Opt_Key2, {})[Opt_Key3] = Sim_config[Sim_Key1][Sim_Key2]
                    with open(f'{self.Output_directory}/config_{idx}.yaml', 'w') as f:
                        yaml.dump(Opt_config, f) 
    def Translate_Dicts_Sim(self, Transdict_Path):
        interval = 3600  # seconds between time points

        def add_timestamps(data_list, start_day=0):
            if isinstance(data_list, list) and all(isinstance(x, (int, float)) for x in data_list):
                return [[(start_day * len(data_list) + i) * interval, x] for i, x in enumerate(data_list)]
            return data_list  # return as-is if it's not a numeric list

        for idx in self.scenario_name:
            df = pd.read_excel(Transdict_Path)

            with open(f'{self.Output_directory}/config_{idx}.yaml', 'r') as f:
                Opt_output = yaml.safe_load(f)

            with open(f'{self.Output_directory}/{idx}.yaml') as f:
                Sim_config = yaml.safe_load(f)

            for _, row in df.iterrows():
                Sim_Key1 = row['Sim_Key1']
                Sim_Key2 = row['Sim_Key2']
                Opt_Key1 = row['Opt_Key1']
                Opt_Key2 = row['Opt_Key2']
                Opt_Key3 = row['Opt_Key3']

                if pd.isna(Opt_Key1):
                    # Only two levels of nesting
                    value = Opt_output[Opt_Key2][Opt_Key3]
                    Sim_config[Sim_Key1][Sim_Key2] = add_timestamps(value)
                else:
                    if isinstance(Opt_output.get(Opt_Key1), list):
                        for item in Opt_output[Opt_Key1]:
                            if Opt_Key2 in item:
                                value = item[Opt_Key2][Opt_Key3]
                                Sim_config[Sim_Key1][Sim_Key2] = add_timestamps(value)
                                break
                    else:
                        # Normal 3-stage nesting
                        value = Opt_output.setdefault(Opt_Key1, {}).setdefault(Opt_Key2, {})[Opt_Key3]
                        Sim_config[Sim_Key1][Sim_Key2] = add_timestamps(value)

            with open(f'{self.Output_directory}/{idx}.yaml', 'w') as f:
                yaml.dump(Sim_config, f)

    def Config_Opt_function(self,Config_Opt_path):
        # 
        for idx in self.scenario_name:
            df= pd.read_excel (Config_Opt_path)
            with open(f'{self.Output_directory}/config_{idx}.yaml', 'r') as f:
                Opt_config= yaml.safe_load(f)
                for _, row in df.iterrows():
                    Opt_Key1 = row ['Opt_Key1']
                    Opt_Key2 = row ['Opt_Key2']
                    Opt_Key3 = row ['Opt_Key3']
                    Opt_Value = row ['Opt_Value']
                    if pd.isna(Opt_Key1):
                        # Only two levels of nesting
                        Opt_config[Opt_Key2][Opt_Key3] = Opt_Value
                    else:
                        # Three levels of nesting
                        # Opt_config [Opt_Key1][0][Opt_Key2][Opt_Key3] = Sim_config[Sim_Key1][Sim_Key2]
                                if isinstance(Opt_config.get(Opt_Key1), list):
                                    for item in Opt_config[Opt_Key1]:
                                        if Opt_Key2 in item:
                                            item[Opt_Key2][Opt_Key3] =  Opt_Value
                                            break
                                else:
                                    # Normal 3-stage nesting
                                    Opt_config.setdefault(Opt_Key1, {}).setdefault(Opt_Key2, {})[Opt_Key3] = Opt_Value
            with open(f'{self.Output_directory}/config_{idx}.yaml', 'w') as f:
                yaml.dump(Opt_config, f) 
# ********************************************************************************************************************
    def run_pipeline(self):
        OUTdir_study = f'Study_{time():.00f}'
        OUTdir_study = 'Study_1746704604' #4KPI
        # os.mkdir(f'{self.log_data}/{OUTdir_study}') #4KPI
        self.Output_directory = f'{self.log_data}/{OUTdir_study}' 
        # Step 1: Load Study Configuration
        self.load_study()
        print('Study loaded')
        self.config_file_Nm = self.study_data['base_config']['base_config_xls']
        self.config_file= f'{self.config_file_Nm}.yaml'
        self.MDLfile = 'ElectricSys_CEDERsimple01'
        self.INfile = f'{self.path_config}/{self.config_file_Nm}.xlsx'
        # self.INdir = f'{self.path_simulation}/test_bookChap_data/test_bookChap_config'
        # Step 1-1: Generate yaml files and scenarios
        if self.study_data['base_config']['base_config_yaml'] is None:
            if self.xls_to_yaml():
                # Write updated study data to YAML file (Showing that the base_config_yaml has been updated)
                self.study_data['base_config']['base_config_yaml'] = self.config_file_Nm
                with open(f'{self.study_file}', 'w') as f:
                    yaml.dump(self.study_data, f)
            else:
                print(f"Error: Failed to convert {self.config_file_Nm}.xlsx to YAML")
                exit(1)
        elif self.study_data['base_config']['base_config_yaml'] == self.config_file_Nm:
            pass
        else:
            print(f"Error: base_config_yaml does not match base_config_xls")
            exit(1)
        self.generate_scenarios()
        OUTyamlNmTxt= []
        OUTfile= []
    
        for idx in self.scenario_name:
            OUTyamlNmTxt.append(f'{self.Output_directory}/{idx}.yaml')
            OUTfile.append(f'{self.Output_directory}/{idx}')
            
        
         # Add some lines to yaml files
            with open(f'{self.Output_directory}/{idx}.yaml', 'r') as f:
                    scenario_data= yaml.safe_load(f)
                    scenario_data ['CBD'] ['Total_El_load'] = [sum(row[1:]) + sum(row2[1:]) + sum(row3[1:]) for row, row2, row3 in zip(scenario_data['Cbue_NSCp']['P_baseElectricProfile_val'], scenario_data['Cbu_NSCp']['P_baseElectricProfile_val'], scenario_data['CEV_SCp']['P_baseElectricProfile_val'])]
                    scenario_data ['CBD'] ['Total_Th_load'] = [sum(row[1:]) for row in scenario_data['Ctbu_TCp']['P_baseThermalProfile_val']]
                    scenario_data ['CBD'] ['Total_El_Gen'] = [sum(row[1:]) + sum(row2[1:]) for row, row2 in zip(scenario_data['WG_PPMp']['P_baseElectricProfile_val'], scenario_data['PV_PPMp']['P_baseElectricProfile_val'])]
                    scenario_data ['CBD'] ['Total_Th_Gen'] = [sum(row[1:]) + sum(row2[1:]) for row, row2 in zip(scenario_data['CSP_MS_STPwtRK']['P_baseThermalProfile_val'], scenario_data['TPS_MS_STPnoRK']['P_baseThermalProfile_val'])]
                    # scenario_data ['CBD'] ['Price'] = scenario_data ['elExGRID']['epzProfile_val']
                    # scenario_data ['CBD'] ['Price_gas'] = scenario_data ['elExGRID']['epzProfile_val']
            with open(f'{self.Output_directory}/{idx}.yaml', 'w') as f:
                    yaml.dump(scenario_data, f)
        OUTfile1= list(dict.fromkeys(OUTfile))
        OUTfile1 = '{' + ' '.join(f"'{item}'" for item in OUTfile1) + '}'
        OUTyamlNmTxt1= list(dict.fromkeys(OUTyamlNmTxt))
        OUTyamlNmTxt1 = '{' + ' '.join(f"'{item}'" for item in OUTyamlNmTxt1) + '}'
        # Step 1-2: Translate yaml files keys for optimisation
        # Translate Dicts
        Transdict_Path= f'{self.path_config}/Transdict1.xlsx'
        Config_template_Path= f'{self.path_dispatch_optimisation}/config/config_template.yaml'
        self.Translate_Dicts_Opt (Transdict_Path, Config_template_Path)
        # Step 1-2: Set config parameters for optimisation
        Config_Opt_path= f'{self.path_config}/Config_Opt.xlsx'
        self.Config_Opt_function (Config_Opt_path)
        # Step 2: Run Optimisation
        Opt_path = os.path.join(f'{self.path_dispatch_optimisation}/StandaloneFunctions', 'DispatchOptimisationRunner.py')

        for idx in self.scenario_name:
            config_file = f'{self.Output_directory}/config_{idx}.yaml'
            print(f"Launching optimization for: {config_file}")
            process= subprocess.Popen(["python", Opt_path, config_file]
                             , stdout=subprocess.DEVNULL
                            #  ,stderr=subprocess.DEVNULL
                             )
            process.wait() 
        # Step 2-1: Update yaml files for simulink based on optimization outputs
        Transdict_Path= f'{self.path_config}/Transdict2.xlsx'
        self.Translate_Dicts_Sim (Transdict_Path)
        #Step 3: Run Simulink
        
        # self.execute_simulation( OUTyamlNmTxt1, OUTfile1) #4KPI
        # Add some columns to the _KPI.json files
        for idx in self.scenario_name:
            with open(f'{self.Output_directory}/{idx}_KPI.json', 'r') as f:
                data = json.load(f)
            with open(f'{self.Output_directory}/{idx}.yaml', 'r') as f:
                scenario_data= yaml.safe_load(f)
                data ['Total_El_load'] = [sum(row[1:]) + sum(row2[1:]) + sum(row3[1:]) for row, row2, row3 in zip(scenario_data['Cbue_NSCp']['P_baseElectricProfile_val'], scenario_data['Cbu_NSCp']['P_baseElectricProfile_val'], scenario_data['CEV_SCp']['P_baseElectricProfile_val'])]
                data ['Total_Th_load'] = [sum(row[1:]) for row in scenario_data['Ctbu_TCp']['P_baseThermalProfile_val']]
                data ['Total_El_Gen'] = [sum(row[1:]) + sum(row2[1:]) for row, row2 in zip(scenario_data['WG_PPMp']['P_baseElectricProfile_val'], scenario_data['PV_PPMp']['P_baseElectricProfile_val'])]
                data ['Total_Th_Gen'] = [sum(row[1:]) + sum(row2[1:]) for row, row2 in zip(scenario_data['CSP_MS_STPwtRK']['P_baseThermalProfile_val'], scenario_data['TPS_MS_STPnoRK']['P_baseThermalProfile_val'])]
                data['cost_obj'] = scenario_data['CBD']['cost_obj']
                data['Cost_operation_ESS'] = scenario_data['CBD']['Cost_operation_ESS']
               
            with open(f'{self.Output_directory}/{idx}_KPI.json', 'w') as f:
                json.dump(data, f, indent=4)
        # Copy Base Case data related to KPIs in a file named Base_case_KPI.json
        with open(f'{self.Output_directory}/{self.base_case_NM}_KPI.json', 'r') as f:
            base_case_data = json.load(f)
        with open(f'{self.Output_directory}/Base_case_KPI.json', 'w') as f:
            json.dump(base_case_data, f, indent=4)
        # Step 4: Calculate KPIs for Base Case
        path = self.Output_directory
        kpi_script_path = os.path.join(self.path_kpi_calculation, 'KPI_evaluation.py')
        process= subprocess.Popen(['python', kpi_script_path, f'Base_case_KPI',path])
        process.wait()
        # Step 5: Calculate KPIs for Each Run
        for idx in self.scenario_name:
            self.calculate_kpis(idx, OUTdir_study)
                    
        # Step 6: Calculate Batch KPIs
        self.batch_kpi_calculation()

if __name__ == "__main__":
    # dispatcher = PipelineDispatcher(study_file_Nm="study_file")
    dispatcher = PipelineDispatcher(study_file_Nm="study_complete")
    # dispatcher = PipelineDispatcher(study_file_Nm="study_file1")
    # dispatcher = PipelineDispatcher(study_file_Nm="TEST")
    dispatcher.run_pipeline()