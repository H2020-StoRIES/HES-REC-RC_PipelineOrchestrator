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
        self.study_file_Nm = study_file_Nm
        # self.config_file_Nm = config_file_Nm
        self.study_file = f'{study_file_Nm}.yaml'
        # self.config_file= f'{config_file_Nm}.yaml'
        self.runs = []
        # self.OUTdir = f'{self.config_file_Nm}_output'
        self.scenario_name = []
        # script_parent_dir = Path(__file__).parent.parent
        # self.path_simulation= script_parent_dir/'t32-ref-case-dev'
        # self.path_dispatcher= script_parent_dir/'Pipeline-dispatcher'
        # self.path_kpi_calculation= script_parent_dir/'KPI_Evaluation\\KPI_Evaluation_python'
        self.path_simulation= '../t32-ref-case-dev'
        self.path_dispatcher= '../Pipeline-dispatcher'
        self.path_kpi_calculation= '../KPI_Evaluation/KPI_Evaluation_python'
    def xls_to_yaml(self):
        
        matlab_script = f"""
            clear all; restoredefaultpath %%%%%%%%%%%%%%%
            clearvars -except INstruct; restoredefaultpath
            cd('{self.path_simulation}');
            addpath(genpath('auxFunc'));
            t32_RefCase_ReadCfg_4xlscsv2yalm('{self.INfile}', '{self.INdir}', '{self.config_file_Nm}.yaml',1);
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
        else:
            print(stderr)
            print("Error in xls_to_yaml conversion")

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
        self.study_data = self.read_yaml(self.study_file_Nm, self.path_dispatcher)


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
        with open(f'{self.path_simulation}/test_bookChap_data/test_bookChap_config/{file_path}.csv', mode='r', newline='') as file:
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
                        # pause= input("Press Enter to continue...")
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
                csv_reader = pd.read_csv(f'{self.path_simulation}/test_bookChap_data/test_bookChap_config/{CSV_file}.csv', header=None)
                for i in range(T):
                        id= i+(day-1)*T
                        period= H/interval
                        # Resample the interval-second dataset to 3600-second intervals by mean values, filling missing values with 0
                        df = csv_reader.loc[(csv_reader.iloc[:,0] >= id * H) & (csv_reader.iloc[:,0]  < (id + 1) * H)].fillna(0)
                        # pause= input("Press Enter to continue...")
                        r.append([i * H, factor* float(df.iloc[:, 1].sum() / period), factor* float(df.iloc[:, 2].sum() / period)])
                self.config_copy[outer_key][key] = r
            if key == 'nuProfile_val':
                data_type= 'Anual'
                interval = 3600
                number1= self.config_copy[outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                number1="{:03d}".format(int(number1))
                number2= self.config_copy[outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                number2="{:03d}".format(int(number2))
                CSV_file= f'TP_{self.config_copy['CBD']['Location']}_RC_nu_{number1}_{number2}'
                Day= self.config_copy ['CBD']['Day']
                T=24
                H=3600
                r= []
                csv_reader = pd.read_csv(f'{self.path_simulation}/test_bookChap_data/test_bookChap_config/{CSV_file}.csv', header=None)
                for i in range(T):
                        id= i+(Day-1)*T
                        period= H/interval
                        factor= 1
                        # Resample the interval-second dataset to 3600-second intervals by mean values, filling missing values with 0
                        df = csv_reader.loc[(csv_reader.iloc[:,0] >= id * H) & (csv_reader.iloc[:,0]  < (id + 1) * H)].fillna(0)
                        # pause= input("Press Enter to continue...")
                        r.append([i * H, factor* float(df.iloc[:, 1].sum() / period)])
                self.config_copy[outer_key][key] = r
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
                        if outer_key== 'CSP_MS_STPwtRK':
                            interval = 3600
                        else:
                            interval = 900
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day= Day, interval= interval, factor= factor)
                        
                        self.config_copy[outer_key][key] = Data
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
                            if outer_key== 'CSP_MS_STPwtRK':
                                interval = 3600
                            else:
                                interval = 900
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval= interval, factor= factor)
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                            
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
                if isinstance( self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'], list):
                    number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                    number1="{:03d}".format(int(number1))
                    number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
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
                else:
                    for i in range(len(self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'])):
                        number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'][i]
                        number1="{:03d}".format(int(number1))
                        number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_'][i]
                        number2="{:03d}".format(int(number2))
                        CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                        # pause= input("Press Enter to continue...")
                        Data1= []
                        if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                            data_type= 'Anual'
                            Day= self.config_copy ['CBD']['Day']
                            if outer_key== 'CSP_MS_STPwtRK':
                                interval = 3600
                            else:
                                interval = 900
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=Day, interval=interval, factor= factor)
                            
                            self.config_copy[outer_key][key] = Data
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [Data1[i] + [Data[i][1]] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                            data_type= 'Diary'
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day=1, interval= 3600, factor= factor)
                            
                            self.config_copy[outer_key][key] = Data
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [Data1[i][:] + [Data[i][1]] for i in range(len(Data))]
                            
                            self.config_copy[outer_key][key] = Data1
                        else:
                            print('Error: Short profile not allowed for Thermal Profile')
            elif key.startswith('ctrl'):
                if key.endswith('tON'):
                    # outer_key= key
                    data_type= 'Short profile'
                    day_number = "{:03d}".format(int(self.config_copy['CBD']['Day']))
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_{day_number} (2)'
                    column= f'{outer_key}_tON'
                    path=f'{self.path_simulation}/test_bookChap_data/test_bookChap_config/{CSV_file}.csv'
                    with open(path, mode='r', newline='') as file:
                        csv_reader = pd.read_csv(file)
                        data_list = csv_reader[column].dropna().tolist() # eliminate NaN values
                        self.config_copy[outer_key][key] = data_list
                else:
                    # outer_key= key
                    data_type= 'Diary'
                    day_number = "{:03d}".format(int(self.config_copy['CBD']['Day']))
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_{day_number} (2)'
                    column= f'{outer_key}_{key.split('_')[1]}'
                    path=f'{self.path_simulation}/test_bookChap_data/test_bookChap_config/{CSV_file}.csv'
                    with open(path, mode='r', newline='') as file:
                        csv_reader = pd.read_csv(file)
                        data_list = [[time, data] for time, data in zip(csv_reader['Time'], csv_reader[column])]
                        self.config_copy[outer_key][key] = data_list
            elif isinstance(value, dict):
                outer_key= key
                self.replace_strings_with_csv_columns(value, outer_key)
        
    def generate_scenarios(self):
        config= self.read_yaml(self.config_file_Nm, self.path_simulation)
        
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
                            self.base_case_NM= f'scenario_{run_name}_{idx+1}'
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
            

    def execute_optimization(self, run_id):
        pass
    
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
        path = f'../log_data/{OUTdir_study}'
        print(f'path: {path}')
        kpi_script_path = os.path.join(self.path_kpi_calculation, 'KPI_evaluation.py')
        subprocess.Popen(['python', kpi_script_path, f'{run_id}_KPI',path, run_id])
           
    
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

# ********************************************************************************************************************
    def run_pipeline(self):
        
        OUTdir_study = f'Study_{time():.00f}'
        # OUTdir_study = 'Study_1737132576' #4KPI
        log_data = '../log_data'
        os.mkdir(f'../log_data/{OUTdir_study}') #4KPI
        self.Output_directory = f'../log_data/{OUTdir_study}' 
        # Step 1: Load Study Configuration
        self.load_study()
        print('Study loaded')
        self.config_file_Nm = self.study_data['base_config']['base_config_xls']
        self.config_file= f'{self.config_file_Nm}.yaml'
        self.MDLfile = 'ElectricSys_CEDERsimple01'
        self.INfile = f'{self.config_file_Nm}.xlsx'
        self.INdir = f'{self.path_simulation}/test_bookChap_data/test_bookChap_config'
        if self.study_data['base_config']['base_config_yaml'] is None:
            self.xls_to_yaml() 
            # Write updated study data to YAML file (Showing that the base_config_yaml has been updated)
            self.study_data['base_config']['base_config_yaml'] = self.config_file_Nm
            with open(f'{self.study_file}', 'w') as f:
                yaml.dump(self.study_data, f)
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
        self.execute_simulation( set(OUTyamlNmTxt), set(OUTfile)) #4KPI
        # Copy Base Case data related to KPIs in a file named Base_case_KPI.json
        with open(f'{self.Output_directory}/{self.base_case_NM}_KPI.json', 'r') as f:
            base_case_data = json.load(f)
        with open(f'{self.Output_directory}/Base_case_KPI.json', 'w') as f:
            json.dump(base_case_data, f, indent=4)
        # Add some columns to the _KPI.json files
        for idx in self.scenario_name:
            with open(f'{self.Output_directory}/{idx}_KPI.json', 'r') as f:
                data = json.load(f)
            with open(f'{self.Output_directory}/{idx}.yaml', 'r') as f:
                scenario_data= yaml.safe_load(f)
                data['Total_El_load'] = [sum(row[1:]) + sum(row2[1:]) + sum(row3[1:]) for row, row2, row3 in zip(scenario_data['Cbue_NSCp']['P_baseElectricProfile_val'], scenario_data['Cbu_NSCp']['P_baseElectricProfile_val'], scenario_data['CEV_SCp']['P_baseElectricProfile_val'])]
                data['Total_Th_load'] = [sum(row[1:]) for row in scenario_data['Ctbu_TCp']['P_baseThermalProfile_val']]
                data['WG'] = [sum(row[1:]) for row in scenario_data['WG_PPMp']['P_baseElectricProfile_val']]
                data['PV'] = [sum(row[1:]) for row in scenario_data['PV_PPMp']['P_baseElectricProfile_val']]
                data['Price']= scenario_data['CBD']['Price']

            with open(f'{self.Output_directory}/{idx}_KPI.json', 'w') as f:
                json.dump(data, f, indent=4)
        # Step 4: Calculate KPIs for Base Case
        path = f'../log_data/{OUTdir_study}'
        kpi_script_path = os.path.join(self.path_kpi_calculation, 'KPI_evaluation.py')
        subprocess.Popen(['python', kpi_script_path, f'Base_case_KPI',path])
        # Step 5: Calculate KPIs for Each Run
        for idx in self.scenario_name:
            self.calculate_kpis(idx, OUTdir_study)
            
        pause= input("Press Enter to continue...")
        
        # Step 6: Calculate Batch KPIs
        self.batch_kpi_calculation()

# Example usage
if __name__ == "__main__":
    dispatcher = PipelineDispatcher(study_file_Nm="study_simple1")
    dispatcher.run_pipeline()
