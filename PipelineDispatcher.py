#TODO: Check short profile replacement
#TODO: check initial example values with acceptable ranges
import yaml
import subprocess
import os
from pathlib import Path
from time import time
from itertools import product
import json
import pandas as pd
import numpy as np
class PipelineDispatcher:
    def __init__(self, study_file_Nm):
        self.study_file_Nm = study_file_Nm
        # self.config_file_Nm = config_file_Nm
        self.study_file = f'{study_file_Nm}.yaml'
        # self.config_file= f'{config_file_Nm}.yaml'
        self.runs = []
        # self.OUTdir = f'{self.config_file_Nm}_output'
    
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
            t32_RefCase_ReadCfg_4xlscsv2yalm('{self.INfile}', '{self.INdir}', '{self.config_file_Nm}');
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
            print("Config file converted to YAML by MATLAB.")
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
    
    def Read_data_from_csv (self, file_path, data_type, Day= 1):
        # print(f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{file_path}')
        with open(f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{file_path}.csv', mode='r', newline='') as file:
            csv_reader = pd.read_csv(file)
            r = []
            if data_type =='Diary':
                day=1
            elif data_type== 'Anual':
                day= Day

            for i in range(24):
                id= i+(day-1)*24
                row = csv_reader[csv_reader.isin([id*(3600)]).any(axis=1)]
                if not row.empty:
                    r.append([id*3600, float(row.iloc[0, 1])])
        return r
    def replace_strings_with_csv_columns(self, yaml_content, outer_key):
        for key, value in yaml_content.items():
            if key == 'epzProfile_val':
                data_type= 'Anual'
                CSV_file= f'TP_{self.config_copy['CBD']['Location']}_elePrizes_2022'
                print(f"Excel file: {CSV_file}")
            if key == 'nuProfile_val':
                data_type= 'Anual'
                CSV_file= f'TP_{self.config_copy['CBD']['Location']}_nuPrizes_2022'
                print(f"Excel file: {CSV_file}")
            if key.endswith('ElectricProfile_val'):
                #if there is just one component
                if not isinstance( self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'], list):
                    number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                    number1="{:03d}".format(int(number1))
                    number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                    number2="{:03d}".format(int(number2))
                    print(number1, number2)
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseElectricProfile']}_{number1}_{number2}'
                    print(f"CSV file: {CSV_file}")
                    if self.config_copy [outer_key]['P_baseElectricProfileType']== 'Anual':
                        data_type= 'Anual'
                        Day= self.config_copy ['CBD']['Day']
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day)
                        self.config_copy[outer_key][key] = Data
                    elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Diary':
                        data_type= 'Diary'
                        Data= self.Read_data_from_csv (CSV_file, data_type, 1)
                        self.config_copy[outer_key][key] = Data
                    elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Short profile':
                        # data_type= 'Short profile'
                        # CSV_file1= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_327'
                        # column= f'{outer_key}_tON'
                        # path1=f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{CSV_file1}.csv'
                        # path2=f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{CSV_file}.csv'
                        # with open(path1, mode='r', newline='') as file:
                        #     csv_reader = pd.read_csv(file)
                        #     T_ON = csv_reader[column].tolist()
                        # with open(path2, mode='r', newline='') as file:
                        #     csv_reader = pd.read_csv(file)
                        #     data_list = []
                        #     for i in range (24):
                        #         #TODO: check this for T_ON! A small change was is needed here
                        #         if not np.isnan(T_ON[ii]):
                        #             data_list.append([i*3600, csv_reader.iloc[i, 1]])
                        #         else:
                        #             data_list.append([i*3600, 0])

                        #     self.config_copy[outer_key][key] = data_list
                        pass
                    else:
                        print('Error')
                else:
                    #if there are multiple components
                    data_list = []
                    for i in range(len(self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'])):
                        number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'][i]
                        number1="{:03d}".format(int(number1))
                        number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_'][i]
                        number2="{:03d}".format(int(number2))
                        print(number1, number2)
                        CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseElectricProfile']}_{number1}_{number2}'
                        print(f"CSV file: {CSV_file}")
                        if self.config_copy [outer_key]['P_baseElectricProfileType']== 'Anual':
                            data_type= 'Anual'
                            Day= self.config_copy ['CBD']['Day']
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day)
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [[Data1[i][:],Data[i][1]] for i in range(len(Data))]
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Diary':
                            data_type= 'Diary'
                            Data= self.Read_data_from_csv (CSV_file, data_type, 1)
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [[Data1[i][:],Data[i][1]] for i in range(len(Data))]
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseElectricProfileType']== 'Short profile':
                            # data_type= 'Short profile'
                            # CSV_file1= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_327'
                            # column= f'{outer_key}_tON'
                            # path1=f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{CSV_file1}.csv'
                            # path2=f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{CSV_file}.csv'
                            # with open(path1, mode='r', newline='') as file:
                            #     csv_reader = pd.read_csv(file)
                            #     T_ON = csv_reader[column].tolist()
                            #     print(T_ON)
                            # with open(path2, mode='r', newline='') as file:
                            #     csv_reader = pd.read_csv(file)
                            # if data_list== []:
                            #     for ii in range (24):
                            #         if not np.isnan(T_ON[ii]):
                            #             print(ii)
                            #             data_list.append([ii*3600, csv_reader.iloc[ii, 1]])
                            #         else:
                            #             data_list.append([ii*3600, 0])
                            # else:
                            #     for ii in range (24):
                            #         if not np.isnan(T_ON[ii]):
                            #             print(ii)
                            #             data_list[ii].append(csv_reader.iloc[ii, 1])
                            #         else:
                            #             data_list[ii].append(0)
                            # self.config_copy[outer_key][key] = data_list
                            pass
                        else:
                            print('Error')
            elif key.startswith('ThermalProfile_val'):
                if isinstance( self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'], list):
                    number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_']
                    number1="{:03d}".format(int(number1))
                    number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_']
                    number2="{:03d}".format(int(number2))
                    print(number1, number2)
                    CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                    print(f"CSV file: {CSV_file}")
                    if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                        data_type= 'Anual'
                        Day= self.config_copy ['CBD']['Day']
                        Data= self.Read_data_from_csv (CSV_file, data_type, Day)
                        self.config_copy[outer_key][key] = Data
                    elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                        data_type= 'Diary'
                        Data= self.Read_data_from_csv (CSV_file, data_type, 1)
                        self.config_copy[outer_key][key] = Data
                    else:
                        print('Error')
                else:
                    for i in range(len(self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'])):
                        number1= self.config_copy [outer_key]['ProfileCaseVal1_columnSelectionByCase_'][i]
                        number1="{:03d}".format(int(number1))
                        number2= self.config_copy [outer_key]['ProfileCaseVal2_columnSelectionBySub_case_'][i]
                        number2="{:03d}".format(int(number2))
                        print(number1, number2)
                        CSV_file= f'TP_{self.config_copy['CBD']['Location']}_{self.config_copy[outer_key]['P_baseThermalProfile']}_{number1}_{number2}'
                        print(f"CSV file: {CSV_file}")
                        # pause= input("Press Enter to continue...")
                        Data1= []
                        if self.config_copy [outer_key]['P_baseThermalProfileType']== 'Anual':
                            data_type= 'Anual'
                            Day= self.config_copy ['CBD']['Day']
                            Data= self.Read_data_from_csv (CSV_file, data_type, Day)
                            self.config_copy[outer_key][key] = Data
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [[Data1[i][:],Data[i][1]] for i in range(len(Data))]
                            self.config_copy[outer_key][key] = Data1
                        elif self.config_copy [outer_key]['P_baseThermalProfileType']== 'Diary':
                            data_type= 'Diary'
                            Data= self.Read_data_from_csv (CSV_file, data_type, 1)
                            self.config_copy[outer_key][key] = Data
                            if Data1== []:
                                Data1= Data
                            else:
                                Data1= [[Data1[i][:],Data[i][1]] for i in range(len(Data))]
                            self.config_copy[outer_key][key] = Data1
                        else:
                            print('Error')
            elif key.startswith('ctrl'):
                # outer_key= key
                data_type= 'Diary'
                CSV_file= f'TP_{self.config_copy['CBD']['Location']}_CtrlSyst_day_327'
                column= f'{outer_key}_{key.split('_')[1]}'
                path=f'{self.path_simulation}/StoRIES_RefCase_Config_rev04_input/{CSV_file}.csv'
                with open(path, mode='r', newline='') as file:
                    csv_reader = pd.read_csv(file)
                    data_list = [[time, data] for time, data in zip(csv_reader['Time'], csv_reader[column])]
                    self.config_copy[outer_key][key] = data_list
            elif isinstance(value, dict):
                outer_key= key
                print(f"outer key: {outer_key}")
                self.replace_strings_with_csv_columns(value, outer_key)
        
    def generate_scenarios(self):
        config= self.read_yaml(self.config_file_Nm, self.path_simulation)
        self.config_copy = config
        param_ranges = self.study_data['study_param_range']
        scenarios = self.generate_combinations(param_ranges)
        study_run_dicts= self.study_data['study_run_dicts']
        print(f'''\n
Generating scenarios for the following study \n
parameters: {param_ranges}, \n
run dictionaries: {study_run_dicts}''')

        self.scenario_id = 0
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
                # with open(f'{self.Output_directory}/scenario_{run_name}_{idx}.yaml', 'w') as f:
                self.replace_strings_with_csv_columns(self.config_copy ,outer_key = '')
                with open(f'{self.Output_directory}/scenario_{self.scenario_id}.yaml', 'w') as f:
                    yaml.dump(self.config_copy, f)
                print(f"Scenario {self.scenario_id} and {idx} generated for run {run_name}")
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

        if P2.returncode == 0:
            print(stdout)
            print("Simulation completed.")
        else:
            print(stderr)
            print("Error in simulation")
        return P2.returncode
    
    def calculate_kpis(self, run_id, OUTdir_study):
        print(f'KPI calculation is running for file: {run_id}')
        # script_parent_dir = Path(__file__).parent.parent
        # path = script_parent_dir / 'log_data'/ OUTdir_study
        path = f'../log_data/{OUTdir_study}'
        print(f'path: {path}')
        kpi_script_path = os.path.join(self.path_kpi_calculation, 'KPI_evaluation.py')
        subprocess.Popen(['python', kpi_script_path, f'scenario_{run_id}_KPI',path, run_id])
           
    def batch_kpi_calculation(self):
        pass

    def run_keys(self):
        
            for run_key in self.study_data.keys():
                if run_key.startswith('run_'):
                    self.runs.append(run_key)

# ********************************************************************************************************************
    def run_pipeline(self):
        
        OUTdir_study = f'Study_{time():.00f}'

        # script_parent_dir = Path(__file__).parent.parent
        # log_data = script_parent_dir / 'log_data'
        log_data = '../log_data'
        os.mkdir(f'../log_data/{OUTdir_study}')
        self.Output_directory = f'../log_data/{OUTdir_study}' 
        # Step 1: Load Study Configuration
        # TODO: define xls_to_yaml function based on the study configuration
        self.load_study()
        print('Study loaded')
        self.config_file_Nm = self.study_data['base_config']['base_config_xls']
        self.config_file= f'{self.config_file_Nm}.yaml'
        self.MDLfile = 'ElectricSys_CEDERsimple01'
        self.INfile = f'{self.config_file_Nm}.xlsx'
        self.INdir = f'{self.config_file_Nm}_input'
        if self.study_data['base_config']['base_config_yaml'] is None:
            self.xls_to_yaml() 
            self.study_data['base_config']['base_config_yaml'] = self.config_file_Nm
            with open(f'{self.config_file}.yaml', 'w') as f:
                yaml.dump(self.config_file, f)
        elif self.study_data['base_config']['base_config_yaml'] == self.config_file_Nm:
            pass
        else:
            print(f"Error: base_config_yaml does not match base_config_xls")
            exit(1)
        
        
        # Step 3: run optimization
        # step 4: run simulation
        self.generate_scenarios()
        
        OUTyamlNmTxt= []
        OUTfile= []
        for idx in range(self.scenario_id):
            OUTyamlNmTxt.append(f'{self.Output_directory}\\scenario_{idx+1}.yaml')
            OUTfile.append(f'..\log_data\{OUTdir_study}\scenario_{idx+1}')
        
        self.execute_simulation( set(OUTyamlNmTxt), set(OUTfile))
        # Step 5: Calculate KPIs for Each Run
        for idx in range(self.scenario_id):
            self.calculate_kpis(str(idx+1), OUTdir_study)
        
        # # Step 6: Calculate Batch KPIs
        # self.batch_kpi_calculation()

# Example usage
if __name__ == "__main__":
    dispatcher = PipelineDispatcher(study_file_Nm="study_simple1")
    dispatcher.run_pipeline()
