import yaml
import subprocess
import os
from pathlib import Path
from time import time
from itertools import product


class PipelineDispatcher:
    def __init__(self, study_file_Nm, config_file_Nm):
        self.study_file_Nm = study_file_Nm
        self.config_file_Nm = config_file_Nm
        self.study_file = f'{study_file_Nm}.yaml'
        self.config_file= f'{config_file_Nm}.yaml'
        self.runs = []
        # self.OUTdir = f'{self.config_file_Nm}_output'
        
        self.MDLfile = 'ElectricSys_CEDERsimple01'
        self.INfile = f'{self.config_file_Nm}.xlsx'
        self.INdir = f'{self.config_file_Nm}_input'

        script_parent_dir = Path(__file__).parent.parent
        self.path_simulation= script_parent_dir/'t32-ref-case-dev'
        self.path_dispatcher= script_parent_dir/'Pipeline-dispatcher'
        self.path_kpi_calculation= script_parent_dir/'KPI_Evaluation\\KPI_Evaluation_python'
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
    def generate_scenarios(self):
        config= self.read_yaml(self.config_file_Nm, self.path_simulation)
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
                config_copy= config.copy()
                for outer_key, inner_dict in run_values.items():
                    for inner_key, value in inner_dict.items():
                        config_copy[outer_key][inner_key] = value
                for key, value in scenario.items():
                    key_split = key.split('.')
                    config_copy[key_split[0]][key_split[1]] = value
                with open(f'{self.Output_directory}/scenario_{self.scenario_id}.yaml', 'w') as f:
                    yaml.dump(config_copy, f)
                print(f"Scenario {self.scenario_id} generated for run {run_name}")
                    

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
        script_parent_dir = Path(__file__).parent.parent
        path = script_parent_dir / 'log_data'/ OUTdir_study
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

        script_parent_dir = Path(__file__).parent.parent
        log_data = script_parent_dir / 'log_data'
        os.mkdir(log_data / OUTdir_study)
        self.Output_directory = log_data / OUTdir_study
        # Step 1: Load Study Configuration
        # TODO: define xls_to_yaml function based on the study configuration
        self.load_study()
        print('Study loaded')
       
        # Step 2: Convert XLS to YAML using MATLAB function
        self.xls_to_yaml()
        
        # Step 3: run optimization
        # step 4: run simulation
        self.generate_scenarios()
        
        OUTyamlNmTxt= []
        OUTfile= []
        #TODO: update paths based on current location
        #TODO; remove parent directory from path

        #TODO: organize paths for outputs
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
    dispatcher = PipelineDispatcher(study_file_Nm="study_simple1", 
                                    config_file_Nm="StoRIES_RefCase_Config_rev04")
    dispatcher.run_pipeline()
