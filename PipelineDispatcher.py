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
        param_ranges = self.study_data['study_param_range']
        print(f"Generating scenarios for the following parameter ranges: {param_ranges}")
        scenarios = self.generate_combinations(param_ranges)
        study_run_dicts= self.study_data['study_run_dicts']
        scenario_id = 0
        for run_name, run_values in study_run_dicts.items():
            for idx, scenario in enumerate(scenarios):
                scenario_id += 1
                print(f"Generating scenario {scenario_id} for run {run_name}")
                with open(f'{self.Output_directory}/scenario_{scenario_id}.yaml', 'w') as f:
                    config = scenario.copy()
                    #TODO: Check if this is the correct way to update the config dictionary
                    #TODO: check flattened hierarchical keys
                    

    def execute_optimization(self, run_id):
        pass
    
    def execute_simulation(self, run_id, OUTdir_study):
        OUTyamlNmTxt= f'conf_{run_id}'
        print(f'simulation is running for file: {OUTyamlNmTxt}')
        
        

        matlab_script = f"""
            clear all; restoredefaultpath %%%%%%%%%%%%%%%
            clearvars -except INstruct; restoredefaultpath
            cd('{self.path_simulation}');
            addpath(genpath('dataSim')); CIEMAT_EDLC_SC_load
            addpath(genpath('auxFunc'))
            addpath(genpath('{self.path_dispatcher}'))
            %%
            caseNm  = '{self.config_file_Nm}';
            UTfile  = '';
            plotON  = 1;
            cfgON   = 1; 
            OUTdir  = ['..\\log_data\\{OUTdir_study}\\{OUTyamlNmTxt}'];
            INfile = [caseNm '.xlsx']; 
            INdir = [caseNm,'_input']; 
            OUTnm= 'Output';
            [out]=t32_RefCase_RunSlx_4yalm2out('{OUTyamlNmTxt}.yaml',OUTdir,OUTnm,'{self.MDLfile}',cfgON,plotON,UTfile);
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
        path = script_parent_dir / 'log_data'/ OUTdir_study/f'conf_{run_id}'
        print(f'path: {path}')
        kpi_script_path = os.path.join(self.path_kpi_calculation, 'KPI_evaluation.py')
        subprocess.Popen(['python', kpi_script_path, 'Output_KPI',path, run_id])
           
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
        self.load_study()
        print('Study loaded')
       
        # Step 2: Convert XLS to YAML using MATLAB function
        # self.xls_to_yaml()
        
        # Step 3: Generate Configuration Files for Each Run
        self.generate_scenarios()
        

        # Step 3.1: Get Run Keys
        self.run_keys()

        print('Runs generated')
        print(f'runs: {self.runs}')
        
        # Step 4: Run Optimization and Simulation for Each Run
        for run_id in self.runs:


            # Step 4.1: Run Optimization
            if self.execute_optimization(run_id) != 0:
                print(f"Optimization failed for run {run_id}")
            else:
                print(f"Optimization completed for run {run_id}")

            # Step 4.2: Run Simulation
            if self.execute_simulation(run_id, OUTdir_study) != 0:
                print(f"Simulation failed for run {run_id}")
            else:
                print(f"Simulation completed for run {run_id}")
            
            # Step 5: Calculate KPIs for Each Run
            self.calculate_kpis(run_id, OUTdir_study)
        
        # Step 6: Calculate Batch KPIs
        self.batch_kpi_calculation()

# Example usage
if __name__ == "__main__":
    dispatcher = PipelineDispatcher(study_file_Nm="study_simple", 
                                    config_file_Nm="StoRIES_RefCase_Config_Simple")
    dispatcher.run_pipeline()
