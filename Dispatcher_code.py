import yaml
import subprocess
import json
import os
import sys


class PipelineDispatcher:
    def __init__(self, study_file_Nm, config_file_Nm):
        self.study_file_Nm = study_file_Nm
        self.config_file_Nm = config_file_Nm
        self.study_file = f'{study_file_Nm}.yaml'
        self.runs = []
        self.plotON  = 1
        self.cfgON   = 1 
        self.OUTdir = f'{self.config_file_Nm}_output'
        self.UTfile  = ''
        
        self.MDLfile = 'ElectricSys_CEDERsimple01'
        self.INfile = f'{self.config_file_Nm}.xlsx'
        self.INdir = f'{self.config_file_Nm}_input'
        self.path_simulation= 'C:\\Users\\szata\\Codes\\StoriesTeams\\t32-ref-case-dev'
        self.path_dispatcher= 'C:\\Users\\szata\\Codes\\StoriesTeams\\Pipeline_dispatcher_repo'
    
    def xls_to_yaml(self):
        
        matlab_script = f"""
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

    def read_yaml(self, filename):
        with open(f'{filename}.yaml','r') as f:
            output = yaml.safe_load(f)
        return output
    
    def load_study(self):
        """Load study parameters from YAML file."""
        print(self.study_file)
        with open(self.study_file, 'r') as file:
            self.study_data = yaml.safe_load(file)


    def update_conf_run(self, conf_run, configurable_values):
        for key, value in configurable_values.items():
            if isinstance(value, dict):
                self.update_conf_run(conf_run[key], value)
            else:
                if value not in ['default_value', 'default_name', 'default_list']:
                    conf_run[key] = value
                
    def generate_run_configs(self):
        
        #TODO: replace config_example with the actual config file
        #Problem: path to the config file (?)
        config= self.read_yaml('config_example')
        # self.runs = []
        # i=0
        for run_key, configurable_values in self.study_data.items():
            if run_key.startswith('run_'):
                # self.runs.append(run_key)
                conf_run = config.copy()
                self.update_conf_run(conf_run, configurable_values)
                with open(f'conf_{run_key}.yaml', 'w') as f:
                    yaml.safe_dump(conf_run, f)
                    print(f"Generated config files for {run_key}.")
        # print(self.runs)
    def execute_optimization(self, run_id):
        pass
        # """Execute optimization for a given run."""
        # print(f"Running optimization for run {run_id}...")
        # result = subprocess.run(["python", "-batch", f"optimization('{run_id}')"], capture_output=True, text=True)
        # print(result.stdout)
        # return result.returncode
    
    def execute_simulation(self, run_id):
        OUTyamlNmTxt= f'conf_{run_id}.yaml'
        print(OUTyamlNmTxt)
        matlab_script = f"""
            cd('{self.path_simulation}');
            addpath(genpath('auxFunc'));
            addpath(genpath('{self.path_dispatcher}'));
            t32_RefCase_RunSlx_4yalm2out('{OUTyamlNmTxt}','{self.OUTdir}','{self.MDLfile}','{self.cfgON}','{self.plotON}','{self.UTfile}');

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
    
    def calculate_kpis(self, run_id):
        pass
        # """Calculate KPIs for a given run."""
        # print(f"Calculating KPIs for run {run_id}...")
        # result = subprocess.run(["python", "kpi_calculation.py", f"Out_sim_run{run_id}.yaml"], capture_output=True, text=True)
        # print(result.stdout)
        # return result.returncode
    
    def batch_kpi_calculation(self):
        pass
        # """Batch calculation of KPIs across all runs."""
        # print("Calculating batch KPIs...")
        # result = subprocess.run(["python", "kpi_calculation_batch.py", "study.yaml"], capture_output=True, text=True)
        # print(result.stdout)
        # return result.returncode
    def run_keys(self):
            for run_key in self.study_data.keys():
                if run_key.startswith('run_'):
                    self.runs.append(run_key)

    def run_pipeline(self):
        # """Main method to run the entire pipeline."""
        # # Step 1: Convert XLS to YAML using MATLAB function
        # self.xls_to_yaml()
        
        # Step 2: Load Study Configuration
        self.load_study()
        print('Study loaded')
        
        # Step 3: Generate Configuration Files for Each Run
        # self.generate_run_configs()
        self.run_keys()
        print('Runs generated')
        print(self.runs)
        # Step 4: Run Optimization and Simulation for Each Run
        for run in self.runs:
            run_id = run
            # if self.execute_optimization(run_id) != 0:
            #     print(f"Optimization failed for run {run_id}")
            
            if self.execute_simulation(run_id) != 0:
                print(f"Simulation failed for run {run_id}")
            
            # Step 5: Calculate KPIs for Each Run
            self.calculate_kpis(run_id)
        
        # Step 6: Calculate Batch KPIs
        self.batch_kpi_calculation()

# Example usage
if __name__ == "__main__":
    path1= r'C:\Users\szata\Codes\StoriesTeams\t32-ref-case-dev'
    file_path = os.path.join(os.path.dirname(__file__), path1)
    dispatcher = PipelineDispatcher(study_file_Nm="study", config_file_Nm="StoRIES_RefCase_Config_rev04")
    dispatcher.run_pipeline()
