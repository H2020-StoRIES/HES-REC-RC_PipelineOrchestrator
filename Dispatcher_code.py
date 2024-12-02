import yaml
import subprocess
import os


class PipelineDispatcher:
    def __init__(self, study_file_Nm, config_file_Nm):
        self.study_file_Nm = study_file_Nm
        self.config_file_Nm = config_file_Nm
        self.study_file = f'{study_file_Nm}.yaml'
        self.config_file= f'{config_file_Nm}.yaml'
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
        
        config= self.read_yaml(self.config_file_Nm, self.path_simulation)
        for run_key, configurable_values in self.study_data.items():
            if run_key.startswith('run_'):
                conf_run = config.copy()
                self.update_conf_run(conf_run, configurable_values)
                with open(f'conf_{run_key}.yaml', 'w') as f:
                    yaml.safe_dump(conf_run, f)
                    print(f"Generated config files for {run_key}.")

    def execute_optimization(self, run_id):
        pass
    
    def execute_simulation(self, run_id):
        OUTyamlNmTxt= f'conf_{run_id}.yaml'
        print(f'simulation is running for file: {OUTyamlNmTxt}')
        OUTdir = f'conf_{run_id}_output'
        
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

            INfile = [caseNm '.xlsx']; INdir = [caseNm,'_input']; 

            [out]=t32_RefCase_RunSlx_4yalm2out('{OUTyamlNmTxt}','{OUTdir}','{self.MDLfile}',cfgON,plotON,UTfile);


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
    
    def batch_kpi_calculation(self):
        pass

    def run_keys(self):
        
            for run_key in self.study_data.keys():
                if run_key.startswith('run_'):
                    self.runs.append(run_key)

    def run_pipeline(self):
        # # Step 1: Convert XLS to YAML using MATLAB function
        # self.xls_to_yaml()
        
        # Step 2: Load Study Configuration
        self.load_study()
        print('Study loaded')
        
        # # Step 3: Generate Configuration Files for Each Run
        # self.generate_run_configs()
        # Step 3.1: Get Run Keys
        self.run_keys()

        print('Runs generated')
        print(f'runs: {self.runs}')

        # Step 4: Run Optimization and Simulation for Each Run
        for run_id in self.runs:


            # Step 4.1: Run Optimization
            # if self.execute_optimization(run_id) != 0:
            #     print(f"Optimization failed for run {run_id}")
            #     continue

            # Step 4.2: Run Simulation
            if self.execute_simulation(run_id) != 0:
                print(f"Simulation failed for run {run_id}")
                continue
            
            # Step 5: Calculate KPIs for Each Run
            self.calculate_kpis(run_id)
        
        # Step 6: Calculate Batch KPIs
        self.batch_kpi_calculation()

# Example usage
if __name__ == "__main__":
    dispatcher = PipelineDispatcher(study_file_Nm="study", 
                                    config_file_Nm="StoRIES_RefCase_Config_rev04")
    dispatcher.run_pipeline()
