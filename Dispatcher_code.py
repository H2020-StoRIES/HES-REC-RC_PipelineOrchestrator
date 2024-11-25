import yaml
import subprocess
import json
import os
#What we need:
#1. Study parameters and the structure of the study file
#2. making configuration files for each run out of the study file and the initial configuration file
# = generate_run_configs
#3. Having optimizaton output files for each run or at least a single output file as an example
#4. Missing Matlab File related to xls_to_yaml conversion
#5. Missing python File related to optimization
path= './..'
class PipelineDispatcher:
    def __init__(self, study_file, config_file):
        self.study_file = study_file
        self.config_file = config_file
        self.config_yaml = "Config_basic.yaml"
        self.runs = []
    
    def xls_to_yaml(self):
        """Convert Config.xlsx to YAML format using a MATLAB function."""
        print("Converting XLS to YAML using MATLAB function...")
        result = subprocess.run(["matlab", "-batch", "xls_to_yaml"], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print("Error in xls_to_yaml conversion")
            return False
        print("Config file converted to YAML by MATLAB.")
        return True
    
    def load_study(self):
        """Load study parameters from YAML file."""
        with open(self.study_file, 'r') as file:
            study_data = yaml.safe_load(file)
        
        self.runs = study_data.get("runs", [])
        print(f"Loaded study data with {len(self.runs)} runs.")
    
    def generate_run_configs(self):
        """Generate YAML and JSON configurations for each run."""
        for run in self.runs:
            run_id = run.get("id")
            yaml_filename = f"Cof_run{run_id}.yaml"
            json_filename = f"Cof_run{run_id}.json"

            """Generate configuration information for each run based on the initial."""

            
            # Save YAML
            with open(yaml_filename, 'w') as yaml_file:
                yaml.dump(run, yaml_file)
            
            # Save JSON
            with open(json_filename, 'w') as json_file:
                json.dump(run, json_file)
            
            print(f"Generated config files for run {run_id}.")
    
    def execute_optimization(self, run_id):
        """Execute optimization for a given run."""
        print(f"Running optimization for run {run_id}...")
        result = subprocess.run(["python", "-batch", f"optimization('{run_id}')"], capture_output=True, text=True)
        print(result.stdout)
        return result.returncode
    
    def execute_simulation(self, run_id):
        """Execute simulation for a given run."""
        print(f"Running simulation for run {run_id}...")
        result = subprocess.run(["matlab", "-batch", f"simulink('{run_id}')"], capture_output=True, text=True)
        print(result.stdout)
        return result.returncode
    
    def calculate_kpis(self, run_id):
        """Calculate KPIs for a given run."""
        print(f"Calculating KPIs for run {run_id}...")
        result = subprocess.run(["python", "kpi_calculation.py", f"Out_sim_run{run_id}.yaml"], capture_output=True, text=True)
        print(result.stdout)
        return result.returncode
    
    def batch_kpi_calculation(self):
        """Batch calculation of KPIs across all runs."""
        print("Calculating batch KPIs...")
        result = subprocess.run(["python", "kpi_calculation_batch.py", "study.yaml"], capture_output=True, text=True)
        print(result.stdout)
        return result.returncode
    
    def run_pipeline(self):
        """Main method to run the entire pipeline."""
        # Step 1: Convert XLS to YAML using MATLAB function
        if not self.xls_to_yaml():
            print("Pipeline stopped due to XLS to YAML conversion error.")
            return
        
        # Step 2: Load Study Configuration
        self.load_study()
        
        # Step 3: Generate Configuration Files for Each Run
        self.generate_run_configs()
        
        # Step 4: Run Optimization and Simulation for Each Run
        for run in self.runs:
            run_id = run.get("id")
            if self.execute_optimization(run_id) != 0:
                print(f"Optimization failed for run {run_id}")
                continue
            
            if self.execute_simulation(run_id) != 0:
                print(f"Simulation failed for run {run_id}")
                continue
            
            # Step 5: Calculate KPIs for Each Run
            self.calculate_kpis(run_id)
        
        # Step 6: Calculate Batch KPIs
        self.batch_kpi_calculation()

# Example usage
if __name__ == "__main__":
    dispatcher = PipelineDispatcher(study_file="Study.yaml", config_file="Config.xlsx")
    dispatcher.run_pipeline()
