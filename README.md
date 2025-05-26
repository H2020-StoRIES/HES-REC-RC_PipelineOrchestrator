
# Pipeline Dispatcher

This script runs the simulation-optimization pipeline for hybrid energy systems within Renewable Energy Communities (RECs). The pipeline automates scenario creation, Simulink-based simulation, optimization via Python, and KPI evaluation.

---

## 📦 Repository Contents

- `PipelineOrchestrator.py` – Main script to execute the pipeline

---

## 🔧 Prerequisites

- **Python 3.12.0**
- **MATLAB R2024b**
  - Required Add-on: Simscape Electrical
- **CBC Solver** (for Mixed-Integer Linear Programming with Pyomo)  
  - Download from: [https://github.com/coin-or/Cbc/releases](https://github.com/coin-or/Cbc/releases)  
  - Recommended file: `Cbc-releases.2.10.12-w64-msvc17-md.zip` (for modern Windows systems)  
  - Extract it (e.g., to `C:\cbc`)  
  - Add the `C:\cbc\bin` directory to your system `Path` environment variable  
  - Open a new terminal and verify installation with:
    ```bash
    cbc --version
    ```

- **Python packages**  
  Install required packages using:
  ```bash
  pip install -r requirements.txt

---

## 🚀 How to Run the Pipeline
0. **Define the study**  
   The study setup is located in the`HES-REC-RC_Config` directory. For detailed instructions on how to define a custom study, please refer to the local `README` file within the `HES-REC-RC_Config` directory. Alternatively, you can run the default studies by following the instructions below.

1. **Choose the study**  
   In `PipelineOrchestrator.py`, uncomment the line for the study you want to run and comment the other one:
   ```python
   # dispatcher = PipelineOrchestrator(study_file_Nm="Study_difinition_Portici")
   dispatcher = PipelineOrchestrator(study_file_Nm="Study_difinition_Soria")
   ```

2. **Run the pipeline**
   ```bash
   python PipelineOrchestrator.py
   ```

---

## ⚙️ Configuration Files
- Located in `HES-REC-RC_Config`

- `Study_difinition_X.yaml` – Define study parameters, days to simulate, and scenario settings.
- `config_simulation.xlsx` – Simulink settings (number of loads, ESS units, etc.)
- `Config_Opt.xlsx` – Optimization inputs (objective weights, efficiencies, etc.)

---

## 📁 Output Structure

Results are saved in `HES-REC-RC_log_data/Study_TIMESTAMP` folders. Each contains:

- `Execution_definition.json` – Overview of study and run configs
- `scenario_run_XX_YY.yaml` – Inputs for Simulink
- `config_scenario_run_XX_YY.yaml` – Inputs and outputs for optimization
- `scenario_run_XX_YY_KPI.csv/json` – Simulink output data
- `KPI_outputs_scenario_run_XX_YY.xlsx/json` – Calculated KPIs
- `KPI_summary.xlsx` – Summary of KPIs for all runs

---

## 📊 KPI Definitions

| Abbreviation   | Description                                                  | Unit       |
|----------------|--------------------------------------------------------------|------------|
| FF             | Flexibility Factor                                           | –          |
| FF_base        | Baseline Flexibility Factor                                  | –          |
| FF_W           | Weighted Flexibility Factor                                  | –          |
| FF_SB          | Flexibility Shift Benefit                                    | EUR        |
| FF_shift       | Relative Flexibility Shift                                   | –          |
| Eff_el         | Electrical Efficiency                                        | –          |
| Eff_th         | Thermal Efficiency                                           | –          |
| Eff            | Overall System Efficiency                                    | –          |
| LCOE           | Levelized Cost of Energy                                     | EUR/kWh    |
| Capex          | Capital Expenditure                                          | EUR        |
| Annual_Opex    | Annual Operational Expenditure                               | EUR/year   |
| Opex_per_kWh   | Operational Cost per Unit Energy Delivered                   | EUR/kWh    |
| Co2_emission   | CO₂ Emission reduction vs. no-renewable baseline             | kgCO₂      |

---

## 📎 Notes

- MATLAB must be accessible from the command line (e.g., via `matlab -batch`).
- You can define any study day and system scale directly from the YAML file.

---

For more examples and log files, see the `HES-REC-RC_log_data/` repository
