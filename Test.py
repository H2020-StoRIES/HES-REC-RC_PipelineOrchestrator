import os
import subprocess
import sys

# Define case name and file paths
caseNm = 'StoRIES_RefCase_Config_rev04'
INfile = f'{caseNm}.xlsx'
INdir = f'{caseNm}_input'
OUTdir = f'{caseNm}_output'

# sys.path.append(r'C:\\Users\\szata\\Codes\\StoriesTeams\\t32-ref-case-dev')
# sys.path.append(r'C:\\Users\\szata\\Codes\\StoriesTeams\\t32-ref-case-dev\\auxFunc')


matlab_script = f"""
cd('C:\\Users\\szata\\Codes\\StoriesTeams\\t32-ref-case-dev')
addpath(genpath('auxFunc'))
t32_RefCase_ReadCfg_4xlscsv2yalm('{INfile}', '{INdir}', '{caseNm}');

"""


# Print the MATLAB script for debugging
print("MATLAB Script:", matlab_script)

# Run MATLAB process
XLS_to_Yaml_process = subprocess.Popen(
    ['matlab', '-batch', matlab_script], 
    stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE, 
    universal_newlines=True
)

stdout, stderr = XLS_to_Yaml_process.communicate()

if XLS_to_Yaml_process.returncode == 0:
    print(stdout)
else:
    print(stderr)
    
    
# Test_process = subprocess.Popen(['python', 'File1.py', "4"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# stdout, stderr = Test_process.communicate()

# if Test_process.returncode == 0:
#     print(stdout)
# else:
#     print(stderr)