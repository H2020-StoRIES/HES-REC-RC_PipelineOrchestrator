import csv
import yaml
import pandas as pd

def Read_data (file_path, data_type, Day= 1):
    with open(file_path, mode='r', newline='') as file:
        csv_reader = pd.read_csv(file)
        # data_list = [[time, bat] for time, bat in zip(csv_reader['Time'], csv_reader['BAT_ESSm_PP'])]
        data_list= csv_reader['BAT_ESSm_PP'].tolist()
        print(data_list)


    
Read_data('TP_Soria_CtBu_Pt_008_001.csv', 'Diary')