import re
import glob
from typing import Union

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from datetime import date
from colorama import Fore
from pandas import DataFrame, Series

"""
Auswertung und Benchmarking von Daten aus dem VfU Kennzahlentool
Evtl auch Skript um die Daten aus mehreren Jahren zusammen zu führen.
"""

def setup_storage_dataframes(xlsx_layout_path):
    # VfU Kennzahlentool Dummy um die Struktur des Dataframes aufzubauen
    df_xls = pd.read_excel(str(xlsx_layout_path), sheet_name='C1 - Results VfU')
    periode = df_xls.loc[0, "Unnamed: 2"]
    einbezogene_mitarbeiter = df_xls.loc[4, "Unnamed: 4"]
    # df_xls = pd.read_excel(r'VfU-Kennzahlen-Dummy.xlsx', sheet_name='C1 - Results VfU')
    # Attention! Using slashes in Windows in the xlsx_path can break the system. Raw Strings are then needed (r'path')
    df_xls = df_xls.drop(index=[0, 1, 2, 3, 4, 6, 99, 100, 101, 102, 103, 104, 105, 106, 107])

    df_layout_dq = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)
    df_layout = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)

    column_names = ["bezeichnung"]

    df_layout_dq.columns = column_names
    df_layout_dq["bezeichnung"] = "DataQuali - " + df_layout_dq["bezeichnung"].astype(str)
    df_layout_dq = df_layout_dq.T
    df_layout_dq.columns = df_layout_dq.iloc[0]

    df_layout.columns = column_names
    df_layout = df_layout.T
    df_layout.columns = df_layout.iloc[0]

    df_layout: Union[DataFrame, Series] = pd.concat([df_layout, df_layout_dq], axis=1)

    df_layout['id_company'] = ""
    df_layout['id_dataset'] = ""
    df_layout['periode'] = ""
    df_layout['mitarbeiterzahl'] = ""

    df_layout = df_layout.drop("bezeichnung")

    return df_layout

def extract_data_from_xlsx2(xlsx_path: object, bank_name, dataset_name):
    df_xls = pd.read_excel(str(xlsx_path), sheet_name='C1 - Results VfU')
    periode = df_xls.loc[0, "Unnamed: 2"]
    einbezogene_mitarbeiter = df_xls.loc[4, "Unnamed: 4"]
    # df_xls = pd.read_excel(r'VfU-Kennzahlen-Dummy.xlsx', sheet_name='C1 - Results VfU')
    # Attention! Using slashes in Windows in the xlsx_path can break the system. Raw Strings are then needed (r'path')
    df_xls = df_xls.drop(index=[0, 1, 2, 3, 4, 6, 99, 100, 101, 102, 103, 104, 105, 106, 107])

    column_names = ["bezeichnung", "data"]
    column_names = ["bezeichnung", "data"]

    df_temp_dataquali = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15]], axis=1)
    df_temp_dataquali.columns = column_names
    df_temp_dataquali["bezeichnung"] = "DataQuali - " + df_temp_dataquali["bezeichnung"].astype(str)
    df_temp_dataquali = df_temp_dataquali.T
    df_temp_dataquali.columns = df_temp_dataquali.iloc[0]

    df_einbezma = df_xls.drop(df_xls.columns[[0, 1, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)
    df_einbezma.columns = column_names
    df_einbezma = df_einbezma.T
    df_einbezma.columns = df_einbezma.iloc[0]
    df_einbezma['id_company'] = bank_name
    df_einbezma['id_dataset'] = dataset_name
    df_einbezma['periode'] = periode
    df_einbezma['mitarbeiterzahl'] = einbezogene_mitarbeiter
    df_einbezma = pd.concat([df_einbezma, df_temp_dataquali], axis=1)
    df_einbezma = df_einbezma.drop("bezeichnung")

    df_percentma = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)
    df_percentma.columns = column_names
    df_percentma = df_percentma.T
    df_percentma.columns = df_percentma.iloc[0]
    df_percentma['id_company'] = bank_name
    df_percentma['id_dataset'] = dataset_name
    df_percentma['periode'] = periode
    df_percentma['mitarbeiterzahl'] = einbezogene_mitarbeiter
    df_percentma = pd.concat([df_percentma, df_temp_dataquali], axis=1)
    df_percentma = df_percentma.drop("bezeichnung")

    df_absprojahr = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)
    df_absprojahr.columns = column_names
    df_absprojahr = df_absprojahr.T
    df_absprojahr.columns = df_absprojahr.iloc[0]
    df_absprojahr['id_company'] = bank_name
    df_absprojahr['id_dataset'] = dataset_name
    df_absprojahr['periode'] = periode
    df_absprojahr['mitarbeiterzahl'] = einbezogene_mitarbeiter
    df_absprojahr = pd.concat([df_absprojahr, df_temp_dataquali], axis=1)
    df_absprojahr = df_absprojahr.drop("bezeichnung")

    df_absextrapolprojahr = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15]], axis=1)
    df_absextrapolprojahr.columns = column_names
    df_absextrapolprojahr = df_absextrapolprojahr.T
    df_absextrapolprojahr.columns = df_absextrapolprojahr.iloc[0]
    df_absextrapolprojahr['id_company'] = bank_name
    df_absextrapolprojahr['id_dataset'] = dataset_name
    df_absextrapolprojahr['periode'] = periode
    df_absextrapolprojahr['mitarbeiterzahl'] = einbezogene_mitarbeiter
    df_absextrapolprojahr = pd.concat([df_absextrapolprojahr, df_temp_dataquali], axis=1)
    df_absextrapolprojahr = df_absextrapolprojahr.drop("bezeichnung")

    df_relproma = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15]], axis=1)
    df_relproma.columns = column_names
    df_relproma = df_relproma.T
    df_relproma.columns = df_relproma.iloc[0]
    df_relproma['id_company'] = bank_name
    df_relproma['id_dataset'] = dataset_name
    df_relproma['periode'] = periode
    df_relproma['mitarbeiterzahl'] = einbezogene_mitarbeiter
    df_relproma = pd.concat([df_relproma, df_temp_dataquali], axis=1)
    df_relproma = df_relproma.drop("bezeichnung")

    df_thg = df_xls.drop(df_xls.columns[[0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15]], axis=1)
    df_thg.columns = column_names
    df_thg = df_thg.T
    df_thg.columns = df_thg.iloc[0]
    df_thg['id_company'] = bank_name
    df_thg['id_dataset'] = dataset_name
    df_thg['periode'] = periode
    df_thg['mitarbeiterzahl'] = einbezogene_mitarbeiter
    df_thg = pd.concat([df_thg, df_temp_dataquali], axis=1)
    df_thg = df_thg.drop("bezeichnung")

    # "einbezma" 4
    # "percentma" 5
    # "absprojahr" 6
    # "absextrapolprojahr" 7
    # "datenquali" 8
    # "relproma" 9
    # "thg" 12

    return df_einbezma, df_percentma, df_absprojahr, df_absextrapolprojahr, df_relproma, df_thg

def add_data_to_data_storage(df_storage, xlsx_path, bank_name, dataset_name):
    df_einbezma, df_percentma, df_absprojahr, df_absextrapolprojahr, df_relproma, df_thg = extract_data_from_xlsx2(
        xlsx_path, bank_name, dataset_name)
    df_storage_absprojahr = df_storage.append(df_absprojahr)
    df_storage_absextrapolprojahr = df_storage.append(df_absextrapolprojahr)
    df_storage_relproma = df_storage.append(df_relproma)
    df_storage_thg = df_storage.append(df_thg)
    return df_storage_absprojahr, df_storage_absextrapolprojahr, df_storage_relproma, df_storage_thg

def file_accessible(file):
    # Check if a file exists and is accessible.
    try:
        f = open(file, mode = "r")
        f.close()
    except IOError as e:
        return False
    return True

def sheet_accesible(file, sheet_name):
    # Check if a sheet in an xls or xlsx file exists and is accessible.
    if file_accessible(file):
        excel_file = pd.ExcelFile(str(file))
        sheets = excel_file.sheet_names
        searched_sheet = sheet_name
        if searched_sheet in sheets:
            return True
        else:
            return False
    return False

def save_data_frame(df_tobesaved, note="", mode="n"):
    if mode in ["append", "a"]:
        filename = "vfu_benchmark_" + str(note) + "_" + str(date.today()) + ".csv"
        if not file_accessible(filename):
            df_tobesaved.to_csv(filename, index=True)
        else:
            df_tobesaved.sort_index(axis=1).to_csv(filename, header=None, mode='a')
    elif mode in ["new", "n"]:
        filename = "vfu_benchmark" + str(note) + "_" + str(date.today()) + ".csv"
        if not file_accessible(filename):
            df_tobesaved.to_csv(filename, index=True)
        else:
            filename = "vfu_benchmark" + str(note) + "_" + str(date.today()) + ".csv"
            counter = 1
            while file_accessible(filename):
                counter += 1
                filename = "vfu_benchmark" + str(date.today()) + "_" + str(counter) + ".csv"
            df_tobesaved.to_csv(filename, index=True)
    else:
        print(Fore.RED + "File not successfully saved in mode ", str(mode))

    print(Fore.GREEN + "File successfully saved in mode ", str(mode))
    return

def load_data_frame(path):
    """    import csv

    list_of_email_addresses = []
    with open('users.csv', newline='') as users_csv:
        user_reader = csv.DictReader(users_csv)
        for row in user_reader:
            list_of_email_addresses.append(row['Email'])"""
    return

def get_demografic_data(df_storage):
    df_demografics = df_storage["id_company", "id_dataset", "periode", "mitarbeiterzahl"]
    df_demografics = df_demografics.drop_duplicates()
    # df_demografics = df_demgrafics.drop_duplicates(subset = ["id_dataset"])
    return df_demografics

def get_path_files(path):
    # Get filenames of XLSX data of the path
    filenames = glob.glob(path + "/*.xlsx")
    for file in filenames:
        if file_accessible(file):
            var = None
        else:
            print(Fore.RED + "Error with file ", str(file))
            break
    print(Fore.GREEN + "Files are accessible!")

    # print(filenames)
    return filenames

def get_standort(file):
    if file_accessible(file):
        sheet_name = "Stammdaten"
        if sheet_accesible(file, sheet_name):
            try:
                df_xls = pd.read_excel(str(file), sheet_name='Stammdaten')
                standort = df_xls.loc[5, "Unnamed: 1"]
                return standort
            except IOError as e:
                print("File " + str(file) + " results error: " + str(e))
                return
        else:
            print("Sheet " + str(sheet_name) + " ist not accessible!")
    else:
        print("File " + str(file) + " ist not accessible!")
        return

def get_berichtsperiode(file):
    if file_accessible(file):
        sheet_name = "Stammdaten"
        if sheet_accesible(file, sheet_name):
            try:
                df_xls = pd.read_excel(str(file), sheet_name='Stammdaten')
                berichtsperiode = df_xls.loc[13, "Unnamed: 1"]
                return berichtsperiode
            except IOError as e:
                print("File " + str(file) + " results error: " + str(e))
                return
        else:
            print("Sheet " + str(sheet_name) + " ist not accessible!")
    else:
        print("File " + str(file) + " ist not accessible!")
        return

def get_systemgrenze(file):
    if file_accessible(file):
        sheet_name = "Stammdaten"
        if sheet_accesible(file, sheet_name):
            try:
                df_xls = pd.read_excel(str(file), sheet_name='Stammdaten')
                systemgrenze = df_xls.loc[15, "Unnamed: 1"]
                return systemgrenze
            except IOError as e:
                print("File " + str(file) + " results error: " + str(e))
                return
        else:
            print("Sheet " + str(sheet_name) + " ist not accessible!")
    else:
        print("File " + str(file) + " ist not accessible!")
        return

def get_gesamtmazahl(file):
    if file_accessible(file):
        sheet_name = "Stammdaten"
        if sheet_accesible(file, sheet_name):
            try:
                df_xls = pd.read_excel(str(file), sheet_name='Stammdaten')
                mazahl = df_xls.loc[25, "Unnamed: 1"]
                return mazahl
            except IOError as e:
                print("File " + str(file) + " results error: " + str(e))
                return
        else:
            print("Sheet " + str(sheet_name) + " ist not accessible!")
    else:
        print("File " + str(file) + " ist not accessible!")
        return

def get_metadata(file):
    year = get_berichtsperiode(file)
    name = file.split("/", -1)
    name = name[-1]
    return name, year

def get_path_combined_df(filenames):
    # Input filenames

    # Dataframe Initialization
    concat_all_sheets_all_files = pd.DataFrame()

    for file in filenames:
        # Get all the sheets in a single Excel File using  pd.read_excel command, with sheet_name=None
        # Note that the result is given as an Ordered Dictionary File
        # Hell can be found here: https://pandas.pydata.org/pandas-docs...
        sheet_name = 'C1 - Results VfU'
        if sheet_accesible(file, sheet_name):
            df = pd.read_excel(file, sheet_name=sheet_name)
            if sheet_accesible(file, "Stammdaten"):
                standort = get_standort(file)
                periode = get_berichtsperiode(file)
                df["Standort"] = standort
                df["Periode"] = periode
                # df = pd.read_excel(file, sheet_name=None, skiprows=0,nrows=34,usecols=105,header = 9,index_col=None)
                
        # Use pd.concat command to Concatenate pandas objects as a Single Table.
            concat_all_sheets_single_file = pd.concat([df], sort=False)
            
        # Use append command to append/stack the previous concatenated data on top of each other
        # as the iteration goes on for every files in the folder

            concat_all_sheets_all_files = concat_all_sheets_all_files.append(concat_all_sheets_single_file)
        # print(concat_all_sheets)
        else:
            print("Sheet " + str(sheet_name) + " nicht verfügbar!")
    return concat_all_sheets_all_files


# Data Path
xlsx_path = "VfU-Kennzahlen-Dummy.xlsx"
xlsx_layout_path = "VfU-Kennzahlen-Dummy.xlsx"

path = r"/Users/martinhillenbrand/Nextcloud/6 Projekte/VfU Kennzahlentool - Benchmarking/Ausgefüllte Tools/"
filenames = get_path_files(path)
dummy_file = filenames[1]

#Print Meta Daten Übersicht
for file in filenames:
    name, year = get_metadata(file)
    print(str(year) + " - " + str(name))
