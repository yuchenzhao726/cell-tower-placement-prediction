import os
import pandas as pd

folder_path = 'E:/ZHAO Yuchen/NYU/23-24Fall/BigDataApplicationDev/Projects/cell-tower-placement-prediction/testsetcsv'
files = os.listdir(folder_path)
dataframes = []
for file in files:
    if file.endswith('.csv'):
        print('Loading file {}...'.format(file))
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)  # Assuming the CSV files have headers
        dataframes.append(df)

combined_df = pd.concat(dataframes, axis=0, ignore_index=True)
combined_df.to_csv(folder_path + '/combined.csv', index=False)

