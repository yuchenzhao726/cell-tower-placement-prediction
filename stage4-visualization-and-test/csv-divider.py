import os
import pandas as pd

file_path = 'E:/ZHAO Yuchen/NYU/23-24Fall/BigDataApplicationDev/Projects/cell-tower-placement-prediction/stage3-placement-prediction/placement-prediction.csv'
chunksize = 2000
i = 0
for chunk in pd.read_csv(file_path, chunksize=chunksize):
    output_file = os.path.join(os.path.dirname(file_path), f'chunk_{i}.csv')
    chunk.to_csv(output_file, index=False)
    i += 1