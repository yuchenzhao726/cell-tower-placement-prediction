import os
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader, TensorDataset
from IPython.display import display
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from tqdm import tqdm

# Function to load CSV files from a folder
def load_data_from_folder(folder_path):
    files = os.listdir(folder_path)
    dataframes = []
    for file in files:
        if file.endswith('.csv'):
            print('Loading file {}...'.format(file))
            file_path = os.path.join(folder_path, file)
            df = pd.read_csv(file_path)  # Assuming the CSV files have headers
            dataframes.append(df)
    return dataframes

# Function to preprocess data
def preprocess_data(combined_df):

    # Extract features and target columns
    features = combined_df.drop(['score', 'lon', 'lat', 'n', 'd'], axis=1)
    scaler = MinMaxScaler()
    features = scaler.fit_transform(features)
    
    # Convert data to PyTorch tensors
    features_tensor = torch.tensor(features, dtype=torch.float32)
    return features_tensor

# Load data from folder
folder_path = 'E:/ZHAO Yuchen/NYU/23-24Fall/BigDataApplicationDev/Projects/cell-tower-placement-prediction/testsetcsv/'
dataframes = load_data_from_folder(folder_path)
# Combine all dataframes into a single dataframe
combined_df = pd.concat(dataframes, ignore_index=True)

combined_df.fillna(0, inplace=True)
combined_df.drop_duplicates(inplace=True)
combined_df.reset_index(drop=True, inplace=True)

features = preprocess_data(combined_df)

# load the model
class Model(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(Model, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        x = self.sigmoid(x)
        return x

model = Model(22, 100, 1)
model.load_state_dict(torch.load('E:/ZHAO Yuchen/NYU/23-24Fall/BigDataApplicationDev/Projects/cell-tower-placement-prediction/model-former.pth'))
model.eval()

# inference
with torch.no_grad():
    output = model(features)
    print(output)
    output = output.numpy()

# concat the output with the original longitude and latitude
idx = combined_df.index.values
lon = combined_df['lon'].values
lat = combined_df['lat'].values

result = np.column_stack((idx, lon, lat, output))

def find_max_score(result):
    max_score = result[0][3]
    max_idx = 0
    for i in range(len(result)):
        if result[i][3] > max_score:
            max_score = result[i][3]
            max_idx = i
    return max_idx

import math
# original algorithm: 1 / (2 + 47 * d) + 0.5 * (1 - 1 / math.pow(math.log(n + 40, 40), 2))
def adjust_score(result, lon, lat):
    for i in range(len(result)):
        distance = math.sqrt((result[i][1]-lon)**2 + (result[i][2]-lat)**2)
        if distance < 0.4:
            result[i][3] -= (0.000016 / (distance**2 + 0.000016))
    return result

# predict the placement
import csv
# N = 7386
N = 1000
with open('E:/ZHAO Yuchen/NYU/23-24Fall/BigDataApplicationDev/Projects/cell-tower-placement-prediction/stage3-placement-prediction/placement-prediction.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['lon', 'lat', 'score'])
    for i in tqdm(range(N)):
        max_idx = find_max_score(result)
        writer.writerow([result[max_idx][1], result[max_idx][2], result[max_idx][3]]) # save the result
        result = adjust_score(result, result[max_idx][1], result[max_idx][2])