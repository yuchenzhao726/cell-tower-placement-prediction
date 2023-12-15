import os
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader, TensorDataset
from IPython.display import display
from sklearn.preprocessing import MinMaxScaler

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
def preprocess_data(dataframes):
    # Combine all dataframes into a single dataframe
    combined_df = pd.concat(dataframes, ignore_index=True)
    # print('dataframe shape {}'.format(combined_df.shape))
    display(combined_df)

    combined_df.fillna(0, inplace=True)
    combined_df.drop_duplicates(inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    # Extract features and target columns
    features = combined_df.drop(['score', 'lon', 'lat'], axis=1)
    scaler = MinMaxScaler()
    features = scaler.fit_transform(features)
    target = combined_df['score']
    
    # Convert data to PyTorch tensors
    features_tensor = torch.tensor(features, dtype=torch.float32)
    target_tensor = torch.tensor(target, dtype=torch.float32).reshape(-1, 1)
    print('features_tensor.shape {}, target_tensor.shape {}'.format(features_tensor.shape, target_tensor.shape))
    return features_tensor, target_tensor

# Load data from folder
folder_path = 'E:/ZHAO Yuchen/NYU/23-24Fall/BigDataApplicationDev/Projects/cell-tower-placement-prediction/stage2-point-scorer/data/stage2'
dataframes = load_data_from_folder(folder_path)
features, target = preprocess_data(dataframes)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=31)
print('X_train.shape {}, y_train.shape {}'.format(X_train.shape, y_train.shape))

# Create PyTorch DataLoader
train_dataset = TensorDataset(X_train, y_train)
train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

# Define the model
class Model(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(Model, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, hidden_size)
        self.relu2 = nn.ReLU()
        self.fc3 = nn.Linear(hidden_size, output_size)
        
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        x = self.relu2(x)
        x = self.fc3(x)
        return x

# train the model
model = Model(22, 100, 1)
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=0.01)
num_epochs = 30
for epoch in range(num_epochs):
    for i, (features, target) in enumerate(train_loader):
        optimizer.zero_grad()
        output = model(features)
        loss = criterion(output, target)
        loss.backward()
        optimizer.step()
        if (i + 1) % 10 == 0:
            print('Epoch [{}/{}], Step [{}/{}], Loss: {:.4f}'.format(epoch + 1, num_epochs, i + 1, len(train_loader), loss.item()))

# Save the model
torch.save(model.state_dict(), 'model.pth')
print('Saved PyTorch Model State to model.pth')

# Evaluate the model
model.eval()
with torch.no_grad():
    y_pred = model(X_test)
    loss = criterion(y_pred, y_test)
    print('Test loss: {:.4f}'.format(loss.item()))
    print('y_pred.shape {}, y_test.shape {}'.format(y_pred.shape, y_test.shape))

    # Save the predictions to a CSV file
    y_pred = y_pred.numpy()
    y_test = y_test.numpy()
    df = pd.DataFrame({'y_pred': y_pred.flatten(), 'y_test': y_test.flatten()})
    df.to_csv('predictions.csv', index=False)
    print('Saved predictions to predictions.csv')
