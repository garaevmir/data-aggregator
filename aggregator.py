import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging as lg

INPUT_DIR = 'input'
OUTPUT_DIR = 'output'
INTERMEDIATE_DIR = 'intermediate'

def LoadDataLogs(date):
    file_path = f'{INPUT_DIR}/{date.strftime("%Y-%m-%d")}.csv'
    if os.path.exists(file_path):
        return pd.read_csv(file_path, names=["email", "action", "dt"])
    lg.error(f'No logs for {date.strftime("%Y-%m-%d")}')
    return pd.DataFrame(columns=["email", "action", "dt"])

def LoadIntermediateData(date):
    intermediate_file = f'{INTERMEDIATE_DIR}/{date.strftime("%Y-%m-%d")}.csv'
    if os.path.exists(intermediate_file):
        return pd.read_csv(intermediate_file)
    return pd.DataFrame(columns=["email", "create_count", "read_count", "update_count", "delete_count"])

def AggregateData(data):
    aggregated = data.pivot_table(index='email', columns='action', aggfunc='size', fill_value=0).reset_index()
    aggregated.columns = ['email', 'create_count', 'delete_count', 'read_count', 'update_count']
    return aggregated

def SaveData(data, date, target):
    file_name = f'{target}/{date.strftime("%Y-%m-%d")}.csv'
    data.to_csv(file_name, index=False)

def ProcessLogs(target_date):
    date = datetime.strptime(target_date, '%Y-%m-%d')
    dates = [(date - timedelta(days=i)) for i in range(1, 8)]
    
    data_frames = []
    for day in dates:
        intermediate_data = LoadIntermediateData(day)
        if intermediate_data.empty:
            daily_data = LoadDataLogs(day)
            if not daily_data.empty:
                intermediate_data = AggregateData(daily_data)
                SaveData(intermediate_data, day, INTERMEDIATE_DIR)
        
        data_frames.append(intermediate_data)
    
    report = pd.concat(data_frames).groupby('email').sum().reset_index()
    SaveData(report, date, OUTPUT_DIR)

if __name__ == "__main__":
    lg.basicConfig(filename = "aggregator.log")
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    if not os.path.exists(INTERMEDIATE_DIR):
        os.makedirs(INTERMEDIATE_DIR)
    
    if len(sys.argv) != 2:
        print("Please provide date: python script.py <YYYY-mm-dd>")
        sys.exit(1)
    
    target_date = sys.argv[1]
    
    ProcessLogs(target_date)
