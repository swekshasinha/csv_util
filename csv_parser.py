# -*- coding: utf-8 -*-
"""
Created on Sat Sep 26 18:36:56 2020

"""

import pandas as pd
import os
import numpy as np

def parse_file(file_location, dir_name):
    #file_location = csv_files[7]
    nu = pd.read_csv(file_location, sep='delimiter', header=None)
    nu.columns = ['col1']
    avg_index = [item-1 for item in list(nu[nu['col1'].str.contains('Continuous CPU %')].index)]
    avg_df = nu[nu.index.isin(avg_index)].reset_index(drop=True)
    format_avg_df = pd.DataFrame()
    col_list = ['THROUGHPUT (KV pair/second)',
                    'LATENCY (micros/KV pair)', '95 percentile latency per KV ' \
                    'pair (micros)', '99 percentile latency per KV pair (' \
                     'micros)', 'RATE (MBs/sec)', 'Avg CPU Usage (%)',
                    'Avg Memory Usage (%)', 'Compaction Started (seconds)',
                    'Compaction Ended (seconds)', 'Total Number of SST Files',
                    'Total benchmark time']

    for i in range(len(avg_df)):
        row_data = [np.nan if item=='-nan' else float(item)  for item in list((avg_df['col1'][i]).split(',')) ]
        format_avg_df = format_avg_df.append(pd.Series(row_data, index=col_list), ignore_index=True, sort=True)

    format_avg_df = format_avg_df[col_list]
    output_df = pd.DataFrame(format_avg_df.mean().to_dict(), index=[0])
    chakr_db_client_mode = nu[nu['col1'].str.contains('chakr_db_client_mode')].reset_index(drop=True)['col1'][0].split(',')[1]
    benchmark = nu[nu['col1'].str.contains('benchmark')].reset_index(drop=True)['col1'][1].split(',')[1]
    num_chakrdb_vnodes_per_instance = nu[nu['col1'].str.contains('num_chakrdb_vnodes_per_instance')].reset_index(drop=True)['col1'][0].split(',')[1]
    num_outstanding = nu[nu['col1'].str.contains('num_outstanding')].reset_index(drop=True)['col1'][0].split(',')[1]

    final_filename = chakr_db_client_mode+'_'+benchmark+'_vnodes_'+num_chakrdb_vnodes_per_instance+'_out_'+num_outstanding+'_'+file_location.split('/')[-1]
    output_df.to_csv(dir_name+final_filename, index=False)
    print('Processing Done')
    
dir_name = '/Users/home/chakr/writebm'
all_files = os.listdir(dir_name)
csv_files = []

# Making a list of all csv files in the directory
for filename in all_files:
    if filename.endswith('.csv'):
        csv_files.append(dir_name + filename)

# Parsing and writing output to new csv files
for i in range(len(csv_files)):
    parse_file(csv_files[i], dir_name)
