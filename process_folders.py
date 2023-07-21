import os
import pandas as pd

import process_results as results

def groupby(data, groupby='Size (Bytes)', groupby_type=int):
    # Convert column to desired type
    data[groupby] = data[groupby].astype(groupby_type)
    
    # Group the data by specified column
    grouped = data.groupby(groupby)

    return grouped

def na_ratio(data, column='Retrieval Time (ms)'):
    data = data[column]

    total_values = len(data)
    na_values = data.isna().sum()
    na_ratio = (na_values / total_values) * 100

    results = pd.DataFrame({
        'Total': [total_values],
        'NAs': [na_values],
        'NA Ratio (%)': [na_ratio]
    })

    return results

def process_experiments(root_dir, column, functions):
    for dirName, subdirList, fileList in os.walk(root_dir):
        print('Found directory:', dirName)
        found_csv_foler = False
        all_data = pd.DataFrame()

        for fname in fileList:
            if fname.endswith('.csv'):
                print('\tProcessing file:', fname)
                file_path = os.path.join(dirName, fname)
                found_csv_foler = True

                data = results.read_all(file_path, dropna=False)
                all_data = pd.concat([all_data, data], ignore_index=True)
                    
        # Process the data only after all CSV files have been appended
        if found_csv_foler:
            for func_info in functions:
                data = all_data.copy()

                dropna = func_info.get('dropna', True)
                if dropna:
                    data.dropna(inplace=True)
                
                if 'data_load_function' in func_info:
                    args = func_info['args']
                    data_load_function = func_info['data_load_function']
                    data = data_load_function(data, *args)
                
                function = func_info['function']

                if 'data_process_function' in func_info:
                    process_function = func_info['data_process_function']
                    data = process_function(data)

                data = function(data)

                if 'output_function' in func_info:
                    output_function = func_info['output_function']

                    if 'output_dir' in func_info:
                        output_dir = func_info['output_dir']
                        parent_dir = os.path.dirname(dirName)
                        grand_parent_dir = os.path.dirname(dirName)
                        output_dir = os.path.join(os.path.dirname(parent_dir), os.path.basename(parent_dir) + '_processed_by_experiment', output_dir)

                    output_function(data, output_dir, os.path.basename(dirName))

            del subdirList[:]  # Prevent os.walk from descending into subdirectories

def process_all(root_dir, column, functions):
    all_data = pd.DataFrame()

    for dirName, subdirList, fileList in os.walk(root_dir):
        print('Found directory:', dirName)
        found_csv_foler = False

        for fname in fileList:
            if fname.endswith('.csv'):
                print('\tProcessing file:', fname)
                file_path = os.path.join(dirName, fname)
                found_csv_foler = True

                data = results.read_all(file_path, dropna=False)
                all_data = pd.concat([all_data, data], ignore_index=True)
                    
    # Process the data only after all CSV files have been appended
    if found_csv_foler:
        for func_info in functions:
            data = all_data.copy()

            dropna = func_info.get('dropna', True)
            if dropna:
                data.dropna(inplace=True)
            
            if 'data_load_function' in func_info:
                args = func_info['args']
                data_load_function = func_info['data_load_function']
                data = data_load_function(data, *args)
            
            function = func_info['function']

            if 'data_process_function' in func_info:
                process_function = func_info['data_process_function']
                data = process_function(data)

            data = function(data)

            if 'output_function' in func_info:
                output_function = func_info['output_function']

                if 'output_dir' in func_info:
                    output_dir = func_info['output_dir']
                    output_dir = os.path.join(os.path.dirname(root_dir), os.path.basename(root_dir) + '_processed', output_dir)

                output_function(data, output_dir, 'all')

        del subdirList[:]  # Prevent os.walk from descending into subdirectories

def process_clients(root_dir, column, functions):
    clients = ['degroot', 'miletus', 'nancy', 'lille', 'grenoble', 'sophia', 'rennes']
    clients_dic = {}
    for client in clients:
        clients_dic[client] = pd.DataFrame()

    # all_data = pd.DataFrame()

    for dirName, subdirList, fileList in os.walk(root_dir):
        print('Found directory:', dirName)
        found_csv_foler = False

        for fname in fileList:
            if fname.endswith('.csv'):
                for client in clients_dic:
                    if client in fname:
                        print('\tProcessing file:', fname)
                        file_path = os.path.join(dirName, fname)
                        found_csv_foler = True

                        data = results.read_all(file_path, dropna=False)
                        clients_dic[client] = pd.concat([clients_dic[client], data], ignore_index=True)
                    
    # Process the data only after all CSV files have been appended
    if found_csv_foler:
        for func_info in functions:
            for client in clients_dic:
                data = clients_dic[client].copy()

                if len(data):
                    dropna = func_info.get('dropna', True)
                    if dropna:
                        data.dropna(inplace=True)
                    
                    if 'data_load_function' in func_info:
                        args = func_info['args']
                        data_load_function = func_info['data_load_function']
                        data = data_load_function(data, *args)
                    
                    function = func_info['function']

                    if 'data_process_function' in func_info:
                        process_function = func_info['data_process_function']
                        data = process_function(data)

                    data = function(data)

                    if 'output_function' in func_info:
                        output_function = func_info['output_function']

                        if 'output_dir' in func_info:
                            output_dir = func_info['output_dir']
                            output_dir = os.path.join(os.path.dirname(root_dir), os.path.basename(root_dir) + '_processed_by_client', output_dir)

                        output_function(data, output_dir, client)

        del subdirList[:]  # Prevent os.walk from descending into subdirectories


if __name__ == "__main__":
    # List of functions to apply
    functions = [
    {
        'function': results.plot_distribution,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.save_figs,
        'output_dir': 'histograms'
    }, 
    {
        'function': results.plot_boxplot,
        'output_function': results.save_fig,
        'output_dir': 'box_plots'
    },
    {
        'function': na_ratio,
        'output_function': results.save_csv,
        'output_dir': 'na_ratio',
        'dropna': False
    },
    {
        'function': results.find_outliers,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.save_csv,
        'output_dir': 'outliers'
    },
    {
        'function': results.find_measures,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.save_csv,
        'output_dir': 'measures'
    },
    {
        'function': results.remove_outliers_and_find_measures,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.save_csv,
        'output_dir': 'measures_clean'
    },
    {
        'function': results.remove_outliers,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.save_csv,
        'output_dir': 'data_clean'
    },
    {
        'function': results.remove_outliers_and_average,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.write_to_txt,
        'output_dir': 'averages'
    },
    {
        'function': results.group_results,
        'data_load_function': groupby,
        'args': [],
        'output_function': results.save_csv,
        'output_dir': 'data_grouped'
    }
    ]

    root_dirs = [
        './accumulated_csv_records/11-06 & 12-06 & 16-06 - 18-06/ipfs/retrieve',
        './accumulated_csv_records/miletus_degroot_nancy/19-06-2023/ipfs/retrieve',
        './accumulated_csv_records/13-06 & 18-06/swarm/retrieve'
    ]
    column = 'Retrieval Time (ms)'

    for root_dir in root_dirs:
        process_experiments(root_dir, column, functions)
        process_all(root_dir, column, functions)
        process_clients(root_dir, column, functions)
