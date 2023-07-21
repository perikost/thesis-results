import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def bytes_to_size(size, decimal_places=0):
    units = ['B', 'KB', 'MB', 'GB']
    
    for unit in units:
        # If size < 1024 or we're at the last unit, we're done
        # Else go to the next size
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.{decimal_places}f}{unit}"
        else:
            size /= 1024.0

def plot_distribution(data, column='Retrieval Time (ms)'):
    figs = []
    
    for name, group in data:
        name = bytes_to_size(name)

        print(f"Plotting distribution for group: {name}")

        group_data = group[column]
        mean = group_data.mean()
        std = group_data.std()
        median = group_data.median()
        variance = group_data.var()
        skewness = group_data.skew()

        # Create figure and axes
        fig, ax = plt.subplots()
        
        sns.histplot(group[column], kde=True, label=name)

        # Add info about data
        fig.text(0.75, 0.85, f'Mean: {mean:.2f}', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12)
        fig.text(0.75, 0.80, f'Std: {std:.2f}', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12)
        fig.text(0.75, 0.75, f'Median: {median:.2f}', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12)
        fig.text(0.75, 0.70, f'Variance: {variance:.2f}', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12)
        fig.text(0.75, 0.65, f'Skewness: {skewness:.2f}', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, fontsize=12)
        fig.name = name

        figs.append(fig)

        plt.legend()
        # plt.show()
    return figs

def plot_boxplot(data, x='Size (Bytes)', y='Retrieval Time (ms)'):
    fig, _ = plt.subplots()

    data = data.sort_values(by=x)
    data[x] = data[x].apply(bytes_to_size)
    sns.boxplot(x=x, y=y, data=data)

    return fig

def group_results(data):
    # DataFrame for concatenating all groups
    data_grouped = pd.DataFrame()

    for name, group in data:
        data_grouped = pd.concat([data_grouped, group])
    
    # when resetting the index, the old index is added as a column
    data_grouped = data_grouped.reset_index()
    return data_grouped

def average(data, column='Retrieval Time (ms)'):
    rows = []
    for name, group in data:
        name = bytes_to_size(name)
        cols = []

        average = group[column].mean()
        message = f"Size: {name} Average value is: {average}"
        cols.append(message)
        print(message)

        rows.append(cols)
    
    return rows

def find_measures(data, column='Retrieval Time (ms)'):  
    measures = []
    for name, group in data:
        name = bytes_to_size(name)

        print(f"Calculating measures for group: {name}")
        
        group_data = group[column]

        mean = group_data.mean()
        std = group_data.std()
        median = group_data.median()
        variance = group_data.var()
        skewness = group_data.skew()

        measures.append([name, mean, std, median, variance, skewness, group_data.min(), group_data.max(), len(group_data)])

    df = pd.DataFrame(measures, columns=['Group', 'Mean', 'Std', 'Median', 'Variance', 'Skewness', 'Min', 'Max', 'Group size'])
    
    return df

def find_outliers(data, column='Retrieval Time (ms)'):
    # DataFrame for concatenating all groups
    df = pd.DataFrame()

    for name, group in data:
        name = bytes_to_size(name)

        print(f"Finding outliers for group: {name}")
        Q1 = group[column].quantile(0.25)
        Q3 = group[column].quantile(0.75)
        IQR = Q3 - Q1
        outlier_step = 1.5 * IQR
        print('Lower outlier gate:', Q1 - outlier_step)
        print('Upper outlier gate:', Q3 + outlier_step)
        outliers = group[(group[column] < Q1 - outlier_step) | (group[column] > Q3 + outlier_step)]
        df = pd.concat([df, outliers])
        print('Outliers for the dataset are:', outliers)

    df = df.reset_index()
    return df
    

def remove_outliers_from_group(data, column='Retrieval Time (ms)'):
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    outlier_step = 1.5 * IQR
    data_clean = data[(data[column] >= Q1 - outlier_step) & (data[column] <= Q3 + outlier_step)]

    data_clean = data_clean.reset_index()
    return data_clean

def remove_outliers(data, column='Retrieval Time (ms)'):
    # DataFrame for concatenating all groups
    data_clean = pd.DataFrame()

    for name, group in data:
        name = bytes_to_size(name)

        print(f"Removing outliers for group: {name}")
        group_clean = remove_outliers_from_group(group, column)
        data_clean = pd.concat([data_clean, group_clean])

    return data_clean

def remove_outliers_and_average(data, column='Retrieval Time (ms)'):
    rows = []
    for name, group in data:
        name = bytes_to_size(name)

        print(f"Removing outliers and finding average for group: {name}")

        cols = []

        average = group[column].mean()
        message = f"Size: {name} Average value (with outliers) is: {average}"
        cols.append(message)
        print(message)

        data_clean = remove_outliers_from_group(group, column)

        average = data_clean[column].mean()
        message = f"Average value (excluding outliers) is: {average}"
        cols.append(message)
        print(message)

        rows.append(cols)
    
    return rows

def remove_outliers_and_boxplot(data, column='Retrieval Time (ms)'):
    # DataFrame for concatenating all groups
    data_clean = pd.DataFrame()

    for name, group in data:
        name = bytes_to_size(name)

        print(f"Removing outliers and plotting boxplot: {name}")
        group_clean = remove_outliers_from_group(group, column)
        data_clean = pd.concat([data_clean, group_clean])

    return plot_boxplot(data_clean)

def remove_outliers_and_find_measures(data, column='Retrieval Time (ms)'):
    clean_data = pd.DataFrame()
    size_diff = {}
    
    for name, group in data:
        name = bytes_to_size(name)

        print(f"Removing outliers and finding measures: {name}")
        group_clean = remove_outliers_from_group(group, column)
        
        size_diff[name] = len(group) - len(group_clean)
        clean_data = pd.concat([clean_data, group_clean])
    
    measures_df = find_measures(clean_data.groupby('Size (Bytes)'))
    measures_df['Outliers'] = measures_df['Group'].map(size_diff)

    return measures_df

def read_all(file_path, dropna=True, na_values='-'):
    data = pd.read_csv(file_path, na_values=na_values)
    if dropna:
        data.dropna(inplace=True) # drop na values
    
    return data

def read_columns(file_path, columns=['Retrieval Time (ms)', 'Size (Bytes)'], dropna=True, na_values='-'):
    data = pd.read_csv(file_path, usecols=columns, na_values=na_values)
    if dropna:
        data.dropna(inplace=True) # drop na values
    
    return data

def read_columns_and_groupby(file_path, groupby='Size (Bytes)', groupby_type=int, columns=['Retrieval Time (ms)', 'Size (Bytes)'], dropna=True, na_values='-'):
    data = read_columns(file_path, columns=columns, dropna=dropna, na_values=na_values)
    
    # Convert column to desired type
    data[groupby] = data[groupby].astype(groupby_type)
    
    # Group the data by specified column
    grouped = data.groupby(groupby)

    return grouped

def read_columns_groupby_and_filter(file_path, groupby='Size (Bytes)', groupby_type=int, columns=['Retrieval Time (ms)', 'Size (Bytes)'], dropna=True, na_values='-'):
    data = read_columns(file_path, columns=columns, dropna=dropna, na_values=na_values)
    
    print('Will remove', data[data['Retrieval Time (ms)'] > 15000])

    # Keep only values smaller than 15 seconds
    data = data[data['Retrieval Time (ms)'] <= 15000]

    # Convert column to desired type
    data[groupby] = data[groupby].astype(groupby_type)
    
    # Group the data by specified column
    grouped = data.groupby(groupby)

    return grouped

def save_fig(fig, directory, file_name):
    os.makedirs(directory, exist_ok=True)
    file_name = os.path.join(directory, file_name)

    fig.savefig(f'{file_name}.png')

def save_figs(figs, directory, file_name):
    os.makedirs(directory, exist_ok=True)
    file_name = os.path.join(directory, file_name)

    for fig in figs:
        fig.savefig(f'{file_name}_{fig.name}.png')

def save_csv(df, directory, file_name):
    os.makedirs(directory, exist_ok=True)
    file_name = os.path.join(directory, file_name)
    df.to_csv(f'{file_name}_.csv', index=False)

def write_to_txt(data, directory, file_name):
    os.makedirs(directory, exist_ok=True)
    file_name = os.path.join(directory, file_name)

    # Open file in append mode. If file does not exist, it will be created.
    with open(file_name, 'w') as f:
        for row in data:
            line = ', '.join(map(str, row))
            f.write(line + '\n')

def process_csv_files(root_dir, column, functions):
    for dirName, subdirList, fileList in os.walk(root_dir):
        print('Found directory:', dirName)
        found_csv_foler = False

        for fname in fileList:
            if fname.endswith('.csv'):
                print('\tProcessing file:', fname)
                file_path = os.path.join(dirName, fname)
                found_csv_foler = True
                
                #for each function load data and execute function
                for func_info in functions:
                    function = func_info['function']
                    data_load_function = func_info['data_load_function']
                    args = func_info['args']

                    data = data_load_function(file_path, *args)
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
                            output_dir = os.path.join(os.path.dirname(grand_parent_dir), os.path.basename(parent_dir) + '_processed_by_experiment_and_client', os.path.basename(dirName), output_dir)
                        name, ext = os.path.splitext(fname)
                        output_function(data, output_dir, name)
        
        #  We need to process only the initial csv files. So we should prevent os.walk
        #  from descending deeper into the directory's structure after the first csv is found.
        if found_csv_foler:
            del subdirList[:]


if __name__ == "__main__":
    # List of functions to apply
    functions = [
    {
        'function': plot_distribution,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': save_figs,
        'output_dir': 'histograms'
    }, 
    {
        'function': plot_boxplot,
        'data_load_function': read_columns,
        'args': [],
        'output_function': save_fig,
        'output_dir': 'box_plots'
    }, 
    {
        'function': find_outliers,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': save_csv,
        'output_dir': 'outliers'
    },
    {
        'function': find_measures,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': save_csv,
        'output_dir': 'measures'
    },
    {
        'function': remove_outliers_and_find_measures,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': save_csv,
        'output_dir': 'measures_clean'
    },
    {
        'function': remove_outliers,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': save_csv,
        'output_dir': 'data_clean'
    },
    {
        'function': remove_outliers_and_average,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': write_to_txt,
        'output_dir': 'averages'
    },
    {
        'function': group_results,
        'data_load_function': read_columns_and_groupby,
        'args': [],
        'output_function': save_csv,
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
        process_csv_files(root_dir, column, functions)

