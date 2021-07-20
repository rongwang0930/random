import pandas as pd
import os
from pathlib import Path


def filter_sort_csv_data(input_path, expr):
    # making data frame from csv file
    data = pd.read_csv(input_path)

    # replacing blank spaces with '_'
    data.columns = [column.replace(" ", "_") for column in data.columns]

    # filtering with query method
    data.query(expr, inplace=True)
    return data


def filter_sort_csv_data_dir(input_dir, output_path, expr, sort_col, sort_col_ascd):
    list_of_filtered_data = []

    # traverse the whole input dir and filter all files
    for subdir, dirs, files in os.walk(input_dir):
        for filename in files:
            file_path = subdir + os.sep + filename
            filtered_data = filter_sort_csv_data(file_path, expr)
            list_of_filtered_data.append(filtered_data)

    # merge all filtered dataframe and save to a single file
    merged_filtered_data = pd.concat(list_of_filtered_data)
    # sort data before combining
    merged_filtered_data[sort_col] = pd.to_datetime(merged_filtered_data[sort_col])
    merged_filtered_data.sort_values(sort_col, ascending=sort_col_ascd, inplace=True)
    # save to a single file
    merged_filtered_data.to_csv(output_path, sep=',', index=False, header=True)


def combine_multi_csv_files_and_sort(file_path_list, output_path, sort_col, sort_col_ascd):
    list_of_df = []
    for file_path in file_path_list:
        df = pd.read_csv(file_path)
        list_of_df.append(df)

    merged_df = pd.concat(list_of_df)

    # sort and save to a single file
    merged_df[sort_col] = pd.to_datetime(merged_df[sort_col])
    merged_df.sort_values(sort_col, ascending=sort_col_ascd, inplace=True)

    create_dir_if_not_exist(os.path.dirname(output_path))
    merged_df.to_csv(output_path, sep=',', index=False, header=True)


def create_dir_if_not_exist(dir):
    Path(dir).mkdir(parents=True, exist_ok=True)


if __name__ == '__main__':
    filter_expr = 'pnode_id == 49604 and row_is_current == True'
    sort_col = 'datetime_beginning_ept'
    sort_col_ascd = True

    filter_sort_csv_data_dir('input', 'output/result.csv', filter_expr, sort_col, sort_col_ascd)

    combine_multi_csv_files_and_sort(['output/result.csv', 'output/result2.csv'], 'output/combined_result.csv', sort_col, sort_col_ascd)
