import pandas as pd
import yaml


def fusion_same_col_files(file_names, export_file_name=None, drop_duplicate=True,
                          file_format="csv",
                          sep=";", import_encoding="ISO-8859-1", export_encoding="utf-8"):
    """
       Fusion a set of files that contains the same columns but not the same lines

       :param file_names: (list of str) path and file_names fo the files
       :param export_file_name: (str or None) if None return a Pandas DataFrame instance otherwise save the file as
       export_file_name
       :param file_format: str, for now only csv is supported
       :param sep:
       :param drop_duplicate: if True and some data are dupplicated, then we delete it.
       :param import_encoding:
       :param export_encoding:
       :return:
    """
    assert file_format == "csv"

    dfs = []
    for file_name in file_names:
        dfs.append(pd.read_csv(file_name, encoding=import_encoding, sep=sep))

    df = pd.concat(dfs)
    if drop_duplicate:
        duplicate_rows_df = df[df.duplicated()]
        print(f"Number of duplicated rows: {duplicate_rows_df.shape[0]}")
        df = df.drop_duplicates()

    if export_file_name is not None:
        df.to_csv(export_file_name, encoding=export_encoding, index=False)
    else:
        return df


def rename_columns(columns_mapping, file_name=None, df_to_use=None, columns_to_drop=None,
                   export_file_name=None,
                   file_format="csv",
                   sep=";", import_encoding="ISO-8859-1", export_encoding="utf-8"):
    """

    :param file_name: a csv file
    :param export_file_name: None or a csv file
    :param file_format:
    :param sep:
    :param columns_mapping: a str representing a yaml file or a
    dict containing original column name as key and value being the new column name
    :param import_encoding:
    :param export_encoding:
    :return:
    """

    assert isinstance(columns_mapping, dict) or isinstance(columns_mapping, str)
    assert file_name is not None or df_to_use is not None

    if df_to_use is not None:
        df = df_to_use
    else:
        df = pd.read_csv(file_name, encoding=import_encoding, sep=';')

    columns_name = list(df)
    columns_name = [c.strip() for c in columns_name]
    # we change columns' name if the mapping is not None

    if isinstance(columns_mapping, str):
        mapping_yaml_file = columns_mapping
        with open(mapping_yaml_file, 'r') as stream:
            columns_mapping = yaml.load(stream, Loader=yaml.FullLoader)

    for i, c_name in enumerate(columns_name):
        if c_name in columns_mapping:
            columns_name[i] = columns_mapping[c_name]
            print(f"Col '{c_name}' changed to '{columns_mapping[c_name]}'")
        else:
            print(f"- Col '{c_name}' not changed")

    df.columns = columns_name

    if columns_to_drop is not None:
        # a yaml file or a list
        if isinstance(columns_to_drop, str):
            columns_to_drop_file = columns_to_drop
            with open(columns_to_drop_file, 'r') as stream:
                columns_to_drop = yaml.load(stream, Loader=yaml.FullLoader)
            if isinstance(columns_to_drop, dict):
                columns_to_drop = list(columns_to_drop.keys())

        columns_to_drop_filtered = []
        for c in columns_to_drop:
            if c in columns_name:
                columns_to_drop_filtered.append(c)

        if columns_to_drop_filtered:
            df.drop(columns=columns_to_drop_filtered, inplace=True)

    if export_file_name is not None:
        df.to_csv(export_file_name, encoding=export_encoding, index=False)
    else:
        return df


def keep_not_na_values(df, columns_to_check):
    """
    Drop the lines in which one of the column has a NA value
    :param df:
    :param columns_to_check: str or list of str
    :return:
    """
    if isinstance(columns_to_check, str):
        columns_to_check = [columns_to_check]

    for column_name in columns_to_check:
        df = df[df[column_name].notna()]

    return df

def merge_files_by_key(file_1, file_2, on, export_file_name=None,
                       file_format="csv",
                       sep=";", import_encoding="ISO-8859-1", export_encoding="utf-8"):
    """

    :param file_1: either str (file_name) or Pandas dataframe
    :param file_2: either str (file_name) or Pandas dataframe
    :param on:
    :param export_file_name:
    :param file_format:
    :param sep:
    :param import_encoding:
    :param export_encoding:
    :return:
    """
    assert file_format == "csv"

    if isinstance(file_1, str):
        df_1 = pd.read_csv(file_1, encoding=import_encoding, sep=sep)
    else:
        df_1 = file_1

    if isinstance(file_2, str):
        df_2 = pd.read_csv(file_2, encoding=import_encoding, sep=sep)
    else:
        df_2 = file_2

    df = pd.merge(df_1, df_2, how='inner', on=on, suffixes=(None, "_merged"))

    # now deleting merged columns present in double
    columns_name = list(df)
    columns_to_drop = []
    for column_name in columns_name:
        if "_merged" in column_name:
            columns_to_drop.append(column_name)

    if columns_to_drop:
        df.drop(columns=columns_to_drop, inplace=True)

    if export_file_name is not None:
        df.to_csv(export_file_name, encoding=export_encoding, index=False)
    else:
        return df


# TODO: Add function to drop line if a certain field is Nan