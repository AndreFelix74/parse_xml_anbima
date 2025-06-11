#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 12:58:01 2025

@author: andrefelix
"""


import pandas as pd


def generate_final_columns(tree_horzt):
    """
    Generate final columns based on hierarchical levels in the 'tree_horzt' DataFrame.

    This function calculates the maximum depth of the hierarchy using the 'nivel' column
    and uses it to generate final versions of multiple columns by aggregating or selecting
    values based on that depth.

    Parameters:
    ----------
    tree_horzt : pandas.DataFrame
        The hierarchical tree DataFrame containing the 'nivel' column and intermediate
        columns used to compute final columns.

    Returns:
    -------
    None
        The function modifies the input DataFrame in place, adding new final columns.
    """
    max_deep = tree_horzt['nivel'].max()

    columns_to_generate = [
        'NEW_TIPO', 'NEW_NOME_ATIVO', 'NEW_GESTOR_WORD_CLOUD', 'fEMISSOR.NOME_EMISSOR'
    ]

    for base_col in columns_to_generate:
        create_column_based_on_levels(tree_horzt, f"{base_col}_FINAL", base_col, max_deep)


def create_column_based_on_levels(tree_hrzt, new_col, base_col, deep):
    """
    Creates a new column by filling values from a sequence of level-based columns in cascade order.

    The function searches for the first non-null value among columns named 
    `{base_col}_nivel_{deep}` down to `{base_col}_nivel_1`, and finally `{base_col}`, 
    applying a left-to-right backfill strategy.

    Args:
        tree_hrzt (pd.DataFrame): Input DataFrame.
        new_col (str): Name of the new column to be created.
        base_col (str): Base column name used to generate level columns.
        deep (int): Depth of levels to search, starting from `nivel_{deep}`.

    Returns:
        pd.DataFrame: The original DataFrame with the new column added.
    """
    cascading_cols = [f"{base_col}_nivel_{i}" for i in range(deep, 0, -1)]
    cascading_cols.append(base_col)

    tree_hrzt[new_col] = tree_hrzt[cascading_cols].bfill(axis=1).iloc[:, 0]


def fill_level_columns_forward(tree_hrzt, base_col, deep):
    """
    Forward-fills missing values in level columns by cascading values from higher to lower levels.

    Args:
        tree_hrzt (pd.DataFrame): The DataFrame with hierarchical level columns.
        base_col (str): Base name of the columns (e.g., 'estrutura').
        deep (int): Number of levels (e.g., 4 means columns from _nivel_1 to _nivel_4).

    Returns:
        pd.DataFrame: The modified DataFrame with levels filled hierarchically.
    """
    for i in range(0, deep):
        curr_level_suffix = f"_nivel_{i}" if i > 0 else ''
        current_col = f"{base_col}{curr_level_suffix}"
        next_col = f"{base_col}_nivel_{i+1}"

        mask = tree_hrzt[next_col].isna() | (tree_hrzt[next_col] == '')
        tree_hrzt.loc[mask, next_col] = tree_hrzt.loc[mask, current_col]


def find_matched_returns_from_tree(tree_horzt, returns_by_fund, deep):
    """
    Finds matched return records from a horizontal investment tree across all levels.

    This function iterates through each level of the tree structure and searches for
    return data (`returns_by_fund`) that matches each fund's CNPJ and reporting date (`dtposicao`).
    It returns only the successful matches, annotated with the corresponding tree level and 
    metadata indicating the source.

    Args:
        tree_horzt (pd.DataFrame): Horizontal investment tree with columns such as 
            'cnpjfundo', 'cnpjfundo_nivel_1', ..., containing the nested fund structure.
        returns_by_fund (pd.DataFrame): DataFrame with columns ['cnpjfundo', 'dtposicao', 'rentab']
            that holds return information by fund and date.
        deep (int): Maximum recursion depth (i.e., the number of tree levels).

    Returns:
        pd.DataFrame: A DataFrame of matched return entries, with columns:
            ['cnpjfundo_alvo', 'dtposicao', 'rentab', 'nivel', 'origem'].
    """
    returns_by_fund.rename(columns={'cnpjfundo': 'ret_cnpjfundo'}, inplace=True)
    returns_by_fund['dtposicao'] = pd.to_datetime(returns_by_fund['dtposicao'])

    group_keys_tree = ['cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb', 'nivel',
                       'NEW_TIPO', 'NEW_NOME_ATIVO', 'NEW_GESTOR_WORD_CLOUD',
                       'fEMISSOR.NOME_EMISSOR', 'PARENT_FUNDO']

    cnpj_cols = [f'cnpjfundo{"_nivel_" + str(i) if i > 0 else ""}' for i in range(deep)]

    tree_aux = tree_horzt[group_keys_tree + cnpj_cols].drop_duplicates().copy()
    tree_aux.to_csv('tree_aux.csv')
    tree_aux['original_index'] = tree_aux.index
    tree_aux['dtposicao'] = pd.to_datetime(tree_aux['dtposicao'])

    returns_by_level = []

    for i in range(deep - 1, -1, -1):
        curr_level_suffix = f"_nivel_{i}" if i > 0 else ''
        cnpjfundo_col = f"cnpjfundo{curr_level_suffix}"
        returns = tree_aux.merge(
            returns_by_fund[['ret_cnpjfundo', 'dtposicao', 'rentab']],
            left_on=[cnpjfundo_col, 'dtposicao'],
            right_on=['ret_cnpjfundo', 'dtposicao'],
            how='inner'
        )

        tree_aux = tree_aux[~tree_aux['original_index'].isin(returns['original_index'])]
        returns.drop(columns=['ret_cnpjfundo', 'original_index'], inplace=True)
        returns['NEW_TIPO'] = 'rentab'

        returns_by_level.append(returns.drop_duplicates())

    return pd.concat(returns_by_level)


def enrich_tree(tree_horzt, returns_by_puposicao):
    """
    Enriches a tree structure with derived textual fields and governance structure mappings.

    This function:
        - Generates final label columns.
        - Propagates level-based data forward.
        - Creates a combined search string from relevant textual fields.
        - Attempts to fill in missing governance structure information.

    Args:
        tree_horzt (pd.DataFrame): The horizontal tree structure containing fund
        relationships.

    Returns:
        None: The input DataFrame is modified in-place.
    """
    max_deep = tree_horzt['nivel'].max()
    returns = find_matched_returns_from_tree(tree_horzt, returns_by_puposicao, max_deep)
    returns.to_csv('rentab.csv')
    tree_horzt = pd.concat([tree_horzt, returns])

    generate_final_columns(tree_horzt)

    fill_level_columns_forward(tree_horzt, 'NEW_NOME_ATIVO', max_deep)

    tree_horzt['SEARCH'] = (
        tree_horzt['NEW_NOME_ATIVO_FINAL'].fillna('')
        + ' ' + tree_horzt['NEW_GESTOR_WORD_CLOUD_FINAL'].fillna('')
        + ' ' + tree_horzt['fEMISSOR.NOME_EMISSOR_FINAL'].fillna('')
        + ' ' + tree_horzt['PARENT_FUNDO'].fillna('')
    )

    return tree_horzt
