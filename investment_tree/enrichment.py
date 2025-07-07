#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 12:58:01 2025

@author: andrefelix
"""


def accumulate_columns_by_level(tree_hrzt, result_col, base_col, deep):
    """
    Accumulates numerical values from level-specific columns by multiplying them,
    producing a final consolidated value.

    Each level column is expected to follow the pattern '{base_col}_nivel_{i}' 
    for i in range(1, deep + 1). Missing values are treated as 1.0 (neutral element).

    Args:
        df (pd.DataFrame): DataFrame containing level-specific columns.
        base_col (str): Name of the base column (e.g., 'equity_stake').
        deep (int): Maximum depth to consider for level-specific columns.
        result_col (str): Name of the resulting column to store the accumulated value.

    Returns:
        None: Modifies the DataFrame in place by adding the result_col.
    """
    accumulate_cols = [f"{base_col}_nivel_{i}" for i in range(1, deep + 1)]
    accumulate_cols.append(base_col)
    tree_hrzt[result_col] = tree_hrzt[accumulate_cols].fillna(1).prod(axis=1)


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


def enrich_tree(tree_horzt):
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

    accumulate_columns_by_level(tree_horzt, 'equity_stake', 'equity_stake', max_deep)

    for i in range(1, max_deep + 1):
        mask_deep = tree_horzt['nivel'] == i
        col_name = f"valor_calc_nivel_{i}"
        tree_horzt.loc[mask_deep, 'valor_calc'] = (
            tree_horzt.loc[mask_deep, col_name]
            * tree_horzt.loc[mask_deep, 'equity_stake']
        )

    tree_horzt['total_invest'] = (
        tree_horzt.groupby(['cnpb', 'dtposicao'])['valor_calc']
        .transform('sum')
    )

    tree_horzt['composicao'] = (
        tree_horzt['valor_calc']
        / tree_horzt['total_invest']
    )

    for i in range(max_deep, -1, -1):
        mask_deep = tree_horzt['nivel'] == i
        suffix = '' if i == 0 else f"_nivel_{i}"
        col_returns = f"rentab{suffix}"
        tree_horzt.loc[mask_deep, 'rentab_ponderada'] = (
            tree_horzt.loc[mask_deep, 'composicao']
            * tree_horzt.loc[mask_deep, col_returns].fillna(0.0)
        )
        tree_horzt.loc[mask_deep, 'rentab_nominal'] = tree_horzt.loc[mask_deep, col_returns]

    base_final_cols = [
        'NEW_TIPO', 'NEW_NOME_ATIVO', 'NEW_GESTOR_WORD_CLOUD',
        'fEMISSOR.NOME_EMISSOR', 'fNUMERACA.TIPO_ATIVO', 'fNUMERACA.DESCRICAO'
    ]
    for base_col in base_final_cols:
        create_column_based_on_levels(tree_horzt, f"{base_col}_FINAL", base_col, max_deep)

    create_column_based_on_levels(tree_horzt, 'isin', 'isin', max_deep)

    fill_level_columns_forward(tree_horzt, 'NEW_NOME_ATIVO', max_deep)

    tree_horzt['SEARCH'] = (
        tree_horzt['NEW_NOME_ATIVO_FINAL'].fillna('')
        + ' ' + tree_horzt['NEW_GESTOR_WORD_CLOUD_FINAL'].fillna('')
        + ' ' + tree_horzt['fEMISSOR.NOME_EMISSOR_FINAL'].fillna('')
        + ' ' + tree_horzt['PARENT_FUNDO'].fillna('')
        + ' ' + tree_horzt['isin'].fillna('')
    )
