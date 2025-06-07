#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  7 12:58:01 2025

@author: andrefelix
"""


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
    Preenche uma nova coluna com valores prioritários entre colunas base e níveis sucessivos.

    Args:
        tree_hrzt (pd.DataFrame): DataFrame de entrada.
        new_col (str): Nome da nova coluna a ser criada.
        base_col (str): Nome da coluna base.
        deep (int): Número de níveis (sufixos _nivel_1 a _nivel_{deep}).

    Returns:
        pd.DataFrame: O DataFrame com a nova coluna preenchida.
    """
    priority_cols = [f"{base_col}_nivel_{i}" for i in range(deep, 0, -1)]
    priority_cols.append(base_col)

    tree_hrzt[new_col] = tree_hrzt[priority_cols].bfill(axis=1).iloc[:, 0]


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
    generate_final_columns(tree_horzt)
    max_deep = tree_horzt['nivel'].max()
    fill_level_columns_forward(tree_horzt, 'NEW_NOME_ATIVO', max_deep)

    tree_horzt['SEARCH'] = (
        tree_horzt['NEW_NOME_ATIVO_FINAL'].fillna('')
        + ' ' + tree_horzt['NEW_GESTOR_WORD_CLOUD_FINAL'].fillna('')
        + ' ' + tree_horzt['fEMISSOR.NOME_EMISSOR_FINAL'].fillna('')
        + ' ' + tree_horzt['PARENT_FUNDO'].fillna('')
    )
