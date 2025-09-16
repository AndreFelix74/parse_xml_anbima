#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 16 09:19:03 2025

@author: andrefelix
"""


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
    tree_hrzt[new_col] = None
    unresolved = tree_hrzt[new_col].isna()

    for i in range(deep, -1, -1):
        suffix = '' if i == 0 else f"_nivel_{i}"
        col = f"{base_col}{suffix}"

        mask = unresolved & tree_hrzt[col].notna()
        tree_hrzt.loc[mask, new_col] = tree_hrzt.loc[mask, col]
        unresolved &= ~mask


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


def enrich_text(tree_horzt):
    """
    Enriches the tree DataFrame with derived textual fields.

    This function:
        - Resolves final values for descriptive and categorical fields
          (e.g., type, asset name, issuer, manager, ISIN) by cascading across
          hierarchical level columns.
        - Forward-fills missing values in name-related level columns to ensure
          consistent labeling across levels.
        - Builds a unified free-text search field (`SEARCH`) by concatenating
          selected descriptive attributes.

    Args:
        tree_horzt (pd.DataFrame): Input DataFrame containing hierarchical
            fund/asset relationships and level-based columns.

    Returns:
        None: Modifies the input DataFrame in place by adding derived textual
        columns (`*_FINAL`, `isin_FINAL`, `SEARCH`).
    """
    max_deep = tree_horzt['nivel'].max()

    final_cols_base = [
        'NEW_TIPO', 'NEW_NOME_ATIVO', 'NEW_GESTOR_WORD_CLOUD',
        'fEMISSOR.NOME_EMISSOR', 'fNUMERACA.TIPO_ATIVO', 'fNUMERACA.DESCRICAO'
    ]
    for col_base in final_cols_base:
        create_column_based_on_levels(tree_horzt, f"{col_base}_FINAL", col_base, max_deep)

    create_column_based_on_levels(tree_horzt, 'isin_FINAL', 'isin', max_deep)

    fill_level_columns_forward(tree_horzt, 'NEW_NOME_ATIVO', max_deep)

    tree_horzt['SEARCH'] = (
        tree_horzt['NEW_NOME_ATIVO_FINAL'].fillna('')
        + ' ' + tree_horzt['NEW_GESTOR_WORD_CLOUD_FINAL'].fillna('')
        + ' ' + tree_horzt['fEMISSOR.NOME_EMISSOR_FINAL'].fillna('')
        + ' ' + tree_horzt['PARENT_FUNDO'].fillna('')
        + ' ' + tree_horzt['isin'].fillna('')
    )
