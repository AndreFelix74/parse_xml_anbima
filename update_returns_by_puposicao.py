#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  6 18:18:07 2025

@author: andrefelix
"""


import numpy as np
import pandas as pd


def _find_duplicate_puposicao(data):
    """
    Identifies rows where the same (cnpjfundo, dtposicao) has conflicting puposicao values.

    Returns:
        Index of duplicated rows based on cnpjfundo and dtposicao.
    """
    group_cols = ['cnpjfundo', 'dtposicao', 'puposicao']

    data_sorted = data[group_cols].sort_values(by=group_cols).drop_duplicates()

    dupl_mask = data_sorted.duplicated(subset=['cnpjfundo', 'dtposicao'], keep='last')

    return data_sorted.index[dupl_mask]


def validate_unique_puposicao(data):
    """
    Returns indices of valid and duplicated rows based on unique (cnpjfundo, dtposicao) for puposicao.

    Args:
        data: DataFrame with columns ['cnpjfundo', 'dtposicao', 'puposicao'].

    Returns:
        valid_idx: Index of non-conflicting rows.
        dupl_idx: Index of duplicated/conflicting rows.
    """
    dupl_idx = _find_duplicate_puposicao(data)
    valid_idx = data.index.difference(dupl_idx)
    return valid_idx, dupl_idx


def generate_position_grid(base: pd.DataFrame, range_date) -> pd.DataFrame:
    """
    Creates a complete time grid (cnpjfundo x dtposicao) and merges with base data.

    Args:
        base: DataFrame with at least 'cnpjfundo', 'dtposicao', 'puposicao'
        range_date: list or pd.Series of dates

    Returns:
        Merged DataFrame with all combinations of cnpjfundo and dtposicao.
    """
    unique_cnpjs = base['cnpjfundo'].unique()
    grade = pd.DataFrame([(cnpj, date) for cnpj in unique_cnpjs for date in range_date],
                         columns=['cnpjfundo', 'dtposicao'])
    merged = grade.merge(base, on=['cnpjfundo', 'dtposicao'], how='left')
    return merged


def calculate_return_15_digits(df: pd.DataFrame, group_col: str, value_col: str) -> pd.Series:
    """
    Computes percentage change within groups and formats the result
    to 15 significant digits (Excel-compatible).

    Args:
        df: DataFrame containing the data.
        group_col: Column name to group by (e.g., 'cnpjfundo').
        value_col: Column name on which to compute pct_change (e.g., 'puposicao').

    Returns:
        Series of formatted percentage changes.
    """
    pct = df.groupby(group_col)[value_col].pct_change(fill_method=None)
    formatted = np.char.mod('%.15g', pct.values.astype(np.float64))
    return pd.to_numeric(formatted, errors='coerce')


def update_returns_from_puposicao(range_date, new_data, persisted_returns):
    """
    Validates new position data, merges it with persisted returns,
    fills missing dates, and calculates return (rentab) with Excel-safe precision.

    Args:
        range_date: Sequence of dates to generate the time grid.
        new_data: DataFrame with new position data.
        persisted_returns: DataFrame with previously stored position data.

    Returns:
        DataFrame with columns: cnpjfundo, dtposicao, puposicao, rentab
    """
    dupl_idx = _find_duplicate_puposicao(new_data)
    if not dupl_idx.empty:
        duplicated_rows = new_data.loc[dupl_idx, ['cnpjfundo', 'dtposicao', 'puposicao']]
        raise ValueError(f"puposicao diferente para mesmo cnpjfundo e dtposicao.:\n{duplicated_rows}")

    new_data['dtposicao'] = pd.to_datetime(new_data['dtposicao'])
    persisted_returns['dtposicao'] = pd.to_datetime(persisted_returns['dtposicao'])

    mask_existing = ~persisted_returns.set_index(['cnpjfundo', 'dtposicao']).index.isin(
        new_data.set_index(['cnpjfundo', 'dtposicao']).index
    )
    base = pd.concat([persisted_returns[mask_existing], new_data], ignore_index=True)

    base = base[['cnpjfundo', 'dtposicao', 'puposicao']].drop_duplicates()
    base = base[base['cnpjfundo'].notnull()]

    full_data = generate_position_grid(base, range_date)
    full_data.sort_values(['cnpjfundo', 'dtposicao'], inplace=True)

    full_data['rentab'] = calculate_return_15_digits(full_data, 'cnpjfundo', 'puposicao')

    return full_data[['cnpjfundo', 'dtposicao', 'puposicao', 'rentab']]
