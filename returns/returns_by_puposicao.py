#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  6 18:18:07 2025

@author: andrefelix
"""


import pandas as pd


def _find_duplicate_puposicao(data):
    """
    Identifies rows where the same (isin, dtposicao) has conflicting puposicao values.

    Returns:
        Index of duplicated rows based on isin and dtposicao.
    """
    group_cols = ['isin', 'dtposicao', 'puposicao']

    data_sorted = data[group_cols].sort_values(by=group_cols).drop_duplicates()

    dupl_mask = data_sorted.duplicated(subset=['isin', 'dtposicao'], keep='last')

    return data_sorted.index[dupl_mask]


def validate_unique_puposicao(data):
    """
    Returns indices of valid and duplicated rows based on unique (isin, dtposicao) for puposicao.

    Args:
        data: DataFrame with columns ['isin', 'dtposicao', 'puposicao'].

    Returns:
        valid_idx: Index of non-conflicting rows.
        dupl_idx: Index of duplicated/conflicting rows.
    """
    dupl_idx = _find_duplicate_puposicao(data)
    valid_idx = data.index.difference(dupl_idx)
    return valid_idx, dupl_idx


def generate_position_grid(base: pd.DataFrame, range_date) -> pd.DataFrame:
    """
    Creates a complete time grid (isin x dtposicao) and merges with base data.

    Args:
        base: DataFrame with at least 'isin', 'dtposicao', 'puposicao'
        range_date: list or pd.Series of dates

    Returns:
        Merged DataFrame with all combinations of isin and dtposicao.
    """
    unique_cnpjs = base['isin'].unique()
    grade = pd.DataFrame([(cnpj, date) for cnpj in unique_cnpjs for date in range_date],
                         columns=['isin', 'dtposicao'])
    merged = grade.merge(base, on=['isin', 'dtposicao'], how='left')
    return merged


def compute_returns_from_puposicao(range_date, new_data, persisted_returns):
    """
    Computes a time series of position returns ('rentab') from updated 'puposicao' values.

    This function merges newly received position data with previously stored data,
    fills in missing dates over a specified range, and calculates daily returns 
    using percentage change. It ensures consistency and handles duplicate checks 
    before performing the calculation.

    Parameters:
    ----------
    range_date : Sequence
        A sequence of datetime objects representing the target date range.

    new_data : pd.DataFrame
        DataFrame with new position data. Must contain columns
            ['isin', 'dtposicao', 'puposicao'].

    persisted_returns : pd.DataFrame
        Previously stored position data with the same column structure as `new_data`.

    Returns:
    -------
    pd.DataFrame
        DataFrame with columns ['isin', 'dtposicao', 'puposicao', 'rentab'],
            where 'rentab' represents the percentage change in 'puposicao'
            across dates per 'isin'.
    """
    dupl_idx = _find_duplicate_puposicao(new_data)
    if not dupl_idx.empty:
        duplicated_rows = new_data.loc[dupl_idx, ['isin', 'dtposicao', 'puposicao']]
        raise ValueError(f"puposicao diferente para mesmo isin e dtposicao.:\n{duplicated_rows}")

    new_data['dtposicao'] = pd.to_datetime(new_data['dtposicao'])
    persisted_returns['dtposicao'] = pd.to_datetime(persisted_returns['dtposicao'])

    mask_existing = ~persisted_returns.set_index(['isin', 'dtposicao']).index.isin(
        new_data.set_index(['isin', 'dtposicao']).index
    )
    base = pd.concat([persisted_returns[mask_existing], new_data], ignore_index=True)

    base = base[['isin', 'dtposicao', 'puposicao']].drop_duplicates()
    base = base[base['isin'].notnull()]

    full_data = generate_position_grid(base, range_date)
    full_data.sort_values(['isin', 'dtposicao'], inplace=True)

    pct = full_data.groupby('isin')['puposicao'].pct_change(fill_method=None)
    full_data['rentab'] = pct.round(8)

    return full_data[['isin', 'dtposicao', 'puposicao', 'rentab']]
