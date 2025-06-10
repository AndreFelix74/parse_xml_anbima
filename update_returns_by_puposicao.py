#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  6 18:18:07 2025

@author: andrefelix
"""


import numpy as np
import pandas as pd


def _find_duplicate_puposicao(data):
    group_cols = ['cnpjfundo', 'dtposicao', 'puposicao']

    data_sorted = data[group_cols].sort_values(by=group_cols).drop_duplicates()

    dupl_mask = data_sorted.duplicated(subset=['cnpjfundo', 'dtposicao'], keep='last')

    return data_sorted.index[dupl_mask]


def validate_unique_puposicao(data):
    dupl_idx = _find_duplicate_puposicao(data)
    valid_idx = data.index.difference(dupl_idx)
    return valid_idx, dupl_idx


def update_returns_from_puposicao(range_date, new_data, persisted_returns):
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

    unique_cnpjs = base['cnpjfundo'].unique()
    grade = pd.DataFrame([(cnpj, date) for cnpj in unique_cnpjs for date in range_date],
                         columns=['cnpjfundo', 'dtposicao'])

    full_data = grade.merge(base, on=['cnpjfundo', 'dtposicao'], how='left')

    full_data.sort_values(['cnpjfundo', 'dtposicao'], inplace=True)

    rentab_series = full_data.groupby('cnpjfundo')['puposicao'].pct_change(fill_method=None)
    formatted = np.char.mod('%.15g', rentab_series.values.astype(np.float64))

    full_data['rentab'] = pd.to_numeric(formatted, errors='coerce')

    return full_data[['cnpjfundo', 'dtposicao', 'puposicao', 'rentab']]
