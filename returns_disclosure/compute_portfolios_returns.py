#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 28 18:30:39 2025

@author: andrefelix
"""


import pandas as pd


def ensure_list(group_column):
    """
    Ensure the parameter `group_column` is returned as a list.

    If `group_column` is None, return [].
    If it is a string, wrap it as [group_column].
    If it is already a list, return it unchanged.

    Args:
        group_column (str | list | None): Column name, a list of column names, or None.

    Returns:
        list: A list of column names ready for use.
    """
    if isinstance(group_column, str):
        return [group_column]

    if group_column is None:
        return []

    return group_column


def calculate_ytd_returns(monthly_returns, key_column=None):
    """
    Compute Year-To-Date (YTD) return by compounding monthly returns within each year.

    The function compounds `RENTAB_MES_PONDERADA_MEC_SAC` per group and year,
    then subtracts 1. The result aligns to the original row index.

    Args:
        monthly_returns (pd.DataFrame): Must include ['DT', 'RENTAB_MES_PONDERADA_MEC_SAC']
            and any grouping keys referenced by `key_column`.
        key_column (str | list | None): Optional grouping key(s). Use None for no grouping.

    Returns:
        pd.Series: YTD returns aligned to the original DataFrame index.
    """
    key_column = ensure_list(key_column)

    returns_sorted = monthly_returns.copy()
    returns_sorted['original_index'] = returns_sorted.index
    returns_sorted['ANO'] = pd.to_datetime(returns_sorted['DT']).dt.year

    returns_sorted.sort_values(key_column + ['DT'], inplace=True)

    returns_sorted['1_plus_r'] = returns_sorted['RENTAB_MES_PONDERADA_MEC_SAC'] + 1

    returns_sorted['cumprod'] = (
        returns_sorted
        .groupby(key_column + ['ANO'])['1_plus_r']
        .transform('cumprod')
    )

    returns_sorted['cum_rentab'] = returns_sorted['cumprod'] - 1

    result = returns_sorted.set_index('original_index').sort_index()['cum_rentab']

    return result


def calculate_t12m_returns(mec_sac):
    """
    Compute trailing 12-month (T12M) return by compounding monthly returns per CODCLI.

    Uses a rolling product window of size 12 over (1 + RENTAB_MES), then subtracts 1.
    The result aligns to the original row index.

    Args:
        mec_sac (pd.DataFrame): Must include ['CLCLI_CD', 'DT', 'RENTAB_MES'].

    Returns:
        pd.Series: T12M returns aligned to the original DataFrame index.
    """
    mec_sac_sorted = mec_sac.copy()
    mec_sac_sorted['original_index'] = mec_sac_sorted.index

    mec_sac_sorted.sort_values(['CLCLI_CD', 'DT'], inplace=True)

    mec_sac_sorted['1_plus_r'] = 1 + mec_sac_sorted['RENTAB_MES']

    rolling_prod = (
        mec_sac_sorted.groupby('CLCLI_CD')['1_plus_r']
        .rolling(window=12, min_periods=12)
        .agg('prod')
        .reset_index(level=0, drop=True)
    )

    mec_sac_sorted['t12m_return'] = rolling_prod - 1

    result = mec_sac_sorted.set_index('original_index').sort_index()['t12m_return']

    return result


def calculate_weighted_returns_by_group(mec_sac_dcadplanosac, group_column=None):
    """
    Compute monthly weighted returns per group by aggregating daily weighted returns.

    Daily returns are weighted by `VL_PATRLIQTOT1` within the group for each day,
    converted to factors, compounded by month, and then converted back to returns.

    Args:
        mec_sac_dcadplanosac (pd.DataFrame): Joined mec_sac with dCadPlanoSSC.
            Must include ['DT', 'RENTAB_DIA', 'VL_PATRLIQTOT1'] and the grouping column(s).
        group_column (str | list | None): Grouping column(s), e.g., 'TIPO_PLANO', 'GRUPO',
            'INDEXADOR'. Use None for the consolidated case.

    Returns:
        pd.DataFrame: Columns [group_column(s), 'DT', 'RENTAB_MES_PONDERADA_MEC_SAC'].
    """
    group_column = ensure_list(group_column)

    group_columns = group_column + ['DT', 'RENTAB_DIA', 'VL_PATRLIQTOT1']
    df_aux = mec_sac_dcadplanosac[group_columns].copy().dropna()

    df_aux['TOTAL_PL_MEC_SAC'] = (
        df_aux.groupby(group_columns)['VL_PATRLIQTOT1']
        .transform('sum')
    )

    df_aux['RENTAB_DIA_PONDERADA_MEC_SAC'] = (
        df_aux['VL_PATRLIQTOT1']
        / df_aux['TOTAL_PL_MEC_SAC']
        * df_aux['RENTAB_DIA']
        )

    df_aux['RENTAB_DIA_PONDERADA_MEC_SAC_FATOR'] = (
        df_aux['RENTAB_DIA_PONDERADA_MEC_SAC']
        + 1.0
        )

    df_aux['ANO_MES'] = df_aux['DT'].dt.to_period('M')

    group_returns = (
        df_aux
        .groupby(group_column + ['ANO_MES'], as_index=False)
        .agg(
            RENTAB_MES_PONDERADA_MEC_SAC=('RENTAB_DIA_PONDERADA_MEC_SAC_FATOR', 'prod'),
            DT=('DT', 'max')
        )
    )

    group_returns['RENTAB_MES_PONDERADA_MEC_SAC'] -= 1.0

    return group_returns[group_column + ['DT', 'RENTAB_MES_PONDERADA_MEC_SAC']]


def build_returns_df(all_dfs):
    """
    Concatenate, sort, and return the final returns DataFrame.

    Expects each input DataFrame to already contain harmonized columns such as
    ['TIPO', 'NOME', 'DT', ...].

    Args:
        all_dfs (list[pd.DataFrame]): List of precomputed return DataFrames.

    Returns:
        pd.DataFrame: Concatenated and sorted DataFrame.
    """
    rentab = pd.concat(all_dfs, ignore_index=True)
    rentab.sort_values(['TIPO', 'NOME', 'DT'], inplace=True)

    return rentab


def compute_aggregate_returns(mec_sac, dcadplanosac):
    """
    Build the aggregated returns table across multiple grouping levels.

    Steps:
      1) Normalize join keys and merge mec_sac with dCadPlanoSAC.
      2) For each group in ['TIPO_PLANO', 'GRUPO', 'INDEXADOR', None]:
         - Compute monthly weighted returns.
         - Compute YTD returns over those monthly values.
         - Rename columns and tag with 'TIPO' and 'NOME' (use 'VIVEST' for consolidated).
      3) Append latest available snapshot per CODCLI as 'PLANO'.
      4) Concatenate all parts and add year/month helper columns.

    Args:
        mec_sac (pd.DataFrame): Per-plan daily returns and PL, includes ['CLCLI_CD', 'DT', ...].
        dcadplanosac (pd.DataFrame): Plan attributes, includes ['CODCLI_SAC', ...].

    Returns:
        pd.DataFrame: Unified returns DataFrame including consolidated and grouped views,
        with helper columns ['ANO', 'MES'].
    """
    dcadplanosac['CODCLI_SAC'] = dcadplanosac['CODCLI_SAC'].astype(str).str.strip()
    mec_sac['CLCLI_CD'] = mec_sac['CLCLI_CD'].astype(str).str.strip()

    mec_sac_dcadplanosac = mec_sac.merge(
        dcadplanosac,
        how='left',
        left_on='CLCLI_CD',
        right_on='CODCLI_SAC'
    )

    all_dfs = []
    groups = ['TIPO_PLANO', 'GRUPO', 'INDEXADOR', None] #None eh o consolidado
    for group in groups:
        df_aux = calculate_weighted_returns_by_group(mec_sac_dcadplanosac, group)
        ytd_returns = calculate_ytd_returns(df_aux, group)
        df_aux['RENTAB_ANO'] = ytd_returns
        df_aux.rename(columns={group: 'NOME',
                               'RENTAB_MES_PONDERADA_MEC_SAC': 'RENTAB_MES',
                               },
                      inplace=True)

        df_aux.insert(0, 'TIPO', group if group else 'CONSOLIDADO')
        if not group:
            df_aux['NOME'] = 'VIVEST'

        all_dfs.append(df_aux)

    idx_last_day = mec_sac_dcadplanosac.groupby('CLCLI_CD')['DT'].idxmax()
    last_day_per_codcli = mec_sac_dcadplanosac.loc[idx_last_day].copy()

    last_day_per_codcli.drop(columns=['VL_PATRLIQTOT1', 'CLCLI_CD', 'CLCLI_CD',
                                      'RENTAB_DIA'], inplace=True)
    last_day_per_codcli['TIPO'] = 'PLANO'
    #renomei coluna NOME para usar o mesmo codigo do lado de fora
    last_day_per_codcli.rename(columns={'NOME': 'nome_old', 'NOME_PLANO': 'NOME'},
                               inplace=True)


    rentab = build_returns_df(all_dfs + [last_day_per_codcli])

    rentab['ANO'] = rentab['DT'].dt.year
    rentab['MES'] = rentab['DT'].dt.month

    return rentab
