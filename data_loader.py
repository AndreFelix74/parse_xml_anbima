#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 12 10:21:25 2025

@author: andrefelix
"""


import os
import pandas as pd


def load_mec_sac_last_day_month(directory_path):
    """
    Loads the row with the latest DT for each CODCLI from each _mecSAC_*.xlsx file.

    Args:
        directory_path (str): Path to the directory containing the _mecSAC files.

    Returns:
        pd.DataFrame: DataFrame containing the latest row per CODCLI from each file.
    """
    dfs = []
    columns = ['CLCLI_CD', 'DT', 'VL_PATRLIQTOT1', 'CODCLI', 'NOME',
               'compute_0016', 'compute_0017']

    for filename in os.listdir(directory_path):
        if filename.startswith('_mecSAC_') and filename.endswith('.xlsx'):
            file_path = os.path.join(directory_path, filename)
            mec_sac = pd.read_excel(file_path)

            if mec_sac.empty:
                print(f"Empty mecSAC file: {filename}")
                continue

            mec_sac['DT'] = pd.to_datetime(mec_sac['DT'], dayfirst=True)

            idx = mec_sac.groupby('CODCLI')['DT'].idxmax()
            last_day_per_codcli = mec_sac.loc[idx][columns].copy()

            dfs.append(last_day_per_codcli)

    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        result.rename(columns={
            'compute_0016': 'RENTAB_MES',
            'compute_0017': 'RENTAB_ANO'
        }, inplace=True)

        return result

    return pd.DataFrame()


def load_cnpb_codcli_mapping(dbaux_path):
    """
    Loads the mapping between CNPB (portfolio code) and CODCLI_SAC (client code)
    by joining dCadPlano and dCadPlanoSAC from dbAux.xlsx.

    Parameters
    ----------
    dbaux_path : str
        Path to the dbAux.xlsx file.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['CNPB', 'CODCLI_SAC'].
    """
    dcadplano = pd.read_excel(f"{dbaux_path}dbAux.xlsx", sheet_name='dCadPlano')
    dcadplanosac = pd.read_excel(f"{dbaux_path}dbAux.xlsx", sheet_name='dCadPlanoSAC')

    dcadplano['COD_PLANO'] = dcadplano['COD_PLANO'].astype(str).str.strip()
    dcadplanosac['COD_PLANO'] = dcadplanosac['COD_PLANO'].astype(str).str.strip()
    dcadplanosac['CODCLI_SAC'] = dcadplanosac['CODCLI_SAC'].astype(str).str.strip()

    # Convert CNPB columns to string before merging
    dcadplano['CNPB'] = dcadplano['CNPB'].astype(str).str.strip()
    dcadplanosac['CNPB'] = dcadplanosac['CNPB'].astype(str).str.strip()

    mapping = dcadplano.merge(
        dcadplanosac,
        on='COD_PLANO',
        how='inner'
    )

    diffs = mapping.loc[mapping['CNPB_x'] != mapping['CNPB_y'], ['COD_PLANO', 'CNPB_x', 'CNPB_y']]
    if not diffs.empty:
        raise ValueError(
            f"Inconsistent CNPB values found after merging:\n{diffs.to_string(index=False)}"
        )

    return mapping.rename(columns={'CNPB_x': 'cnpb'})[['cnpb', 'CODCLI_SAC']]
