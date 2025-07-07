#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 30 17:43:29 2025

@author: andrefelix
"""


import os
import pandas as pd
import data_access as dta


def convert_column_types(dtfrm, dtype_map):
    """
    Converts specified columns in a DataFrame to their target data types
    ('date' or 'number').

    Args:
        dtfrm (pd.DataFrame): The DataFrame to modify.
        dtype_map (dict): A mapping of column names to type strings
        ('date' or 'number').

    Raises:
        ValueError: If an unsupported type is provided in the map.
    """
    for col, tipo in dtype_map.items():
        if tipo == 'date':
            dtfrm[col] = pd.to_datetime(dtfrm[col], errors='coerce')
        elif tipo == 'number':
            dtfrm[col] = pd.to_numeric(dtfrm[col], errors='coerce')
        else:
            raise ValueError(f"Tipo não suportado: {tipo}")


def load_assets_aux(data_aux_path):
    """
    Loads auxiliary tables for asset identification (numeraca and emissor) and
    merges them.

    Args:
        data_aux_path (str): Path to the auxiliary data directory.

    Returns:
        pd.DataFrame: Merged DataFrame with prefix 'fNUMERACA.' and 'fEMISSOR.'
        on columns.
    """
    aux_tables = []
    tables_def = [
        {'name': 'numeraca',
         'cols': ['COD_ISIN', 'COD_EMISSOR', 'DESCRICAO', 'TIPO_ATIVO']
         },
        {'name': 'emissor',
         'cols': ['COD_EMISSOR', 'NOME_EMISSOR', 'CNPJ_EMISSOR']
         }
    ]

    for table in tables_def:
        table_name = table['name']
        cols = table['cols']

        cols_names = dta.read(f"{table_name}_columns")
        dtypes = dta.read(f"{table_name}_dtypes")

        table_aux = pd.read_csv(f"{data_aux_path}{table_name.upper()}.TXT",
                                header=None,
                                names=cols_names,
                                encoding='utf-8',
                                dtype=str)
        convert_column_types(table_aux, dtypes)
        aux_tables.append(table_aux[cols])

    numeraca = aux_tables[0]
    emissor = aux_tables[1]

    return numeraca.add_prefix('fNUMERACA.').merge(
        emissor.add_prefix('fEMISSOR.'),
        left_on='fNUMERACA.COD_EMISSOR',
        right_on='fEMISSOR.COD_EMISSOR',
        how='left'
    )


def load_db_cad_fi_cvm(data_aux_path):
    """
    Loads and cleans the CVM fund registration database.

    Args:
        data_aux_path (str): Path to the CSV file 'dbCadFI_CVM.csv'.

    Returns:
        pd.DataFrame: Cleaned and typed DataFrame filtered by operational funds.
    """
    db_cad_fi_cvm = pd.read_csv(f"{data_aux_path}dbCadFI_CVM.csv",
                                sep=';',
                                encoding='latin1',
                                dtype=str)

    db_cad_fi_cvm = db_cad_fi_cvm[db_cad_fi_cvm['SIT'] == 'EM FUNCIONAMENTO NORMAL']

    db_cad_fi_cvm['CNPJ_FUNDO'] = (
        db_cad_fi_cvm['CNPJ_FUNDO']
        .str.replace('.', '', regex=False)
        .str.replace('/', '', regex=False)
        .str.replace('-', '', regex=False)
    )

    cols_date = ['DT_REG', 'DT_CONST', 'DT_CANCEL', 'DT_INI_SIT', 'DT_INI_ATIV',
                 'DT_INI_EXERC', 'DT_FIM_EXERC', 'DT_PATRIM_LIQ']
    for col in cols_date:
        db_cad_fi_cvm[col] = pd.to_datetime(db_cad_fi_cvm[col], errors='raise')

    db_cad_fi_cvm['CD_CVM'] = pd.to_numeric(
        db_cad_fi_cvm['CD_CVM'],
        errors='raise',
        downcast='integer'
    )
    db_cad_fi_cvm['VL_PATRIM_LIQ'] = pd.to_numeric(
        db_cad_fi_cvm['VL_PATRIM_LIQ'],
        errors='raise'
    ) / 100

    return db_cad_fi_cvm.add_prefix('dCadFI_CVM.')


def load_dcadplano(data_aux_path):
    """
    Loads the 'dCadPlano' sheet from the dbAux Excel file.

    Args:
        data_aux_path (str): Path to the directory containing 'dbAux.xlsx'.

    Returns:
        pd.DataFrame: Loaded DataFrame from the 'dCadPlano' sheet.
    """
    dbaux_path = f"{data_aux_path}dbAux.xlsx"
    return pd.read_excel(dbaux_path, sheet_name='dCadPlano', dtype=str)


def load_enrich_auxiliary_data(data_aux_path):
    """
    Loads all auxiliary datasets required for data enrichment and classification.

    Args:
        data_aux_path (str): Path to the directory containing auxiliary data files.

    Returns:
        dict: A dictionary with the following keys:
            - 'dcadplano': DataFrame from the 'dCadPlano' sheet in dbAux.xlsx
            - 'assets': Merged DataFrame from numeraca and emissor
            - 'cad_fi_cvm': Cleaned and prefixed CVM fund registration DataFrame
    """
    return {
        'dcadplano': load_dcadplano(data_aux_path),
        'assets': load_assets_aux(data_aux_path),
        'cad_fi_cvm': load_db_cad_fi_cvm(data_aux_path),
    }


def load_governance_struct(data_aux_path):
    """
    Loads the 'dEstruturaGerencial' sheet from the dbAux Excel file.

    Args:
        data_aux_path (str): Path to the directory containing 'dbAux.xlsx'.

    Returns:
        pd.DataFrame: Loaded DataFrame from the 'dEstruturaGerencial' sheet.
    """
    dbaux_path = f"{data_aux_path}dbAux.xlsx"
    return pd.read_excel(dbaux_path, sheet_name='dEstruturaGerencial', dtype=str)


def load_range_eom(data_aux_path):
    """
    Loads the 'dDataMes' sheet from the dbAux Excel file.

    Args:
        data_aux_path (str): Path to the directory containing 'dbAux.xlsx'.

    Returns:
        pd.DataFrame: Loaded DataFrame from the 'dEstruturaGerencial' sheet.
    """
    dbaux_path = f"{data_aux_path}dbAux.xlsx"
    return pd.read_excel(dbaux_path, sheet_name='dDataMes', dtype=str)


def load_returns_by_puposicao(data_aux_path):
    """
    Loads the saved returns from 'isin_rentab.xlsx' if available, or returns
    an empty template.

    Args:
        data_aux_path (str): Path to the directory containing 'isin_rentab.xlsx'.

    Returns:
        pd.DataFrame: DataFrame with columns ['isin', 'dtposicao',
                                              'puposicao', 'rentab'].
                      If the file does not exist, returns an empty DataFrame
                      with the correct schema.
    """
    returns_path = f"{data_aux_path}isin_rentab.xlsx"

    try:
        returns_by_puposicao = pd.read_excel(returns_path, dtype=str)
    except FileNotFoundError:
        returns_by_puposicao = pd.DataFrame({
            'isin': pd.Series(dtype='str'),
            'dtposicao': pd.Series(dtype='datetime64[ns]'),
            'puposicao': pd.Series(dtype='float'),
            'rentab': pd.Series(dtype='float')
            })

    return returns_by_puposicao


def load_mec_sac_last_day_month(data_aux_path):
    """
    Loads the row with the latest DT for each CODCLI from each _mecSAC_*.xlsx file.

    Args:
        data_aux_path (str): Path to the directory containing the _mecSAC files.

    Returns:
        pd.DataFrame: DataFrame containing the latest row per CODCLI from each file.
    """
    dfs = []
    columns = ['CLCLI_CD', 'DT', 'VL_PATRLIQTOT1', 'CODCLI', 'NOME',
               'compute_0016', 'compute_0017']

    for filename in os.listdir(data_aux_path):
        if filename.startswith('_mecSAC_') and filename.endswith('.xlsx'):
            file_path = os.path.join(data_aux_path, filename)
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
        result['CLCLI_CD'] = result['CLCLI_CD'].astype(str).str.strip()
        result['CODCLI'] = result['CODCLI'].astype(str).str.strip()
        result.rename(columns={
            'compute_0016': 'RENTAB_MES',
            'compute_0017': 'RENTAB_ANO'
        }, inplace=True)

        return result

    return pd.DataFrame()


def load_cnpb_codcli_mapping(data_aux_path):
    """
    Loads the mapping between CNPB (portfolio code) and CODCLI_SAC (client code)
    by joining dCadPlano and dCadPlanoSAC from dbAux.xlsx.

    Parameters
    ----------
    data_aux_path : str
        Path to the dbAux.xlsx file.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['CNPB', 'CODCLI_SAC'].
    """
    dcadplano = pd.read_excel(f"{data_aux_path}dbAux.xlsx", sheet_name='dCadPlano')
    dcadplanosac = pd.read_excel(f"{data_aux_path}dbAux.xlsx", sheet_name='dCadPlanoSAC')

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


def load_dcadplanosac(data_aux_path):
    """
    Loads the dCadPlanoSAC sheet from dbAux.xlsx.

    Args:
        data_aux_path (str): Path to dbAux.xlsx.

    Returns:
        pd.DataFrame: dCadPlanoSAC sheet as DataFrame.
    """
    dcadplanosac = pd.read_excel(f"{data_aux_path}dbAux.xlsx",
                                 sheet_name='dCadPlanoSAC',
                                 dtype=str)

    # Substitui carteira com contencioso pela carteira sem contencioso (soh investimentos)
    dcadplanosac['CODCLI_SAC'] = (
        dcadplanosac['CODCLI_SAC_INVEST'].where(
            dcadplanosac['CODCLI_SAC_INVEST'].notnull(),
            dcadplanosac['CODCLI_SAC']
        )
    )

    return dcadplanosac
