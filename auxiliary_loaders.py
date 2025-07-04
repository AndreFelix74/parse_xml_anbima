#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 30 17:43:29 2025

@author: andrefelix
"""


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
