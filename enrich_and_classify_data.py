#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 19 13:51:44 2025

@author: andrefelix
"""


import os
import pandas as pd
import numpy as np
import util as utl
import data_access as dta
import file_handler as fhdl


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


def add_nome_ativo(entity):
    """
    Creates or modifies the 'NOME_ATIVO' column based on emission name and TPF
    type rules.

    Args:
        entity (pd.DataFrame): The input DataFrame containing asset information.
    """
    entity['NOME_ATIVO'] = entity['NEW_TIPO']

    nome_emissor_nulo = entity['fEMISSOR.NOME_EMISSOR'].isna()
    tipo_tpf = entity['NEW_TIPO'] == 'TPF'

    entity.loc[~nome_emissor_nulo, 'NOME_ATIVO'] = entity['fEMISSOR.NOME_EMISSOR']
    entity.loc[tipo_tpf & ~nome_emissor_nulo, 'NOME_ATIVO'] = (
        entity['fNUMERACA.TIPO_ATIVO'].fillna('')
        + ' '
        + entity['ANO_VENC_TPF'].fillna('').astype(str)
    ).str.strip()


def add_vencimento_tpf(entity):
    """
    Creates the 'ANO_VENC_TPF' column based on the 'dtvencimento' date for TPF
    and OVER types.

    Args:
        entity (pd.DataFrame): The input DataFrame. Assumes presence of
        'NEW_TIPO' and 'dtvencimento'.
    """
    #carteiras nao tem dtvencativo
    default_column = entity['dtvencativo'] if 'dtvencativo' in entity.columns else pd.NaT

    dt_venc_aux = np.where(
        entity['NEW_TIPO'].isin(['TPF', 'OVER']),
        entity['dtvencimento'],
        default_column
    )

    dt_venc_indices = pd.Series(dt_venc_aux, index=entity.index)

    entity['DATA_VENC_TPF'] = pd.to_datetime(dt_venc_indices, errors='raise')
    entity['ANO_VENC_TPF'] = entity['DATA_VENC_TPF'].dt.year


def classify_new_tipo(entity, config):
    """
    Classifica a coluna NEW_TIPO com base em regras declaradas em um dicionário JSON.
    A chave do dicionário é o nome da regra usado como descrição.
    As condições podem ser:
      - uma lista de valores aceitos, mesmo que seja um valor deve estar em lista
      - a string 'NOT_NULL' para testar se o campo não é nulo

    Exemplo de estrutura esperada:
        {
            'Nome da Regra': {
                'conditions': {
                    'coluna1': ['VALOR1', 'VALOR2'],
                    'coluna2': 'NOT_NULL'
                },
                'new_value': 'VALOR_FINAL'
            },
            ...
        }

    Args:
        entity (pd.DataFrame): DataFrame de entrada.
        config (dict): Regras de classificação.

    Returns:
        pd.DataFrame com a coluna NEW_TIPO modificada.
    """
    entity['NEW_TIPO'] = entity['tipo'].str.upper()

    for rule_name, rule in config.items():
        conditions = rule.get('conditions', {})
        new_value = rule.get('new_value')

        mask = pd.Series(True, index=entity.index)

        for col, cond in conditions.items():
            if not col in entity.columns:
                continue
            if isinstance(cond, list):
                mask &= entity[col].isin(cond)
            elif cond == 'NOT_NULL':
                mask &= entity[col].notna()
            else:
                raise ValueError(f"""Condição inválida na regra {rule_name}
                    para coluna '{col}': {cond}""")

        entity.loc[mask, 'NEW_TIPO'] = new_value


def standardize_asset_names(entity, rules):
    """
    Standardizes asset names by applying prefix replacements and global text replacements.

    Args:
        entity (pd.DataFrame): The input DataFrame.
        rules (dict): Dictionary with 'abbreviations' and 'global_replacements' lists.
    """
    entity['NEW_NOME_ATIVO'] = entity['NOME_ATIVO'].fillna('').str.strip().str.upper()

    for rule in rules['abbreviations']:
        prefix, abbrev = rule['prefix'], rule['abbrev']
        mask = entity['NEW_NOME_ATIVO'].str.startswith(prefix, na=False)
        entity.loc[mask, 'NEW_NOME_ATIVO'] = (
            abbrev
            + ' '
            + entity.loc[mask, 'NEW_NOME_ATIVO'].str[len(prefix):].str.strip()
        )

    for replacement in rules['global_replacements']:
        entity['NEW_NOME_ATIVO'] = entity['NEW_NOME_ATIVO'].str.replace(
            replacement['old'],
            replacement['new'],
            regex=False
        )


def merge_cad_plano(entity, dcadplano):
    """
    Merges the input entity DataFrame with dCadPlano on 'cnpb', if it exists.

    Args:
        entity (pd.DataFrame): The entity DataFrame with a potential 'cnpb' column.
        dcadplano (pd.DataFrame): The dCadPlano DataFrame.

    Returns:
        pd.DataFrame: Merged DataFrame if applicable, otherwise the original.
    """
    if 'cnpb' in entity.columns:
        return entity.merge(
            dcadplano.add_prefix('dCadPlano.'),
            left_on='cnpb',
            right_on='dCadPlano.CNPB',
            how='left'
        )

    return entity


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
    tables_aux = [
        {'name': 'numeraca',
         'cols': ['COD_ISIN', 'COD_EMISSOR', 'DESCRICAO', 'TIPO_ATIVO']
         },
        {'name': 'emissor',
         'cols': ['COD_EMISSOR', 'NOME_EMISSOR', 'CNPJ_EMISSOR']
         }
    ]

    for table in tables_aux:
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

    numeraca, emissor = aux_tables

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

    return db_cad_fi_cvm


def clean_gestor_names_for_wordcloud(entity, stopwords=None):
    """
    Cleans asset manager names for word cloud generation by removing unwanted words.

    Args:
        df (pd.DataFrame): Input DataFrame with asset manager names.
        column (str): Name of the column containing the raw names.
        output_column (str): Name of the new column with cleaned names.
        stopwords (list or set): List of words to remove (case-insensitive).

    Returns:
        pd.DataFrame: DataFrame with the new cleaned column added.
    """
    if stopwords is None:
        stopwords = set()

    stopwords = set(word.upper() for word in stopwords)

    def clean_text(text):
        if pd.isna(text):
            return ''
        words = str(text).upper().split()
        filtered_words = [w for w in words if w not in stopwords]
        return ' '.join(filtered_words)

    entity['NEW_GESTOR_WORD_CLOUD'] = entity['NEW_GESTOR'].apply(clean_text)


def explode_partplanprev_and_allocate(portfolios, types_to_exclude):
    """
    Decomposes aggregated allocations of type 'partplanprev' into proportional
    entries based on real underlying assets in the 'portfolios' dataset.

    This function is specific to portfolios that contain entries of type
    'partplanprev', which represent consolidated participation (e.g., of
    beneficiaries or plans). For each aggregated record, it generates new
    rows representing proportional allocations across the actual portfolio
    assets, using the 'percpart' percentage.

    Parameters
    ----------
    portfolios : pandas.DataFrame
        Must include ['percpart', 'valor_calc', 'codcart', 'nome', 'cnpb', 'dtposicao', 'tipo'].

    types_to_exclude : list of str
        A list of non-asset types that should be excluded from the allocation process.
        Typically includes series-like records or auxiliary types.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the newly generated rows, each representing a
        proportional allocation from a 'partplanprev' entry. Includes:
        - 'valor_calc': calculated based on the original percentage.
        - 'flag_rateio': a flag set to 0, indicating generated allocation rows.

    Raises
    ------
    ValueError
        If any required columns are missing from the input DataFrame.

    Notes
    -----
    - The function performs an inner join between 'partplanprev' entries and
      the actual underlying assets of the portfolio to compute proportional values.
    - This process effectively expands the data structure by creating new rows.
    """
    if portfolios[portfolios['tipo'] == 'partplanprev'].empty:
        return portfolios

    partplanprev = portfolios[portfolios['tipo'] == 'partplanprev'][
        ['codcart', 'nome', 'percpart', 'cnpb', 'dtposicao']
    ]

    assets_to_allocate = portfolios[
        ~portfolios['tipo'].isin(types_to_exclude + ['partplanprev'])
    ].drop(columns=['cnpb', 'percpart'])

    assets_to_allocate = assets_to_allocate.copy()
    assets_to_allocate['original_index'] = assets_to_allocate.index

    allocated_assets = partplanprev.merge(
        assets_to_allocate.dropna(subset=['valor_calc']),
        on=['codcart', 'nome', 'dtposicao'],
        how='inner'
    )

    allocated_assets['percpart'] = pd.to_numeric(allocated_assets['percpart'], errors='raise')
    allocated_assets['valor_calc'] = pd.to_numeric(allocated_assets['valor_calc'], errors='raise')

    allocated_assets['valor_calc'] = (
        allocated_assets['percpart'] * allocated_assets['valor_calc'] / 100.0
    )

    allocated_assets['flag_rateio'] = 0

    portfolios['flag_rateio'] = portfolios.index.isin(allocated_assets['original_index'].unique()).astype(int)

    portfolios = pd.concat([
        portfolios,
        allocated_assets
    ], ignore_index=True)

    portfolios['valor_calc'] = portfolios['valor_calc'].where(portfolios['flag_rateio'] != 1, 0)

    return portfolios


def main():
    """
    Main function that orchestrates the enrichment of asset data for
    'fundos' and 'carteiras':
    - Loads configurations and metadata.
    - Loads and merges auxiliary and CVM data.
    - Applies classification, naming, and enrichment rules.
    - Outputs enriched Excel files.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"
    file_ext = config['Paths'].get('destination_file_extension', 'xlsx')

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"
    dbaux_path = f"{data_aux_path}dbAux.xlsx"

    header_daily_values = dta.read('header_daily_values')
    tipos_serie = [key for key, value in header_daily_values.items() if value.get('serie', False)]

    name_standardization_rules = dta.read('name_standardization_rules')
    new_tipo_rules = dta.read('enrich_de_para_tipos')

    dcadplano = pd.read_excel(f"{dbaux_path}", sheet_name='dCadPlano', dtype=str)
    aux_asset = load_assets_aux(data_aux_path)
    cad_fi_cvm = load_db_cad_fi_cvm(data_aux_path)
    cad_fi_cvm = cad_fi_cvm.add_prefix('dCadFI_CVM.')

    entities = ['fundos', 'carteiras']

    for entity_name in entities:
        dtypes = dta.read(f"{entity_name}_metadata")
        file_name = f"{xlsx_destination_path}{entity_name}_values_cleaned"
        entity = fhdl.load_df(file_name, file_ext, dtypes)

        entity = explode_partplanprev_and_allocate(entity, tipos_serie)

        entity['FLAG_SERIE'] = np.where(entity['tipo'].isin(tipos_serie), 'SIM', 'NAO')

        entity = entity.merge(
            aux_asset,
            left_on='isin',
            right_on='fNUMERACA.COD_ISIN',
            how='left'
        )

        entity = merge_cad_plano(entity, dcadplano)
        classify_new_tipo(entity, new_tipo_rules)
        add_vencimento_tpf(entity)
        add_nome_ativo(entity)

        left_col = 'cnpj' if entity_name == 'fundos' else 'fEMISSOR.CNPJ_EMISSOR'
        entity = entity.merge(
            cad_fi_cvm,
            left_on=left_col,
            right_on='dCadFI_CVM.CNPJ_FUNDO',
            how='left'
        )

        standardize_asset_names(entity, name_standardization_rules)

        entity['dCadFI_CVM.CLASSE_ANBIMA'] = entity['dCadFI_CVM.CLASSE_ANBIMA'].str.upper()
        entity['NEW_GESTOR'] = entity['dCadFI_CVM.GESTOR'].fillna('VIVEST')
        entity['NEW_GESTOR'] = entity['NEW_GESTOR'].replace('FUNDACAO CESP', 'VIVEST')
        clean_gestor_names_for_wordcloud(entity, ['LTDA', 'A', 'DTVM', 'GESTAO',
                                                  'GESTÃO', 'S', 'RECURSOS',
                                                  'INVESTIMENTOS', 'LIMITADA',
                                                  'ASSET', 'BRASIL', 'UNIBANCO',
                                                  'DE', 'BANCO', 'PARIBAS', 'COMPANY',
                                                  'MANAGEMENT', ])

        file_name = f"{xlsx_destination_path}{entity_name}_enriched"
        fhdl.save_df(entity, file_name, file_ext)


if __name__ == "__main__":
    main()
