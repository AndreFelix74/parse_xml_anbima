#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 19 13:51:44 2025

@author: andrefelix
"""


import pandas as pd
import numpy as np
import util as utl


def add_nome_ativo(entity):
    """
    Creates or modifies the 'NOME_ATIVO' column based on emission name and TPF
    type rules.

    Args:
        entity (pd.DataFrame): The input DataFrame containing asset information.
    """
    entity['NOME_ATIVO'] = entity['NEW_TIPO']

    has_nome_emissor = ~entity['fEMISSOR.NOME_EMISSOR'].isna()

    over = entity['NEW_TIPO'] == 'OVER'

    tipo_tpf = entity['NEW_TIPO'] == 'TPF'

    entity.loc[has_nome_emissor & tipo_tpf & ~over, 'NOME_ATIVO'] = (
        entity['fNUMERACA.TIPO_ATIVO'].fillna('')
        + ' '
        + entity['ANO_VENC_TPF']
    ).str.strip()

    entity.loc[has_nome_emissor & ~tipo_tpf & ~over, 'NOME_ATIVO'] = entity['fEMISSOR.NOME_EMISSOR']


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
    entity['ANO_VENC_TPF'] = entity['DATA_VENC_TPF'].dt.strftime('%Y')


def classify_new_tipo(entity, new_tipo_rules):
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
        new_tipo_rules (dict): Regras de classificação.

    Returns:
        pd.DataFrame com a coluna NEW_TIPO modificada.
    """
    entity['NEW_TIPO'] = entity['tipo'].str.upper()

    for rule_name, rule in new_tipo_rules.items():
        conditions = rule.get('conditions', {})
        new_value = rule.get('new_value')

        mask = pd.Series(True, index=entity.index)

        for col, cond in conditions.items():
            if not col in entity.columns:
                utl.log_message(
                    f"Regra '{rule_name}' descartada. "
                    f"Coluna '{col}' não encontrada.",
                    'warn')
                break
            if isinstance(cond, list):
                mask &= entity[col].isin(cond)
            elif cond == 'NOT_NULL':
                mask &= entity[col].notna()
            else:
                raise ValueError(f"""Condição inválida na regra {rule_name}
                    para coluna '{col}': {cond}""")
        else:
            entity.loc[mask, 'NEW_TIPO'] = new_value


def standardize_asset_names(entity, rules):
    """
    Standardizes asset names by applying prefix replacements and global text replacements.

    Args:
        entity (pd.DataFrame): The input DataFrame.
        rules (dict): Dictionary with 'abbreviations' and 'global_replacements' lists.
    """
    entity['NEW_NOME_ATIVO'] = entity['NOME_ATIVO'].fillna('').str.strip().str.upper()

    for replacement in rules['global_replacements']:
        entity['NEW_NOME_ATIVO'] = entity['NEW_NOME_ATIVO'].str.replace(
            replacement['old'],
            replacement['new'],
            regex=False
        )
        
    for rule in rules['abbreviations']:
        prefix, abbrev = rule['prefix'], rule['abbrev']
        mask = entity['NEW_NOME_ATIVO'].str.startswith(prefix, na=False)
        entity.loc[mask, 'NEW_NOME_ATIVO'] = (
            abbrev
            + ' '
            + entity.loc[mask, 'NEW_NOME_ATIVO'].str[len(prefix):].str.strip()
        )


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


def enrich_and_classify(joined, tipos_serie, name_standardization_rules,
                        new_tipo_rules, gestor_name_stopwords):
    """
    Main function that orchestrates the enrichment of asset data for
    'fundos' and 'carteiras':
    - Applies classification, naming, and enrichment rules.
    """
    joined['FLAG_SERIE'] = np.where(joined['tipo'].isin(tipos_serie), 'SIM', 'NAO')

    classify_new_tipo(joined, new_tipo_rules)
    add_vencimento_tpf(joined)
    add_nome_ativo(joined)

    standardize_asset_names(joined, name_standardization_rules)

    joined['dCadFI_CVM.CLASSE_ANBIMA'] = joined['dCadFI_CVM.CLASSE_ANBIMA'].str.upper()
    joined['NEW_GESTOR'] = joined['dCadFI_CVM.GESTOR'].fillna('VIVEST')
    joined['NEW_GESTOR'] = joined['NEW_GESTOR'].replace('FUNDACAO CESP', 'VIVEST')
    clean_gestor_names_for_wordcloud(joined, gestor_name_stopwords)
