#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 11:37:55 2025

@author: andrefelix
"""


import os
import multiprocessing
import numpy as np
import pandas as pd
from logger import log_timing, RUN_ID

import auxiliary_loaders as aux_loader
from parse_pdf_custodia import cetip, selic
import util as utl
from file_handler import save_df, load_df


def load_config():
    """
    Loads and parses configuration values from config.ini.

    Returns:
        tuple:
            - custodia_source_path (str): Directory containing input PDF files.
            - custodia_destin_path (str): Directory to save final outputs.
            - intermediate_cfg (dict): Debug settings including whether to save
                intermediates.
    """
    config = utl.load_config('config.ini')

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    custodia_source_path = config['Paths']['custodia_source_path']
    custodia_source_path = f"{os.path.dirname(utl.format_path(custodia_source_path))}/"

    custodia_destin_path = config['Paths']['custodia_destin_path']
    custodia_destin_path = f"{os.path.dirname(utl.format_path(custodia_destin_path))}/"

    intermediate_cfg = {
        'save': config['Debug'].get('write_intermediate_files').lower() == 'yes',
        'output_path': config['Debug'].get('intermediate_output_path'),
        'file_format': config['Paths'].get('destination_file_extension')
    }

    return [custodia_source_path, custodia_destin_path, intermediate_cfg,
            data_aux_path, xlsx_destination_path]


def setup_folders(paths):
    """
    Create directories if they do not already exist.

    Args:
        paths (list): List of directory paths to create.
    """
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)

def save_intermediate(dtfrm, filename, config, log):
    """
    Saves an intermediate DataFrame to a unique RUN_ID subfolder.

    Parameters
    ----------
    dtfrm : pandas.DataFrame
        DataFrame to be saved.
    name : str
        Base filename (without extension).
    config : configparser.ConfigParser
        Config object with [Debug] and [Paths] sections.
    run_id : str
        Unique identifier for this pipeline execution.

    Returns
    -------
    str
        Full path of saved file.

    Raises
    ------
    KeyError
        If required configuration keys or sections are missing.
    ValueError
        If file writing is disabled by configuration.
    """
    run_folder = os.path.join(config['output_path'], RUN_ID)
    os.makedirs(run_folder, exist_ok=True)

    full_path = os.path.join(run_folder, filename)
    save_df(dtfrm, full_path, config['file_format'])

    log.info('intermediate_files_saved', arquivo=f"{full_path}.{config['file_format']}")


def find_all_files(files_path, file_ext):
    """
    Recursively finds all files with the given extension and returns metadata.

    Args:
        files_path (str): Root directory.
        file_ext (str): File extension (e.g., '.xml').

    Returns:
        dict: {
            full_path (str): {
                'filename': str,
                'mtime': float
            }
        }
    """
    return {
        os.path.join(root, file): {
            'filename': file,
            'mtime': os.path.getmtime(os.path.join(root, file))
        }
        for root, _, files in os.walk(files_path)
        for file in files
        if file.lower().endswith(file_ext.lower())
    }


def parse_files(custodia_source_path, processes):
    """
    Identifies PDF files and parses their contents in parallel using multiprocessing.

    Args:
        custodia_source_path (str): Path where the source PDF files are located.
        processes (int): Number of processes to use in parallel.

    Returns:
        tuple:
            - parsed_selic_content (list[list[list[str]]]): Parsed rows from SELIC PDFs.
            - parsed_cetip_content (list[list[list[str]]]): Parsed rows from CETIP PDFs.
    """
    with log_timing('parse_custodia', 'load_pdf_files') as log:
        pdf_files_to_process = find_all_files(custodia_source_path, '.pdf')

        pdf_selic_files = []
        pdf_cetip_files = []
        pdf_not_processed = []

        for path, info in pdf_files_to_process.items():
            filename = info['filename']
            if 'POS_SELIC' in filename:
                pdf_selic_files.append(path)
            elif 'POS_CETIP' in filename:
                pdf_cetip_files.append(path)
            else:
                pdf_not_processed.append(path)

        log.info(
            'parse',
            total=len(pdf_files_to_process),
            cetip=len(pdf_cetip_files),
            selic=len(pdf_selic_files),
            dados=[{"pdfs_nao_processados": nome} for nome in pdf_not_processed]
        )

    with log_timing('parse_custodia', 'paser_pdf_content') as log:
        with multiprocessing.Pool(processes=processes) as pool:
            parsed_selic_content = pool.map(selic.parse_file, pdf_selic_files)
            parsed_cetip_content = pool.map(cetip.parse_file, pdf_cetip_files)

    return [parsed_selic_content, parsed_cetip_content]


def convert_parsed_to_dataframe(intermediate_cfg, parsed_selic_content, parsed_cetip_content):
    """
    Converts the parsed raw content from PDF files into structured DataFrames.

    Args:
        parsed_selic_content (list[list[list[str]]]): Raw parsed SELIC data.
        parsed_cetip_content (list[list[list[str]]]): Raw parsed CETIP data.

    Returns:
        tuple:
            - custodia_selic (pd.DataFrame): DataFrame with SELIC custody data.
            - custodia_cetip (pd.DataFrame): DataFrame with CETIP custody data.
    """
    selic_cols = ['conta', 'data ref', 'Carteira c/d', 'Carteira Qtd', 'A revender',
                  'A recomprar', 'isin', 'Fechamento', 'Abertura', 'Titulo Vencimento',
                  'Titulo Nome', 'Titulo Cod', 'arquivo']
    flattened = [row for file_rows in parsed_selic_content for row in file_rows]
    custodia_selic = pd.DataFrame(flattened, columns=selic_cols)
    cols_float = ['Carteira Qtd', 'A revender', 'A recomprar', 'Fechamento', 'Abertura']
    for col in cols_float:
        custodia_selic[col] = custodia_selic[col].astype(float)

    cetip_cols = ['participante', 'codigo', 'data', 'Fundo (IF)', 'Tipo IF',
                  'Data Inicio', 'Data Venc', 'Data Ref', 'Quantidade', 'PU',
                  'Financeiro', 'Tipo Posicao', 'arquivo']
    flattened = [row for file_rows in parsed_cetip_content for row in file_rows]
    custodia_cetip = pd.DataFrame(flattened, columns=cetip_cols)
    cols_float = ['Quantidade', 'PU', 'Financeiro']
    for col in cols_float:
        custodia_cetip[col] = custodia_cetip[col].astype(float)

    if intermediate_cfg['save']:
        with log_timing('parse', 'save_parsed_raw_data') as log:
            save_intermediate(custodia_selic, 'custodia_selic-parsed', intermediate_cfg, log)
            save_intermediate(custodia_cetip, 'custodia_cetip-parsed', intermediate_cfg, log)

    return [custodia_selic, custodia_cetip]


def filter_positions(entity):
    """
    Filter a single positions DataFrame to keep only rows from the last
    available day within each month (dtposicao='yyyymmdd'), keeping the
    minimal reconciliation columns.

    Steps:
      1) Filter NEW_TIPO to the allowed set and select cols_recon.
      2) Convert dtposicao to datetime and compute year-month buckets.
      3) Keep only rows whose date equals the monthly maximum date.
      4) Return dtposicao as 'yyyymmdd' string.

    Returns:
        pd.DataFrame: Subset with columns
            ['cnpj','qtdisponivel','qtgarantia','isin','NEW_TIPO','dtposicao','valor_calc']
        restricted to the last day per month present in this DataFrame.
    """
    cols_recon = ['cnpj', 'qtdisponivel', 'qtgarantia', 'isin',
                  'NEW_TIPO', 'dtposicao', 'valor_calc']
    type_recon = ['TPF', 'OVER', 'TERMORF']

    entity = entity[entity['NEW_TIPO'].isin(type_recon)][cols_recon].copy()

    entity['_dt'] = pd.to_datetime(entity['dtposicao'], format='%Y%m%d')
    entity = entity.dropna(subset=['_dt'])
    entity['_ym'] = entity['_dt'].dt.to_period('M')
    last_dt = entity.groupby('_ym')['_dt'].transform('max')
    entity = entity[entity['_dt'] == last_dt].drop(columns=['_dt', '_ym'])

    entity['dtposicao'] = entity['dtposicao'].astype(str)

    return entity


def build_unified_position(portfolios, funds):
    """
    Concatenate filtered portfolio and fund positions, normalize identifiers
    and numeric fields, compute qttotal, and aggregate.

    Processing:
      - Rename 'cnpjcpf' to 'cnpj' (no-op if already renamed).
      - Concatenate portfolios and funds.
      - Normalize CNPJ as zero-padded 14-char string.
      - Cast qtdisponivel, qtgarantia, valor_calc to float.
      - Compute qttotal = qtdisponivel + qtgarantia.
      - Group by ['cnpj','isin','dtposicao'] summing ['qttotal','valor_calc'].

    Returns:
        pd.DataFrame: Aggregated positions with
            ['cnpj','isin','dtposicao','qttotal','valor_calc'].
    """
    position = pd.concat([portfolios, funds], ignore_index=True)

    position['cnpj'] = position['cnpj'].astype(str).str.zfill(14)

    position['qtdisponivel'] = position['qtdisponivel'].astype(float)
    position['qtgarantia'] = position['qtgarantia'].astype(float)
    position['qttotal'] = position['qtdisponivel'] + position['qtgarantia']
    position['valor_calc'] = position['valor_calc'].astype(float)

    position = (position
        .groupby(['cnpj', 'isin', 'dtposicao'], as_index=False)[['qttotal', 'valor_calc']]
        .sum()
    )

    return position


def normalize_dcad_crt_brad(dcad_crt_brad):
    """
    Normalize identifiers in dcad_crt_brad:
      - CNPJ zero-padded to 14 digits
      - SELIC zero-padded to 9 digits (or None if missing)
      - CETIP zero-padded to 8 digits with final dash format

    Returns:
        None: Normalized inplace
    """
    dcad_crt_brad['cnpj'] = dcad_crt_brad['CNPJ'].astype(str).str.zfill(14)
    dcad_crt_brad['SELIC'] = np.where(
        dcad_crt_brad['SELIC'].notnull(),
        dcad_crt_brad['SELIC'].astype(str).str.zfill(9),
        None
    )
    dcad_crt_brad['CETIP'] = dcad_crt_brad['CETIP'].astype(str).str.zfill(8)
    dcad_crt_brad['CETIP'] = (
        dcad_crt_brad['CETIP'].str.slice(0, -1)
        + '-'
        + dcad_crt_brad['CETIP'].str.slice(-1)
        )


def reconciliation(unified_position, dcad_crt_brad, custodia_selic, custodia_cetip):
    unified_position = unified_position.merge(
        dcad_crt_brad[['cnpj', 'SELIC', 'CETIP']],
        left_on=['cnpj'],
        right_on=['cnpj'],
        how='inner'
        )

    unified_position['dtposicao'] = unified_position['dtposicao'].astype(str)
    custodia_selic['data ref'] = custodia_selic['data ref'].astype('datetime64[s]').dt.strftime('%Y%m%d')
    custodia_cetip['data'] = custodia_cetip['data'].astype('datetime64[s]').dt.strftime('%Y%m%d')

    cols_selic = ['conta', 'data ref', 'isin', 'Titulo Vencimento',
                  'Titulo Nome', 'Titulo Cod', 'arquivo']
    selic_pos = custodia_selic.groupby(cols_selic, as_index=False)['Fechamento'].sum()
    selic_pos.rename(columns={'data ref': 'dtposicao', 'conta': 'SELIC'}, inplace=True)
    recon_selic = unified_position.merge(
        selic_pos,
        on=['SELIC', 'dtposicao', 'isin'],
        how='outer'
        )
    recon_selic.drop(columns='CETIP', inplace=True)
    recon_selic['dif_xml_selic'] = (
        (recon_selic['qttotal'].fillna(0) - recon_selic['Fechamento'].fillna(0)).abs()
    )

    recon_cetip = unified_position.merge(
        custodia_cetip,
        left_on=['CETIP', 'dtposicao', 'isin'],
        right_on=['codigo', 'data', 'Fundo (IF)'],
        how='left'
        )

    recon_cetip.drop(columns='SELIC', inplace=True)

    return [recon_selic, recon_cetip]


def run_pipeline():
    """
    Main entry point for the custody PDF parsing pipeline.
    
    Loads configuration, sets up folders, parses files in parallel, 
    converts results into DataFrames, and saves final outputs.
    """
    (
     custodia_source_path,
     custodia_destin_path,
     intermediate_cfg,
     data_aux_path,
     xlsx_destination_path
     ) = load_config()

    setup_folders([custodia_destin_path])

    processes = min(8, multiprocessing.cpu_count())

    parsed_selic_content, parsed_cetip_content = parse_files(custodia_source_path, processes)
    custodia_selic, custodia_cetip = convert_parsed_to_dataframe(intermediate_cfg,
                                                                 parsed_selic_content,
                                                                 parsed_cetip_content)

    file_frmt = intermediate_cfg['file_format']

    with log_timing('reconciliation', 'load_aux_data'):
        dcad_crt_brad = aux_loader.load_dcad_crt_brad(data_aux_path)
        portfolios = load_df(f"{xlsx_destination_path}carteiras", file_frmt)
        funds = load_df(f"{xlsx_destination_path}fundos", file_frmt)

    with log_timing('reconciliation', 'normalize_data'):
        normalize_dcad_crt_brad(dcad_crt_brad)
        portfolios.rename(columns={'cnpjcpf': 'cnpj'}, inplace=True)
        portfolios = filter_positions(portfolios)
        funds = filter_positions(funds)
        unified_position = build_unified_position(portfolios, funds)

    with log_timing('reconciliation', 'merging'):
        recon_selic, recon_cetip = reconciliation(unified_position, dcad_crt_brad,
                                                  custodia_selic, custodia_cetip)

    file_frmt = 'xlsx' #intermediate_cfg['file_format']
    with log_timing('finish_custodia', 'save_final_files'):
        save_df(custodia_selic, f"{custodia_destin_path}custodia_selic", file_frmt)
        save_df(custodia_cetip, f"{custodia_destin_path}custodia_cetip", file_frmt)
        save_df(recon_selic, f"{custodia_destin_path}reconciliacao_selic", file_frmt)
        save_df(recon_cetip, f"{custodia_destin_path}reconciliacao_cetip", file_frmt)


if __name__ == "__main__":
    run_pipeline()
