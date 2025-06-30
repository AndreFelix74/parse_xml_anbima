#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 11:37:55 2025

@author: andrefelix
"""


import os
import multiprocessing
import pandas as pd
from logger import log_timing

from parse_pdf_custodia import cetip, selic
import util as utl
from file_handler import save_df


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

    custodia_source_path = config['Paths']['custodia_source_path']
    custodia_source_path = f"{os.path.dirname(utl.format_path(custodia_source_path))}/"

    custodia_destin_path = config['Paths']['custodia_destin_path']
    custodia_destin_path = f"{os.path.dirname(utl.format_path(custodia_destin_path))}/"

    intermediate_cfg = {
        'save': config['Debug'].get('write_intermediate_files').lower() == 'yes',
        'output_path': config['Debug'].get('intermediate_output_path'),
        'file_format': config['Paths'].get('destination_file_extension')
    }

    return [custodia_source_path, custodia_destin_path, intermediate_cfg]


def setup_folders(paths):
    """
    Create directories if they do not already exist.

    Args:
        paths (list): List of directory paths to create.
    """
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


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


def convert_parsed_to_dataframe(parsed_selic_content, parsed_cetip_content):
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

    cetip_cols = ['codigo', 'participante', 'Fundo (IF)', 'Tipo IF', 'Data Inicio',
                  'Data Venc', 'Data Ref', 'Quantidade', 'PU', 'Financeiro',
                  'Tipo Posicao', 'arquivo']
    flattened = [row for file_rows in parsed_cetip_content for row in file_rows]
    custodia_cetip = pd.DataFrame(flattened, columns=cetip_cols)
    cols_float = ['Quantidade', 'PU', 'Financeiro']
    for col in cols_float:
        custodia_cetip[col] = custodia_cetip[col].astype(float)

    return [custodia_selic, custodia_cetip]


def run_pipeline():
    """
    Main entry point for the custody PDF parsing pipeline.
    
    Loads configuration, sets up folders, parses files in parallel, 
    converts results into DataFrames, and saves final outputs.
    """
    custodia_source_path, custodia_destin_path, intermediate_cfg = load_config()

    setup_folders([custodia_destin_path])

    processes = min(8, multiprocessing.cpu_count())

    parsed_selic_content, parsed_cetip_content = parse_files(custodia_source_path, processes)
    custodia_selic, custodia_cetip = convert_parsed_to_dataframe(parsed_selic_content,
                                                                 parsed_cetip_content)

    with log_timing('finish_custodia', 'save_final_files'):
        file_frmt = intermediate_cfg['file_format']
        save_df(custodia_selic, f"{custodia_destin_path}custodia_selic", file_frmt)
        save_df(custodia_cetip, f"{custodia_destin_path}custodia_cetip", file_frmt)


if __name__ == "__main__":
    run_pipeline()
