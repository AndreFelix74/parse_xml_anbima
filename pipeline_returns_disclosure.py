#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 23 16:21:21 2025

@author: andrefelix
"""

import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import pandas as pd

import auxiliary_loaders as aux_loader
import util as utl
from file_handler import save_df
from logger import log_timing
from data_io import auth_provider as auth, maestro_api as api
from returns_disclosure import (
    compute_aggregate_returns,
    reconcile_entities_ids,
    reconcile_monthly_returns,
    reconcile_annually_returns
    )


def load_config():
    """
    Load configuration paths from config.ini and validate sections.

    Returns:
        list[str]: A list containing [xlsx_destination_path, data_aux_path, mec_sac_path].

    Raises:
        KeyError: If required sections [Debug] or [Paths] are missing.
    """
    config = utl.load_config('config.ini')

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"

    mec_sac_path = config['Paths']['mec_sac_path']
    mec_sac_path = f"{os.path.dirname(utl.format_path(mec_sac_path))}/"

    if not config.has_section('Debug'):
        raise KeyError('Missing [Debug] section in config.ini')

    if not config.has_section('Paths'):
        raise KeyError('Missing [Paths] section in config.ini')

    return [xlsx_destination_path, data_aux_path, mec_sac_path]


def find_all_mecsac_files(files_path):
    """
    Recursively find all mec_sac Excel files and return metadata.

    Args:
        files_path (str): Root directory.

    Returns:
        dict: Mapping full file paths to metadata with keys:
            - 'filename' (str): File name.
            - 'mtime' (float): Last modification time.
    """
    return {
        os.path.join(root, file): {
            'filename': file,
            'mtime': os.path.getmtime(os.path.join(root, file))
        }
        for root, _, files in os.walk(files_path)
        for file in files
        if file.startswith('_mecSAC_') and file.endswith('.xlsx')
    }


def load_mecsac(mec_source_path, processes):
    """
    Load all mec_sac Excel files in parallel.

    Args:
        mec_source_path (str): Directory containing mec_sac files.
        processes (int): Number of parallel worker processes.

    Returns:
        pd.DataFrame: Concatenated DataFrame with all mec_sac data.
    """
    with log_timing('load', 'find_mecsac_files') as log:
        all_mecsac_files = find_all_mecsac_files(mec_source_path)

        log.info(
            'load',
            total=len(all_mecsac_files),
        )

    with log_timing('load', 'load_mecsac_content') as log:
        with ProcessPoolExecutor(max_workers=processes) as executor:
            dfs = list(executor.map(aux_loader.load_mecsac_file,
                                    all_mecsac_files))

    mec_sac = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return mec_sac


def compute_aggregate(data_aux_path, mec_sac_path):
    """
    Compute aggregate returns by merging mec_sac data with dCadPlanoSAC.

    Args:
        data_aux_path (str): Path to auxiliary data files.
        mec_sac_path (str): Path to mec_sac Excel files.

    Returns:
        pd.DataFrame: Aggregated returns DataFrame.
    """
    processes = min(8, multiprocessing.cpu_count())

    mec_sac = load_mecsac(mec_sac_path, processes)

    dcadplanosac = aux_loader.load_dcadplanosac(data_aux_path)

    return compute_aggregate_returns(mec_sac, dcadplanosac)


def reconcile_entities(api_ctx, returns_mecsac):
    """
    Reconcile entity IDs in local returns DataFrame with Maestro API entities.

    Args:
        api_ctx (dict): API context used to call Maestro endpoints.
        returns_mecsac (pd.DataFrame): Local returns DataFrame to update with 'api_id'.
    """
    api_entities_map = {
        'GRUPO': '/investimentos/Grupos',
        'INDEXADOR': '/investimentos/Indexadores',
        'PLANO': '/investimentos/Planos',
        'TIPO_PLANO': '/investimentos/TiposPlanos',
    }

    for tipo, endpoint in api_entities_map.items():
        api_resp = api.api_get(api_ctx, endpoint)
        api_data = pd.DataFrame(api_resp.json())
        reconcile_entities_ids(returns_mecsac, tipo, api_data)


def reconcile_returns(api_ctx, returns_mecsac):
    """
    Reconcile monthly and annual returns with Maestro API data.

    Args:
        api_ctx (dict): API context used to call Maestro endpoints.
        returns_mecsac (pd.DataFrame): Local returns DataFrame.

    Returns:
        pd.DataFrame: Merged DataFrame with Maestro monthly and annual returns.
    """
    result = returns_mecsac.copy()

    api_entities_map = {
        'MENSAL': '/investimentos/Rentabilidades/mensais',
        'ANUAL': '/investimentos/Rentabilidades/anuais',
    }

    for tipo, endpoint in api_entities_map.items():
        api_resp = api.api_get(api_ctx, endpoint)
        api_data = pd.DataFrame(api_resp.json())

        if tipo == 'MENSAL':
            returns_mecsac_maestro = reconcile_monthly_returns(result, api_data)
        elif tipo == 'ANUAL':
            returns_mecsac_maestro = reconcile_annually_returns(result, api_data)

    return returns_mecsac_maestro


def main():
    """
    Main orchestration function.

    Loads configuration, authenticates with Maestro API, computes aggregate returns,
    reconciles entities and returns, and saves all outputs to files.
    """
    tenant_id = os.environ['TENANT_ID']
    client_id = os.environ['CLIENT_ID']
    client_secret = os.environ['CLIENT_SECRET']
    scope = os.environ['SCOPE']
    api_base = os.environ['API_BASE']

    auth_ctx = auth.new_auth_context(tenant_id, client_id, client_secret, scope)
    api_ctx  = api.new_api_context(api_base, lambda: auth.get_auth_header(auth_ctx))

    xlsx_destination_path, data_aux_path, mec_sac_path = load_config()

    out_file_frmt = 'csv'

    with log_timing('compute', 'returns_mec_sac'):
        returns_mecsac = compute_aggregate(data_aux_path, mec_sac_path)

    with log_timing('compute', 'save_returns_mec_sac'):
        save_df(returns_mecsac, f"{xlsx_destination_path}divulga_rentab_agregados",
                out_file_frmt)

    reconcile_entities(api_ctx, returns_mecsac)

    with log_timing('compute', 'save_reconciled_ids'):
        save_df(returns_mecsac, f"{xlsx_destination_path}divulga_rentab_ids_comparados",
                out_file_frmt)

    returns_reconciled = reconcile_returns(api_ctx, returns_mecsac)

    with log_timing('compute', 'save_reconciled_returns'):
        save_df(returns_reconciled, f"{xlsx_destination_path}divulga_rentab_rentab_comparadas",
                out_file_frmt)


if __name__ == "__main__":
    with log_timing('full', 'all_process'):
        main()
