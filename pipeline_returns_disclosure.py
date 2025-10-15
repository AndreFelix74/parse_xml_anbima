#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 23 16:21:21 2025

@author: andrefelix
"""

from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
import os
import time
import pandas as pd

import auxiliary_loaders as aux_loader
import util as utl
from file_handler import save_df, load_df
from logger import log_timing
from data_io import auth_provider as auth, maestro_api as api
from returns_disclosure import (
    compute_aggregate_returns,
    reconcile_entities_ids,
    reconcile_monthly_returns,
    reconcile_annually_returns
    )


def show_menu():
    """
    Display the available menu options to the user.

    Prints a numbered list of actions related to reconciling and synchronizing
    entities and returns with Maestro. Options include reconciliation, synchronization,
    and exit from the program.
    """
    print('Escolha uma das opções abaixo:')
    print('1. Reconciliar entidades MEC-SAC com Maestro')
    print('2. Sincronizar entidades MEC-SAC com Maestro')
    print('3. Reconciliar rentabilidades MEC-SAC com Maestro')
    print('4. Sincronizar rentabilidades MEC-SAC com Maestro')
    print('0. Sair')


def print_overview():
    """Mostra visão geral de alto nível do script."""
    msg = (
        "\n=== O que este script faz ===\n"
        "Este script integra dados do MEC-SAC ao Maestro.\n"
        "Fluxo completo, em alto nível:\n"
        "  1) Reconciliar entidades: lê dados de dCadPlanoSAC e compara com o cadastro do Maestro.\n"
        "  2) Sincronizar entidades: cria no Maestro as entidades faltantes.\n"
        "  3) Reconciliar rentabilidades: agrega MEC-SAC, confere IDs, compara mensal e anual com o Maestro.\n"
        "  4) Sincronizar rentabilidades: envia rentabilidades para o fluxo de aprovação no Maestro.\n"
    )
    print(msg)


def ensure_fresh_output(file_path, started_ts):
    """
    Garante que o arquivo existe e foi gerado após started_ts.
    Retorna o caminho completo (com extensão) se válido.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    mtime = os.path.getmtime(file_path)
    return (mtime > started_ts)


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


def load_entities_ids(api_ctx):
    """
    Load entity identifiers from the Maestro API.

    Args:
        api_ctx (object): API context object containing authentication
                          and configuration for requests.

    Returns:
        dict: A dictionary mapping entity labels to their data,
              where each key corresponds to one of:
                - 'GRUPO': Groups of investments
                - 'INDEXADOR': Indexers
                - 'PLANO': Investment plans
                - 'TIPO_PLANO': Plan types
              and each value is the JSON response from the API.
    """
    result = {}

    api_entities_map = {
        'GRUPO': '/investimentos/Grupos',
        'INDEXADOR': '/investimentos/Indexadores',
        'PLANO': '/investimentos/Planos',
        'TIPO_PLANO': '/investimentos/TiposPlanos',
    }

    for label, endpoint in api_entities_map.items():
        api_resp = api.api_get(api_ctx, endpoint)
        result[label] = api_resp.json()

    return result


def load_returns_ids(api_ctx):
    result = {}

    api_entities_map = {
        'MENSAL': '/investimentos/Rentabilidades/mensais',
        'ANUAL': '/investimentos/Rentabilidades/anuais',
    }

    for label, endpoint in api_entities_map.items():
        api_resp = api.api_get(api_ctx, endpoint)
        result[label] = api_resp.json()
    
    return result


def save_entities(api_ctx, missing_maestro_entities_file, out_file_frmt):
    """
    Save missing entities into the Maestro API.

    Args:
        api_ctx (object): API context object containing authentication
                          and configuration for requests.
        missing_maestro_entities_file (str): Path to the file containing
                                             entities not yet present in Maestro.
        out_file_frmt (str): File format for reading the missing entities file
                             (e.g., 'xlsx', 'csv').

    Behavior:
        - Loads missing entities from the specified file.
        - Maps each entity type to its corresponding Maestro API endpoint.
        - For each entity, constructs the required payload:
            * For 'GRUPO' and 'INDEXADOR': only the entity name.
            * For 'NOME_PLANO': includes group ID, name, CNPB code,
              SAC code, plan code, indexer ID, and plan type ID.
        - Sends POST requests to the Maestro API to create the entities.
    """
    missing_entities_maestro = load_df(missing_maestro_entities_file,
                                       out_file_frmt)
    api_entities_map = {
        'GRUPO': '/investimentos/Grupos',
        'INDEXADOR': '/investimentos/Indexadores',
        'NOME_PLANO': '/investimentos/Planos',
        'TIPO_PLANO': '/investimentos/TiposPlanos',
    }

    for _, row in missing_entities_maestro.iterrows():
        label = row['TIPO']
        nome = row['NOME']

        endpoint = api_entities_map[label]

        payload = {'nome': nome}
        if label == 'NOME_PLANO':
            payload = {
                'grupoId': int(row['id_GRUPO']),
                'nome': nome,
                'codigoCNPB': str(row['CNPB']),
                'codigoSAC': str(row['CODCLI_SAC']),
                'codigoPlano': str(row['COD_PLANO']),
                'indexadorId': int(row['id_INDEXADOR']) if pd.notna(row['id_INDEXADOR']) else '',
                'tipoPlanoId': int(row['id_TIPO_PLANO']),
                }

        api.api_post(api_ctx, endpoint, json=payload)


def save_returns(api_ctx, missing_maestro_returns_file, out_file_frmt):
    missing_returns_maestro = load_df(missing_maestro_returns_file,
                                       out_file_frmt)
    api_returns_map = {
        'MENSAL': '/investimentos/Rentabilidades/mensais',
        'ANUAL': '/investimentos/Rentabilidades/anuais',
    }

    for _, row in missing_returns_maestro.iterrows():
        payload_base = {
            'planoId': int(row['api_id']),
            'ano': int(row['ANO']),
            }
        payload_mes = {
            **payload_base,
            'mes': int(row['MES']),
            'valor': float(row['RENTAB_MES']) * 100.0
            }
        payload_ano = {
            **payload_base,
            'valor': float(row['RENTAB_ANO']) * 100.0
            }

        api.api_post(api_ctx, api_returns_map['MENSAL'], json=payload_mes)
        api.api_post(api_ctx, api_returns_map['ANUAL'], json=payload_ano)


def reconcile_entities_ids_with_maestro(api_ctx, entities):
    """
    essa funcao estah ruim, faz uma alteracao inplace e retorna um objeto
    foi criada para evitar repeticao de codigo na reconciliacao das rentabilidades
    """
    with log_timing('reconcile_entities', 'load_maestro'):
        api_data = load_entities_ids(api_ctx)

    with log_timing('reconcile_entities', 'reconcile_ids'):
        for label, json_content in api_data.items():
            reconcile_entities_ids(entities, label, json_content)

    return api_data


def reconcile_entities_dcadplanosac_maestro(data_aux_path, api_ctx):
    groups = ['TIPO_PLANO', 'GRUPO', 'INDEXADOR', 'NOME_PLANO']

    with log_timing('reconcile_entities', 'load_dcadplanosac'):
        dcadplanosac = aux_loader.load_dcadplanosac(data_aux_path)
        df_melt = dcadplanosac[groups].melt(var_name='TIPO', value_name='NOME')
        entities = df_melt.dropna().drop_duplicates().reset_index(drop=True)

    api_data = reconcile_entities_ids_with_maestro(api_ctx, entities)

    with log_timing('reconcile_entities', 'find_missing_entities'):
        mask = entities['api_id'].isna() & (entities['TIPO'] == 'NOME_PLANO')
        missing_entities = entities[mask].copy()
        #As duas linhas seguintes sao gambiarra para colocar sufixo nos merges nao sao usadas
        missing_entities['id'] = None
        missing_entities['nome'] = None
        missing_entities = missing_entities.merge(
            dcadplanosac,
            left_on=['NOME'],
            right_on=['NOME_PLANO'],
            how='left'
        )
        for group in groups:
            if group == 'NOME_PLANO':
                continue
            entities_maestro = pd.DataFrame(api_data[group])
            entities_maestro['nome'] = entities_maestro['nome'].str.upper()
            missing_entities[group] = missing_entities[group].str.upper()
            missing_entities = missing_entities.merge(
                entities_maestro,
                left_on=[group],
                right_on=['nome'],
                how='left',
                suffixes=('', '_' + group)
            )

    return [missing_entities, api_data, entities]


def reconcile_returns_mecsac_maestro(out_file_frmt, xlsx_destination_path,
                                     data_aux_path, mec_sac_path, api_ctx):
    processes = min(8, multiprocessing.cpu_count())

    with log_timing('load', 'find_mecsac_files') as log:
        all_mecsac_files = find_all_mecsac_files(mec_sac_path)

        log.info(
            'load',
            total=len(all_mecsac_files),
        )

    with log_timing('load', 'load_mecsac_content') as log:
        with ProcessPoolExecutor(max_workers=processes) as executor:
            dfs = list(executor.map(aux_loader.load_mecsac_file,
                                    all_mecsac_files))

        mec_sac = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    with log_timing('compute', 'load_dcadplanosac'):
        dcadplanosac = aux_loader.load_dcadplanosac(data_aux_path)

    with log_timing('compute', 'aggregate_returns_mec_sac'):
        returns_mecsac = compute_aggregate_returns(mec_sac, dcadplanosac)

    with log_timing('compute', 'save_returns_mec_sac'):
        save_df(returns_mecsac, f"{xlsx_destination_path}divulga_rentab_agregados",
                out_file_frmt)

    api_data = reconcile_entities_ids_with_maestro(api_ctx, returns_mecsac)

    #na nova versao do site soh ha divulgacao por plano, nao tem agregados
    #remover essa linha caso haja divulgacao de rentabilidades por agregados
    returns_mecsac = returns_mecsac[returns_mecsac['TIPO'] == 'PLANO']
    mask = returns_mecsac['api_id'].isna()
    if mask.any():
        missing_count = returns_mecsac[mask]['NOME'].drop_duplicates().count()
        file_name = f"{xlsx_destination_path}divulga_rentab_rentab_ERROR_missing_ids"
        save_df(returns_mecsac[mask], file_name, out_file_frmt)
        print(
            "Não é possível reconciliar as rentabilidades.\n"
            f"Existem {missing_count} entidades em dCadPlanoSAC sem cadastro no Maestro.\n"
            f"Verifique o arquivo {file_name}.{out_file_frmt}\n"
            "Para corrigir, execute a etapa de sincronizar entidades com Maestro."
        )
        return [None, None, None]

    api_data = load_returns_ids(api_ctx)
    returns_reconciled = reconcile_monthly_returns(returns_mecsac, pd.DataFrame(api_data['MENSAL']))
    returns_reconciled = reconcile_annually_returns(returns_reconciled, pd.DataFrame(api_data['ANUAL']))

    mask = returns_reconciled['id_mensal'].isna()

    return [returns_reconciled[mask], api_data, returns_reconciled]


def save_reconcile_entities_result(missing_entities, entities, api_data,
                                   xlsx_destination_path, out_file_frmt,
                                   missing_maestro_entities_file):
    with log_timing('reconcile_entities', 'save_files'):
        save_df(missing_entities, missing_maestro_entities_file, out_file_frmt)
        save_df(entities, f"{xlsx_destination_path}divulga_rentab_ids_comparados",
                out_file_frmt)
        with open(f"{xlsx_destination_path}divulga_rentab_ids_maestro.json", 'w',
                  encoding='utf-8') as file:
            json.dump(api_data, file, ensure_ascii=False, indent=2)


def save_reconcile_returns_result(missing_returns, entities, api_data,
                                   xlsx_destination_path, out_file_frmt,
                                   missing_maestro_returns_file):
    with log_timing('reconcile_returns', 'save_files'):
        save_df(missing_returns, missing_maestro_returns_file, out_file_frmt)
        save_df(entities, f"{xlsx_destination_path}divulga_rentab_rentab_comparadas",
                out_file_frmt)
        with open(f"{xlsx_destination_path}divulga_rentab_rentab_maestro.json", 'w',
                  encoding='utf-8') as file:
            json.dump(api_data, file, ensure_ascii=False, indent=2)


def main(script_start_ts, option):
    """
    Main entry point for executing reconciliation and synchronization tasks
    between MEC-SAC and Maestro systems.

    Args:
        option (str): The selected menu option

    Behavior:
        - Initializes authentication and API contexts using environment variables:
            TENANT_ID, CLIENT_ID, CLIENT_SECRET, SCOPE, API_BASE.
        - Loads configuration paths for data and outputs.
        - Executes the corresponding task based on the provided option
    """
    tenant_id = os.environ['TENANT_ID']
    client_id = os.environ['CLIENT_ID']
    client_secret = os.environ['CLIENT_SECRET']
    scope = os.environ['SCOPE']
    api_base = os.environ['API_BASE']

    auth_ctx = auth.new_auth_context(tenant_id, client_id, client_secret, scope)
    api_ctx = api.new_api_context(api_base, lambda: auth.get_auth_header(auth_ctx))

    xlsx_destination_path, data_aux_path, mec_sac_path = load_config()

    out_file_frmt = 'csv'
    missing_maestro_entities_file = f"{xlsx_destination_path}divulga_rentab_entidades_a_sincronizar"
    missing_maestro_returns_file = f"{xlsx_destination_path}divulga_rentab_rentab_a_sincronizar"

    if option == '1':
        print('Reconciliando entidades com Maestro...')
        missing_entities, api_data, entities = (
            reconcile_entities_dcadplanosac_maestro(data_aux_path, api_ctx)
            )
        save_reconcile_entities_result(missing_entities, entities, api_data,
                                       xlsx_destination_path, out_file_frmt,
                                       missing_maestro_entities_file)
        if len(missing_entities) == 0:
            print('Não há entidades para sincronizar.')
            print('Execute o passo 3 para reconciliação de rentabilidades.')
        else:
            print(f"Encontradas {len(missing_entities)} entidades para sincronizar com Maestro")
            print(f"Valide o arquivo {missing_maestro_entities_file}.{out_file_frmt}")
            print('Se estiver correto, execute o passo 2 para envio das entidades para o Maestro')
    elif option == '2':
        print('Salvando entidades no Maestro...')
        file_to_sync = f"{missing_maestro_entities_file}.{out_file_frmt}"
        if (ensure_fresh_output(file_to_sync, script_start_ts)):
            save_entities(api_ctx, missing_maestro_entities_file, out_file_frmt)
        else:
            print(
                f"Arquivo {file_to_sync} gerado antes desta execução.\n"
                "Refaça a etapa de reconciliação de entidades e execute o salvamento outra vez."
                )
    elif option == '3':
        print('Reconciliando rentabilidades com Maestro...')
        missing_returns, api_data, return_mecsac = (
            reconcile_returns_mecsac_maestro(out_file_frmt, xlsx_destination_path,
                                             data_aux_path, mec_sac_path, api_ctx)
            )
        if (missing_returns is None and api_data is None and return_mecsac):
            return

        save_reconcile_returns_result(missing_returns, return_mecsac, api_data,
                                      xlsx_destination_path, out_file_frmt,
                                      missing_maestro_returns_file)
        if len(missing_returns) == 0:
            print('Não há rentabilidades para sincronizar.')
            print('Nada a fazer.')
        else:
            print(f"Encontradas {len(missing_returns)} rentabilidades para sincronizar com Maestro")
            print(f"Valide o arquivo {missing_maestro_returns_file}.{out_file_frmt}")
            print('Se estiver correto, execute o passo 4 para envio das rentabilidades para o Maestro')
    elif option == '4':
        print('Salvando rentabilidades no Maestro...')
        file_to_sync = f"{missing_maestro_returns_file}.{out_file_frmt}"
        if (ensure_fresh_output(file_to_sync, script_start_ts)):
            save_returns(api_ctx, missing_maestro_returns_file, out_file_frmt)
        else:
            print(
                f"Arquivo {file_to_sync} gerado antes desta execução.\n"
                "Refaça a etapa de reconciliação de rentabilidades e execute o salvamento outra vez."
                )
    elif option == '0':
        print('Saindo...')
        exit()
    else:
        print('Opção inválida. Escolha uma das opções do menu.')


if __name__ == "__main__":
    script_start_ts = time.time()
    print_overview()
    while True:
        show_menu()
        usr_option = input('Digite o número da opção desejada: ')
        main(script_start_ts, usr_option)
        print('')
