#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 23 16:21:21 2025

@author: andrefelix
"""


import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import json
import multiprocessing
import os
import pandas as pd

import auxiliary_loaders as aux_loader
import util as utl
from file_handler import save_df
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


def print_missing_env():
    print(
        "Erro: variáveis de ambiente não configuradas.\n"
        "É necessário definir as seguintes variáveis:\n"
        "- TENANT_ID\n"
        "- CLIENT_ID\n"
        "- CLIENT_SECRET\n"
        "- SCOPE\n"
        "- API_BASE"
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


def load_api_context():
    required_vars = [
        'TENANT_ID',
        'CLIENT_ID',
        'CLIENT_SECRET',
        'SCOPE',
        'API_BASE',
    ]

    if not any(var in os.environ for var in required_vars):
        print_missing_env()
        return None

    tenant_id = os.environ['TENANT_ID']
    client_id = os.environ['CLIENT_ID']
    client_secret = os.environ['CLIENT_SECRET']
    scope = os.environ['SCOPE']
    api_base = os.environ['API_BASE']

    auth_ctx = auth.new_auth_context(tenant_id, client_id, client_secret, scope)
    api_ctx = api.new_api_context(api_base, lambda: auth.get_auth_header(auth_ctx))

    print(f"API configurada para: {api_base}\n")

    return api_ctx


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


def get_maestro_entity_spec():
    return {
        'GRUPO': {
            'order': 0,
            'endpoint': '/investimentos/Grupos',
            'identity_map': {},
            'payload': lambda row, resolved_fk_ids: {'nome': row['NOME']},
        },
        'INDEXADOR': {
            'order': 1,
            'endpoint': '/investimentos/Indexadores',
            'identity_map': {},
            'payload': lambda row, resolved_fk_ids: {'nome': row['NOME']},
        },
        'TIPO_PLANO': {
            'order': 2,
            'endpoint': '/investimentos/TiposPlanos',
            'identity_map': {},
            'payload': lambda row, resolved_fk_ids: {'nome': row['NOME']},
        },
        'PLANO': {
            'order': 3,
            'endpoint': '/investimentos/Planos',
            'identity_map': None,
            'foreign_keys': [
                ('GRUPO',      'id_GRUPO',      'nome_GRUPO'),
                ('INDEXADOR',  'id_INDEXADOR',  'nome_INDEXADOR'),
                ('TIPO_PLANO', 'id_TIPO_PLANO', 'nome_TIPO_PLANO'),
            ],
            'payload': lambda row, resolved_fk_ids: {
                'grupoId': resolved_fk_ids['GRUPO'],
                'nome': row['NOME'],
                'codigoCNPB': str(row['CNPB']),
                'codigoSAC': str(row['CODCLI_SAC']),
                'codigoPlano': str(row['COD_PLANO']),
                'indexadorId': resolved_fk_ids['INDEXADOR'],
                'tipoPlanoId': resolved_fk_ids['TIPO_PLANO'],
            }
        }
    }


def save_entities(api_ctx, missing_entities_maestro):
    """
    Save missing entities into the Maestro API.

    Args:
        api_ctx (object): API context object containing authentication
                          and configuration for requests.
        missing_entities_maestro (dataframe): entities not yet present in Maestro.

    Behavior:
        - Maps each entity type to its corresponding Maestro API endpoint.
        - For each entity, constructs the required payload:
            * For 'GRUPO' and 'INDEXADOR': only the entity name.
            * For 'PLANO': includes group ID, name, CNPB code,
              SAC code, plan code, indexer ID, and plan type ID.
        - Sends POST requests to the Maestro API to create the entities.
    """
    if missing_entities_maestro is None:
        print('\nEntidades não reconciliadas com Maestro.')
        print('Execute a etapa 1 antes de sincronizar as entidades com Maestro.\n')
        return False
    elif len(missing_entities_maestro) == 0:
        print('\nNão há entidades para reconciliadar. Nada a enviar.\n')
        return True

    def extract_id(resp: dict) -> int:
        if isinstance(resp, dict) and 'id' in resp:
            return int(resp['id'])

    entity_spec = get_maestro_entity_spec()

    missing_entities_maestro['_ord'] = missing_entities_maestro['TIPO'].map(lambda t: entity_spec[t]['order'])
    missing_entities_maestro = missing_entities_maestro.sort_values(['_ord', 'NOME'])

    for _, row in missing_entities_maestro.iterrows():
        cfg = entity_spec[row['TIPO']]

        endpoint = cfg['endpoint']
        resolved_fk_ids = {}

        for fk_entity, fk_id_col, fk_alt_key_col in cfg.get('foreign_keys', []):
            fk_id_val = row.get(fk_id_col)

            if pd.notna(fk_id_val):
                resolved_fk_ids[fk_entity] = int(fk_id_val)
                continue

            fk_alt_key = row.get(fk_alt_key_col)

            if pd.isna(fk_alt_key) or str(fk_alt_key).strip() == "":
                raise ValueError(
                    f"FK não resolvida para {row['TIPO']}='{row.get('NOME')}'. "
                    f"Campos vazios: {fk_id_col}=NaN e {fk_alt_key_col}=NaN/'' "
                    f"(fk_entity={fk_entity})."
                )

            fk_alt_key = str(fk_alt_key).strip()

            fk_map = entity_spec[fk_entity]['identity_map'] or {}
            fk_id = fk_map.get(fk_alt_key)

            if fk_id is None:
                raise KeyError(
                    f"FK não encontrada no identity_map: fk_entity={fk_entity}, chave='{fk_alt_key}'. "
                    f"Item atual: {row['TIPO']}='{row.get('NOME')}'. "
                    f"Chaves disponíveis (amostra): {list(fk_map.keys())[:10]}"
                )

            resolved_fk_ids[fk_entity] = int(fk_id)

        payload = cfg['payload'](row, resolved_fk_ids)
        resp = api.api_post(api_ctx, endpoint, json=payload)

        if cfg['identity_map'] is not None:
            cfg['identity_map'][row['NOME']] = extract_id(resp)

    print('\nEntidades sincronizadas com Maestro com sucesso.\n')
    return True


def save_returns(api_ctx, missing_returns_maestro):
    if not missing_returns_maestro:
        print('\nRentabilidades não reconciliadas com Maestro.')
        print('Execute a etapa 3 antes de sincronizar as rentabilidades com Maestro.\n')
        return

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

    print('\nRentabilidades sincronizadas com Maestro com sucesso.\n')


def reconcile_entities_ids_with_maestro(api_ctx, entities):
    """
    essa funcao estah ruim, faz uma alteracao inplace e retorna um objeto
    foi criada para evitar repeticao de codigo usado nas funcoes:
      reconcile_entities_dcadplanosac_maestro(data_aux_path, api_ctx)
      e
      reconcile_returns_mecsac_maestro(out_file_frmt, run_folder,
                                       data_aux_path, mec_sac_path, api_ctx):
    """
    api_data = load_entities_ids(api_ctx)

    for label, json_content in api_data.items():
        reconcile_entities_ids(entities, label, json_content)

    return api_data


def reconcile_entities_dcadplanosac_maestro(data_aux_path, api_ctx):
    groups = ['TIPO_PLANO', 'GRUPO', 'INDEXADOR', 'PLANO']

    dcadplanosac = aux_loader.load_dcadplanosac(data_aux_path)
    #renomeia a coluna para compatibilizar com os quatro grupos
    dcadplanosac.rename(columns={'NOME_PLANO': 'PLANO'}, inplace=True)
    df_melt = dcadplanosac[groups].melt(var_name='TIPO', value_name='NOME')
    entities = df_melt.dropna().drop_duplicates().reset_index(drop=True)

    api_data = reconcile_entities_ids_with_maestro(api_ctx, entities)

    mask = entities['api_id'].isna()
    missing_entities = entities[mask].copy()
    #As duas linhas seguintes sao gambiarra para colocar sufixo nos merges. nao sao usadas
    missing_entities['id'] = None
    missing_entities['nome'] = None
    missing_entities = missing_entities.merge(
        dcadplanosac,
        left_on=['NOME'],
        right_on=['PLANO'],
        how='left'
    )
    for group in groups:
        if group == 'PLANO':
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


def reconcile_entities(data_aux_path, api_ctx, run_folder, out_file_frmt):
    print('Reconciliando entidades com Maestro...')
    missing_entities, api_data, entities = (
        reconcile_entities_dcadplanosac_maestro(data_aux_path, api_ctx)
        )

    missing_maestro_entities_file = (
        run_folder / "divulga_rentab_entidades_a_sincronizar"
    )

    save_df(missing_entities, missing_maestro_entities_file, out_file_frmt)
    save_df(entities, run_folder / "divulga_rentab_ids_comparados", out_file_frmt)
    with open(run_folder  / "divulga_rentab_ids_maestro.json", 'w', encoding='utf-8') as file:
        json.dump(api_data, file, ensure_ascii=False, indent=2)

    if len(missing_entities) == 0:
        print('Não há entidades para sincronizar.')
        print('Execute o passo 3 para reconciliação de rentabilidades.')
    else:
        print(f"Encontradas {len(missing_entities)} entidades para sincronizar com Maestro")
        print(f"Valide o arquivo {missing_maestro_entities_file}.{out_file_frmt}")
        print('Se estiver correto, execute o passo 2 para envio das entidades para o Maestro')

    return missing_entities


def reconcile_returns_mecsac_maestro(out_file_frmt, run_folder,
                                     data_aux_path, mec_sac_path, api_ctx):
    processes = min(8, multiprocessing.cpu_count())

    all_mecsac_files = find_all_mecsac_files(mec_sac_path)

    with ProcessPoolExecutor(max_workers=processes) as executor:
        dfs = list(executor.map(aux_loader.load_mecsac_file,
                                all_mecsac_files))

    mec_sac = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    dcadplanosac = aux_loader.load_dcadplanosac(data_aux_path)

    returns_mecsac = compute_aggregate_returns(mec_sac, dcadplanosac)

    save_df(returns_mecsac, run_folder / "divulga_rentab_agregados",
            out_file_frmt)

    api_data = reconcile_entities_ids_with_maestro(api_ctx, returns_mecsac)

    #na nova versao do site soh ha divulgacao por plano, nao tem agregados
    #remover essa linha caso haja divulgacao de rentabilidades por agregados
    returns_mecsac = returns_mecsac[returns_mecsac['TIPO'] == 'PLANO']
    mask = returns_mecsac['api_id'].isna()
    if mask.any():
        missing_count = returns_mecsac[mask]['NOME'].drop_duplicates().count()
        file_name = run_folder / "divulga_rentab_rentab_ERROR_missing_ids"
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


def reconcile_returns(out_file_frmt, run_folder, data_aux_path,
                      mec_sac_path, api_ctx):
    print('Reconciliando rentabilidades com Maestro...')
    missing_returns, api_data, return_mecsac = (
        reconcile_returns_mecsac_maestro(out_file_frmt, run_folder,
                                        data_aux_path, mec_sac_path, api_ctx)
        )

    if (missing_returns is None and api_data is None and return_mecsac is None):
        return

    missing_maestro_returns_file = run_folder / "divulga_rentab_rentab_a_sincronizar"

    save_df(missing_returns, missing_maestro_returns_file, out_file_frmt)
    save_df(return_mecsac, run_folder / "divulga_rentab_rentab_comparadas", out_file_frmt)
    with open(run_folder / "divulga_rentab_rentab_maestro.json", 'w', encoding='utf-8') as file:
        json.dump(api_data, file, ensure_ascii=False, indent=2)

    if len(missing_returns) == 0:
        print('Não há rentabilidades para sincronizar.')
        print('Nada a fazer.')
    else:
        print(f"Encontradas {len(missing_returns)} rentabilidades para sincronizar com Maestro")
        print(f"Valide o arquivo {missing_maestro_returns_file}.{out_file_frmt}")
        print('Se estiver correto, execute o passo 4 para envio das rentabilidades para o Maestro')


def main():
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
    print_overview()

    api_ctx = load_api_context()

    if api_ctx is None:
        return

    xlsx_destination_path, data_aux_path, mec_sac_path = load_config()

    run_id = str(uuid.uuid4())
    run_folder = Path(xlsx_destination_path) / run_id
    run_folder.mkdir(parents=True, exist_ok=True)

    out_file_frmt = 'csv'

    missing_entities_maestro = None
    entities_sent_maestro = False
    missing_returns_maestro = None

    while True:
        show_menu()
        usr_option = input('Digite o número da opção desejada: ')

        if usr_option == '1':
            missing_entities_maestro = (
                reconcile_entities(data_aux_path, api_ctx, run_folder, out_file_frmt)
            )
        elif usr_option == '2':
            entities_sent_maestro = save_entities(api_ctx, missing_entities_maestro)
        elif usr_option == '3':
            if missing_entities_maestro is not None and not entities_sent_maestro:
                print('\nEntidades não enviadas para Maestro.')
                print('Execute as etapas 1 e 2 antes de reconciliar as rentabilidades.\n')
                continue
            missing_returns_maestro = (
                reconcile_returns(out_file_frmt, run_folder, data_aux_path,
                                  mec_sac_path, api_ctx)
            )
        elif usr_option == '4':
            save_returns(api_ctx, missing_returns_maestro)
        elif usr_option == '0':
            print('Saindo...')
            exit()
        else:
            print('Opção inválida. Escolha uma das opções do menu.')


# if __name__ == "__main__":
#     main()
