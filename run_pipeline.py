#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 23 23:21:02 2025

@author: andrefelix
"""


import sys
import subprocess


scripts = [
    "parse_xml_anbima.py",
    "clean_and_prepare_raw_data.py",
    "enrich_and_classify_data.py",
    "compute_equiyt_stake.py",
    "investment_tree.py"
]


def show_menu():
    print("\nEscolha o ponto de partida do pipeline:\n")
    for i, script in enumerate(scripts):
        print(f"{i+1}. Iniciar a partir de: {script}")
    print("0. Sair")


def run_scripts(start_index):
    for script in scripts[start_index:]:
        print(f"\n>> Executando: {script}")
        try:
            subprocess.run(
                [sys.executable, script],
                check=True,
                stdout=None,     # Mostra o que o script printar
                stderr=None      # Mostra também os erros
            )
        except subprocess.CalledProcessError as e:
            print(f"Erro ao executar {script}. Código de saída: {e.returncode}")
            break


if __name__ == "__main__":
    while True:
        show_menu()
        try:
            choice = int(input("\nDigite o número da opção desejada: "))
            if choice == 0:
                print("Saindo...")
                break
            elif 1 <= choice <= len(scripts):
                run_scripts(choice - 1)
                break
            else:
                print("Opção inválida. Tente novamente.")
        except ValueError:
            print("Por favor, digite um número.")
