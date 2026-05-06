#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para listar ou apagar índices do OpenSearch com base em um prefixo.
As credenciais e configurações de infraestrutura são carregadas do arquivo .env.

Uso:
    python delete_indices.py [opções]

Opções:
  --opensearch-host STR Host do OpenSearch (opcional, sobrepõe as configurações do .env)
  --prefix STR          Prefixo para filtrar índices (padrão: kemp)
  --log-file STR        Caminho para o arquivo de log CSV (padrão: delete_indices_log_AAAAMMDD_HHMMSS.csv)
  --delete              Apaga os índices encontrados (por padrão, apenas lista)
  --help                Exibe instruções de uso e sai

Exemplo:
    python delete_indices.py --prefix kemp  # Lista índices que começam com 'kemp'
    python delete_indices.py --prefix kemp --delete  # Apaga índices que começam com 'kemp'
    python delete_indices.py --prefix kemp__51  # Lista índices que começam com 'kemp__51'
    python delete_indices.py --prefix "*"  # Lista todos os índices
"""

import argparse
import csv
import datetime
import sys
import time
import os
from pathlib import Path

from dotenv import load_dotenv
from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError

# Carrega as variáveis do arquivo .env local
load_dotenv()

# =======================================
# CONFIGURAÇÕES DE AMBIENTE (.env)
# =======================================
_opensearch_hosts = [h.strip() for h in os.getenv('OPENSEARCH_HOSTS', '127.0.0.1').split(',') if h.strip()]
_opensearch_user  = os.getenv('OPENSEARCH_USER', 'admin')
_opensearch_pass  = os.getenv('OPENSEARCH_PASS', 'admin')
_opensearch_ca    = os.getenv('OPENSEARCH_CA_CERT')
_opensearch_cert  = os.getenv('OPENSEARCH_CLIENT_CERT')
_opensearch_key   = os.getenv('OPENSEARCH_CLIENT_KEY')

# =======================================
# CONFIGURAÇÃO GLOBAL (CLI)
# =======================================
def parse_arguments() -> argparse.Namespace:
    """Parseia argumentos da linha de comando para configurar o script."""
    parser = argparse.ArgumentParser(description="Lista ou apaga índices do OpenSearch com base em um prefixo.", add_help=False)
    parser.add_argument("--opensearch-host", default=None, help="Host do OpenSearch (sobrepõe o .env)")
    parser.add_argument("--prefix", default="kemp", help="Prefixo para filtrar índices")
    parser.add_argument("--log-file", default=None, help="Caminho para o arquivo de log (padrão: gerado com timestamp)")
    parser.add_argument("--delete", action="store_true", help="Apaga os índices encontrados (por padrão, apenas lista)")
    parser.add_argument("--help", action="store_true", help="Exibe instruções de uso e sai")
    args = parser.parse_args()
    if args.help:
        print(__doc__)
        sys.exit(0)
    return args

# Inicializa configurações globais
args = parse_arguments()
cli_host_override: str = args.opensearch_host
prefix: str = args.prefix
delete_log_file: str = args.log_file or f"delete_indices_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
delete_indices_flag: bool = args.delete

# =======================================
# CLIENTE OPENSEARCH
# =======================================
def initialize_opensearch_client(host_override: str = None) -> OpenSearch:
    """Inicializa o cliente OpenSearch com os parâmetros do .env ou CLI."""
    # Se o host foi passado por CLI, usa ele. Senão, usa a lista de hosts do .env
    hosts = [{'host': host_override, 'port': 9200}] if host_override else [{'host': h, 'port': 9200} for h in _opensearch_hosts]

    return OpenSearch(
        hosts=hosts,
        http_auth=(_opensearch_user, _opensearch_pass),
        use_ssl=True,
        verify_certs=True,
        ca_certs=_opensearch_ca,
        client_cert=_opensearch_cert,
        client_key=_opensearch_key,
        timeout=30,
        retry_on_timeout=True
    )

# =======================================
# FUNÇÕES DE LOG
# =======================================
def write_delete_log(
    log_file: str,
    index_name: str,
    status: str,
    details: str = None,
    delete_time: float = None
) -> None:
    """Escreve detalhes da exclusão em um arquivo de log CSV."""
    with open(log_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if Path(log_file).stat().st_size == 0:
            writer.writerow(['timestamp', 'index_name', 'status', 'details', 'delete_time_sec'])
        writer.writerow([datetime.datetime.now().isoformat(), index_name, status, details, delete_time])

# =======================================
# FUNÇÕES DE LISTAGEM E EXCLUSÃO
# =======================================
def list_opensearch_indices(client: OpenSearch, prefix: str) -> list[str]:
    """Lista índices no OpenSearch que começam com o prefixo fornecido."""
    effective_prefix = prefix if prefix != "*" else "*"
    print(f"Listando índices no OpenSearch com prefixo '{effective_prefix}*'")
    try:
        indices = client.indices.get(index=f"{effective_prefix}*").keys()
        indices = sorted(indices)
        if not indices:
            print(f"Nenhum índice encontrado com o prefixo '{effective_prefix}*'")
        else:
            print("\nÍndices encontrados:")
            for i, index in enumerate(indices, 1):
                print(f"{i}. {index}")
        return indices
    except RequestError as e:
        if e.status_code == 404:
            print(f"Nenhum índice encontrado com o prefixo '{effective_prefix}*'")
            return []
        raise
    except Exception as e:
        print(f"Erro ao listar índices com prefixo '{effective_prefix}*': {e}")
        return []

def delete_indices(client: OpenSearch, indices: list[str]) -> None:
    """Apaga os índices fornecidos do OpenSearch e registra as ações no log."""
    if not indices:
        print("Nenhum índice para apagar.")
        return

    for index in indices:
        print(f"Apagando índice '{index}'...")
        try:
            start_time = time.time()
            client.indices.delete(index=index)
            delete_time = time.time() - start_time
            print(f"✅ Índice '{index}' apagado com sucesso.")
            write_delete_log(delete_log_file, index, 'SUCESSO', delete_time=delete_time)
        except RequestError as e:
            print(f"❌ Erro ao apagar o índice '{index}': {e}")
            write_delete_log(delete_log_file, index, 'ERRO', str(e))
        except Exception as e:
            print(f"❌ Erro inesperado ao apagar o índice '{index}': {e}")
            write_delete_log(delete_log_file, index, 'ERRO', str(e))

# =======================================
# EXECUÇÃO PRINCIPAL
# =======================================
if __name__ == "__main__":
    client = initialize_opensearch_client(cli_host_override)
    
    try:
        info = client.info()
        print(f"Conectado ao cluster OpenSearch: {info.get('cluster_name')} (Versão: {info.get('version', {}).get('number')})")
    except Exception as e:
        print(f"❌ Erro ao conectar ao OpenSearch: {e}")
        sys.exit(1)
        
    indices = list_opensearch_indices(client, prefix)
    
    if delete_indices_flag:
        delete_indices(client, indices)