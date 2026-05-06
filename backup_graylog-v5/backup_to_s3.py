#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para realizar backup de índices do OpenSearch e upload para o Amazon S3.
O script lista todos os índices, exporta os dados para arquivos JSONL, compacta em GZ usando pigz,
calcula o hash MD5 e faz upload paralelo para o S3, registrando o processo em um log CSV.
"""

from opensearchpy import OpenSearch
import os
import sys
import json
import boto3
import botocore
from botocore.exceptions import ClientError
from botocore.config import Config
import hashlib
import csv
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import shutil
import re
import subprocess
import base64
import time
import threading
import platform
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAÇÕES DE FILTRO E CAMINHO ---

BASE_FOLDER = os.getenv('S3_BASE_FOLDER', 'backup_legado')

TARGET_PREFIXES = [
    "app_main__",
    "windows__",
]

bucket_name = os.getenv('S3_BUCKET_NAME', 'backup-bucket-default')

year_prefix = datetime.datetime.now().strftime('%Y')

backup_log_file = f"/staging/graylog_backup_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# --- CLIENTE 1: FONTE (Leitura para Backup) ---
client = OpenSearch(
    hosts=[
        {'host': os.getenv('OS_SOURCE_HOST', 'localhost'), 'port': int(os.getenv('OS_SOURCE_PORT', 9200))},    
    ],
    http_auth=(os.getenv('OS_USER', 'admin'), os.getenv('OS_PASSWORD', 'admin')),
)

# --- CLIENTE 2: AUDITORIA (Escrita de Logs) ---
audit_client = OpenSearch(
    hosts=[
        {'host': os.getenv('OS_AUDIT_HOST_1', 'localhost'), 'port': int(os.getenv('OS_AUDIT_PORT', 9200))},
        {'host': os.getenv('OS_AUDIT_HOST_2', 'localhost'), 'port': int(os.getenv('OS_AUDIT_PORT', 9200))},    
    ],
    http_auth=(os.getenv('OS_USER', 'admin'), os.getenv('OS_PASSWORD', 'admin')),
    use_ssl=True,
    verify_certs=True,
    ca_certs=os.getenv('OS_CA_CERT', '/cert/ca-certificate'),
    client_cert=os.getenv('OS_CLIENT_CERT', '/cert/client-cert.crt'),
    client_key=os.getenv('OS_CLIENT_KEY', '/cert/client-cert.key'),
    timeout=30,
    max_retries=3,
    retry_on_timeout=True
)

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
    endpoint_url=os.getenv('S3_ENDPOINT'),
    region_name=os.getenv('S3_REGION', 'us-east-1'),
    config=Config(
        signature_version='s3v4',
        s3={'addressing_style': 'path', 'payload_signing_enabled': True},
        retries={'max_attempts': 5, 'mode': 'standard'},
        connect_timeout=60,
        read_timeout=60
    )
)

class GraylogAuditLogger:
    """Envía logs de auditoria ao índice backup_graylog__* que realmente aceita escrita."""
    def __init__(self, client, stream_id):
        self.client = client
        self.stream_id = stream_id
        self._active_index = None
        self._cache_ts = None
        self._ttl = 300 

    def _is_write_allowed(self, idx):
        """Verifica se o índice tem algum block de escrita."""
        try:
            s = self.client.indices.get_settings(index=idx)
            blocks = s[idx].get('settings', {}).get('index', {}).get('blocks', {})
            if blocks.get('write') == 'true':
                return False
            if blocks.get('read_only') == 'true':
                return False
            if s[idx].get('settings', {}).get('index', {}).get('read_only_allow_delete') == 'true':
                return False
        except Exception:
            pass
        return True

    def _test_write(self, idx):
        """Tenta indexar um documento de teste e o remove."""
        test_doc = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc)
                        .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "test": True,
            "message": "TESTE ESCRITA – IGNORE",
            "streams": [self.stream_id]
        }
        try:
            resp = self.client.index(index=idx, body=test_doc, refresh="wait_for")
            if resp["result"] in ("created", "updated"):
                try:
                    self.client.delete(index=idx, id=resp["_id"])
                finally:
                    return True
        except Exception as e:
            if "cluster_block_exception" in str(e) and "FORBIDDEN/8/index write" in str(e):
                return False
            sys.stdout.write(f"\033[K[WARNING] Erro ao testar escrita em {idx}: {e}\n")
            sys.stdout.flush()
        return False

    def find_active_index(self):
        """Retorna o índice backup_graylog__* que aceita escrita (com cache)."""
        if (self._active_index and self._cache_ts and
                (time.time() - self._cache_ts) < self._ttl):
            return self._active_index

        try:
            resp = self.client.cat.indices(format="json")
        except Exception as e:
            sys.stdout.write(f"\033[K[ERROR] cat.indices falhou no Audit Client: {e}\n")
            sys.stdout.flush()
            return None

        candidates = [
            i["index"] for i in resp
            if i["index"].startswith("backup_graylog__") and i.get("status") == "open"
        ]

        if not candidates:
            sys.stdout.write("\033[K[ERROR] Nenhum backup_graylog__* encontrado no cluster de auditoria\n")
            sys.stdout.flush()
            return None

        candidates.sort(
            key=lambda x: int(re.search(r"__(\d+)", x).group(1))
            if re.search(r"__(\d+)", x) else 0,
            reverse=True
        )

        for idx in candidates:
            if not self._is_write_allowed(idx):
                sys.stdout.write(f"\033[K[INFO] {idx} tem block de escrita – ignorado\n")
                sys.stdout.flush()
                continue

            if self._test_write(idx):
                sys.stdout.write(f"\033[K[INFO] Índice de log ativo encontrado: {idx}\n")
                sys.stdout.flush()
                self._active_index = idx
                self._cache_ts = time.time()
                return idx

            sys.stdout.write(f"\033[K[INFO] {idx} rejeitou escrita – próximo candidato\n")
            sys.stdout.flush()

        sys.stdout.write("\033[K[ERROR] Nenhum backup_graylog__* aceita escrita no cluster de auditoria\n")
        sys.stdout.flush()
        return None

    def log_to_graylog(self, index_name, backup_file, s3_key, md5sum,
                       status, details, duration_minutes, index_size, message):
        active = self.find_active_index()
        if not active:
            sys.stdout.write("\033[K[WARNING] Log Graylog ignorado – índice ativo indisponível no cluster de auditoria\n")
            sys.stdout.flush()
            return

        prefix = "[BACKUP_LEGADO]"
        if not str(message).startswith(prefix):
            formatted_message = f"{prefix} {message}"
        else:
            formatted_message = message

        doc = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc)
                        .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "index_name": index_name,
            "backup_file": backup_file,
            "s3_key": s3_key,
            "md5sum": md5sum,
            "status": status,
            "details": details or "N/A",
            "duration_minutes": duration_minutes if duration_minutes is not None else "N/A",
            "index_size": index_size if index_size is not None else "N/A",
            "source": platform.node(),
            "streams": [self.stream_id],
            "message": formatted_message, 
            "origin_system": "backup_legado" 
        }

        try:
            self.client.index(index=active, body=doc)
            sys.stdout.write(f"\033[K[INFO] Log enviado → {active}\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stdout.write(f"\033[K[ERROR] Falha ao enviar log para {active}: {e}\n")
            sys.stdout.flush()

def get_index_creation_date(client, index_name):
    """Obtém a data de criação do índice a partir das configurações."""
    try:
        settings = client.indices.get_settings(index=index_name)
        creation_date_ms = settings[index_name]['settings']['index'].get('creation_date')
        if creation_date_ms:
            creation_date = datetime.datetime.fromtimestamp(int(creation_date_ms) / 1000)
            return creation_date.strftime('%Y-%m-%d')
        return None
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Erro ao obter data de criação do índice {index_name}: {e}\n")
        sys.stdout.flush()
        return None

def is_index_active(client, index_name):
    """Verifica se o índice é ativo para escrita e foi criado no dia atual."""
    try:
        settings = client.indices.get_settings(index=index_name)
        is_write_index = settings[index_name].get('settings', {}).get('index', {}).get('write', 'true') == 'true'
        if not is_write_index:
            return False
        creation_date = get_index_creation_date(client, index_name)
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        if creation_date == current_date:
            return True
        return False
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Erro ao verificar se o índice {index_name} é ativo: {e}\n")
        sys.stdout.flush()
        return False

def parse_size(size_str):
    """Converte tamanho com unidade (ex.: '10.5gb') para bytes."""
    units = {'b': 1, 'kb': 1024, 'mb': 1024**2, 'gb': 1024**3, 'tb': 1024**4}
    size_str = size_str.lower()
    match = re.match(r'^(\d*\.?\d+)([a-z]+)?$', size_str)
    if match:
        value, unit = float(match.group(1)), match.group(2) or 'b'
        return int(value * units.get(unit, 1))
    try:
        return int(size_str)
    except ValueError:
        return 0

def normalize_index_name(index_name):
    """Extrai a parte base do nome do índice, removendo sufixos numéricos após __."""
    match = re.match(r'^(.*?)(?:__\d+)?$', index_name)
    if match:
        return match.group(1)
    return index_name

def get_all_indexes(client):
    """Lista todos os índices disponíveis no cluster OpenSearch com informações adicionais."""
    try:
        response = client.cat.indices(format='json')
        indexes = []
        for index in response:
            index_name = index['index']
            try:
                settings = client.indices.get_settings(index=index_name)
                replicas = int(settings[index_name]['settings']['index'].get('number_of_replicas', '0'))
            except Exception as e:
                sys.stdout.write(f"\033[K[ERROR] Erro ao obter settings para {index_name}: {e}\n")
                sys.stdout.flush()
                replicas = 0
            size = parse_size(index.get('store.size', '0'))
            primary_size = size // (replicas + 1) if replicas >= 0 else size
            indexes.append({
                'name': index_name,
                'docs_count': index.get('docs.count', '0'),
                'size': primary_size
            })
        return indexes
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Falha ao listar índices do OpenSearch: {e}\n")
        sys.stdout.flush()
        raise

def calculate_md5(file_path):
    """Calcula o hash MD5 de um arquivo para verificação de integridade."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def calculate_sha256(data):
    """Calcula o hash SHA256 de um bloco de dados."""
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)
    return sha256_hash.hexdigest()

def write_backup_log(log_file, index_name, backup_file, s3_key, md5sum, status, details=None, duration_minutes=None, index_size=None, log_lock=None, graylog_logger=None):
    """Registra informações do backup em um arquivo CSV e envia para o Graylog."""
    message = f"Backup do índice {index_name}: {status.lower()} ({details or 'N/A'})"
    if log_lock:
        with log_lock:
            _write_csv_row(log_file, index_name, backup_file, s3_key, md5sum, status, details, duration_minutes, index_size)
    else:
        _write_csv_row(log_file, index_name, backup_file, s3_key, md5sum, status, details, duration_minutes, index_size)
    
    if graylog_logger:
        graylog_logger.log_to_graylog(index_name, backup_file, s3_key, md5sum, status, details, duration_minutes, index_size, message)

def _write_csv_row(log_file, index_name, backup_file, s3_key, md5sum, status, details, duration_minutes, index_size):
    """Escreve uma linha no CSV com campo 'source' (hostname da máquina)."""
    source = platform.node()
    with open(log_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if os.stat(log_file).st_size == 0:
            writer.writerow(['timestamp', 'index_name', 'backup_file', 's3_key', 'md5sum', 'status', 'details', 'duration_minutes', 'index_size', 'source'])
        writer.writerow([
            datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            index_name,
            backup_file,
            s3_key,
            md5sum,
            status,
            details or 'N/A',
            duration_minutes if duration_minutes is not None else 'N/A',
            index_size if index_size is not None else 'N/A',
            source
        ])

def extract_date_from_timestamp(timestamp):
    """Extrai a data de um timestamp em milissegundos ou string ISO."""
    try:
        if isinstance(timestamp, (int, float)):
            dt = datetime.datetime.fromtimestamp(timestamp / 1000)
        else:
            dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return None

def get_records_date(client, index_name):
    """Extrai a data média de registros de um índice usando agregação stats."""
    timestamp_fields = ['@timestamp', 'timestamp']
    for field in timestamp_fields:
        try:
            response = client.search(
                index=index_name,
                body={
                    "query": {"match_all": {}},
                    "size": 0,
                    "aggs": {
                        "stats_timestamp": {
                            "stats": {"field": field}
                        }
                    }
                }
            )
            stats = response['aggregations']['stats_timestamp']
            if stats['count'] > 0 and 'avg' in stats:
                avg_timestamp = stats['avg']
                records_date = extract_date_from_timestamp(avg_timestamp)
                if records_date:
                    return records_date
        except Exception as e:
            sys.stdout.write(f"\033[K[ERROR] Erro ao obter stats para campo {field} no índice {index_name}: {e}\n")
            sys.stdout.flush()

    sys.stdout.write(f"\033[K[WARNING] Agregação stats falhou para {index_name}. Usando fallback para primeiro documento.\n")
    sys.stdout.flush()
    scroll_time = '1m'
    batch_size = 1
    try:
        response = client.search(
            index=index_name,
            scroll=scroll_time,
            size=batch_size,
            body={"query": {"match_all": {}}}
        )
        hits = response['hits']['hits']
        if hits:
            doc = hits[0]['_source']
            timestamp_fields_all = ['timestamp', '@timestamp', 'date', 'created_at', 'time', 'datetime', 'log_date', 'event_time']
            date_patterns = [
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{4}/\d{2}/\d{2})',
                r'(\d{2}/\d{2}/\d{4})',
                r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
                r'(\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}',
            ]
            for field in timestamp_fields_all:
                if field in doc:
                    timestamp_str = str(doc[field])
                    for pattern in date_patterns:
                        match = re.search(pattern, timestamp_str)
                        if match:
                            date_str = match.group(1)
                            try:
                                if '/' in date_str:
                                    if len(date_str.split('/')[0]) == 4:
                                        date_obj = datetime.datetime.strptime(date_str, '%Y/%m/%d')
                                    else:
                                        date_obj = datetime.datetime.strptime(date_str, '%m/%d/%Y')
                                    return date_obj.strftime('%Y-%m-%d')
                                else:
                                    return date_str
                            except ValueError:
                                continue
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Erro no fallback de data para {index_name}: {e}\n")
        sys.stdout.flush()
    return None

def get_backed_up_indexes_from_s3(s3_client, bucket_name):
    """Lista arquivos nas subpastas do bucket S3 baseadas na pasta principal."""
    backed_up_indexes = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        sys.stdout.write(f"\033[K[INFO] Verificando backups em {BASE_FOLDER}/...\n")
        sys.stdout.flush()

        for page in paginator.paginate(Bucket=bucket_name, Prefix=f"{BASE_FOLDER}/", Delimiter='/'):
            if 'CommonPrefixes' in page:
                for prefix_info in page['CommonPrefixes']:
                    year_path = prefix_info['Prefix']
                    
                    path_parts = year_path.rstrip('/').split('/')
                    if path_parts and re.match(r'^\d{4}$', path_parts[-1]):
                        inner_paginator = s3_client.get_paginator('list_objects_v2')
                        for inner_page in inner_paginator.paginate(Bucket=bucket_name, Prefix=year_path, Delimiter='/'):
                            if 'CommonPrefixes' in inner_page:
                                for obj_prefix in inner_page['CommonPrefixes']:
                                    full_prefix = obj_prefix['Prefix']
                                    parts = full_prefix.rstrip('/').split('/')
                                    if parts:
                                        folder_name = parts[-1]
                                        normalized_folder_name = normalize_index_name(folder_name)
                                        backed_up_indexes.add(normalized_folder_name)
        
        sys.stdout.write(f"\033[K[INFO] Índices já backupados encontrados em {BASE_FOLDER}: {backed_up_indexes}\n")
        sys.stdout.flush()
    
    except ClientError as e:
        sys.stdout.write(f"\033[K[ERROR] Erro ao listar objetos no S3: {e}\n")
        sys.stdout.flush()
    
    return backed_up_indexes

def log_stage(index_name, stage, message):
    """Exibe logs simplificados com timestamp e separadores por índice (tempo real no console)."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sys.stdout.write(f"\033[K[{timestamp}] {index_name} | {stage}: {message}\n")
    sys.stdout.flush()

def extract_shard(shard_id, index_name, output_file, scroll_time, batch_size):
    """Extrai dados de um shard específico e escreve em um arquivo temporário."""
    shard_file = f"{output_file}.{shard_id}"
    try:
        response = client.search(
            index=index_name,
            scroll=scroll_time,
            size=batch_size,
            body={"query": {"match_all": {}}},
            preference=f"_shards:{shard_id}"
        )
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        with open(shard_file, 'w', encoding='utf-8') as f:
            while hits:
                for doc in hits:
                    f.write(json.dumps(doc['_source']) + '\n')
                response = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
                scroll_id = response['_scroll_id']
                hits = response['hits']['hits']
        return shard_file
    except Exception as e:
        log_stage(index_name, "Erro", f"Falha na extração do shard {shard_id}: {e}")
        raise

def backup_and_upload_to_s3(client, index, bucket_name, backed_up_indexes, s3_client, log_lock, graylog_logger):
    """Realiza o backup de um índice do OpenSearch, compacta em GZ e faz upload para o S3."""
    start_time = datetime.datetime.now()
    index_name = index['name']
    index_size = index['size']
    output_file = f"/opt/staging/{index_name}_backup.jsonl"
    gz_file = None
    records_date = None
    shard_files = []

    try:
        log_stage(index_name, "Início", f"Processando índice ({index['docs_count']} docs, {index_size} bytes)")
        index_base_name = normalize_index_name(index_name)
        
        records_date = get_records_date(client, index_name)
        
        if records_date is None:
            log_stage(index_name, "Data", f"Warning: Nenhum timestamp válido encontrado. Usando nome sem data.")
        
        expected_gz_name = f"{index_name}_{records_date}_backup.jsonl.gz" if records_date else f"{index_name}_backup.jsonl.gz"

        if len(expected_gz_name) > 200:
            expected_gz_name = f"{index_name[:100]}_backup.jsonl.gz"
            log_stage(index_name, "Nomeação", f"Nome do arquivo truncado para {expected_gz_name} devido a limite de comprimento")

        gz_file = f"/opt/staging/{expected_gz_name}"
        
        if records_date:
            folder_year = records_date.split('-')[0]
        else:
            folder_year = datetime.datetime.now().strftime('%Y')
            
        s3_key = f"{BASE_FOLDER}/{folder_year}/{index_base_name}/{expected_gz_name}"

        backup_exists = False
        existing_key = None
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=f"{BASE_FOLDER}/"):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if index_name in key and key.endswith('.jsonl.gz'):
                            backup_exists = True
                            existing_key = key
                            break
                    if backup_exists:
                        break
        except ClientError as e:
            sys.stdout.write(f"\033[K[WARNING] Erro ao verificar duplicata para {index_name}: {e}\n")

        if backup_exists:
            log_stage(index_name, "Verificação", f"Backup já existe: {existing_key or 'algum caminho'}. Ignorando.")
            write_backup_log(backup_log_file, index_name, 'N/A', 'N/A', 'N/A', 'SKIPPED', 
                             f'Backup já existe em {existing_key or "algum caminho"}', 
                             0, index_size, log_lock, graylog_logger)
            return

        settings = client.indices.get_settings(index=index_name)
        num_shards = int(settings[index_name]['settings']['index'].get('number_of_shards', '1'))
        log_stage(index_name, "Extração", f"Iniciando backup (paralelizado por {num_shards} shards)")

        scroll_time = '2m'
        batch_size = 6000

        def extract_shard_wrapper(shard_id):
            return extract_shard(shard_id, index_name, output_file, scroll_time, batch_size)

        with ThreadPoolExecutor(max_workers=num_shards) as shard_executor:
            futures = [shard_executor.submit(extract_shard_wrapper, i) for i in range(num_shards)]
            for future in as_completed(futures):
                shard_files.append(future.result())

        log_stage(index_name, "Extração", f"Combinando {len(shard_files)} arquivos de shards em {output_file}")
        with open(output_file, 'w', encoding='utf-8') as out:
            for shard_file in shard_files:
                with open(shard_file, 'r', encoding='utf-8') as inp:
                    shutil.copyfileobj(inp, out)
                try:
                    os.unlink(shard_file)
                    log_stage(index_name, "Limpeza", f"Arquivo temporário {shard_file} excluído")
                except OSError as e:
                    log_stage(index_name, "Limpeza", f"Erro ao excluir arquivo temporário {shard_file}: {e}")

        log_stage(index_name, "Extração", f"Backup concluído: {output_file}")

        log_stage(index_name, "Compressão", f"Compactando para {gz_file}")
        try:
            num_cores = os.cpu_count() or 4
            subprocess.run(['pigz', '-1', '-p', str(num_cores), output_file], check=True)
            os.rename(f"{output_file}.gz", gz_file)
            log_stage(index_name, "Compressão", f"Arquivo compactado: {gz_file}")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            log_stage(index_name, "Compressão", f"Erro no pigz, usando gzip padrão: {e}")
            with open(output_file, 'rb') as f_in:
                with gzip.open(gz_file, 'wb', compresslevel=1) as f_out:
                    shutil.copyfileobj(f_in, f_out, length=64 * 1024 * 1024)
            log_stage(index_name, "Compressão", f"Arquivo compactado (fallback): {gz_file}")

        log_stage(index_name, "MD5", f"Calculando hash MD5")
        md5_checksum = calculate_md5(gz_file)
        log_stage(index_name, "MD5", f"Checksum: {md5_checksum}")

        if not os.path.exists(gz_file):
            raise FileNotFoundError(f"Arquivo {gz_file} não encontrado antes do upload")
        log_stage(index_name, "Verificação", f"Arquivo {gz_file} verificado com sucesso")

        log_stage(index_name, "Upload", f"Iniciando upload para S3: {s3_key}")
        result = upload_large_file_parallel(s3_client, bucket_name, gz_file, s3_key)

        end_time = datetime.datetime.now()
        duration_minutes = (end_time - start_time).total_seconds() / 60

        if result:
            log_stage(index_name, "Upload", f"Upload concluído: {s3_key}")
            write_backup_log(backup_log_file, index_name, output_file, s3_key, md5_checksum, 'SUCCESS', None, duration_minutes, index_size, log_lock, graylog_logger)
        else:
            log_stage(index_name, "Upload", f"Falha no upload para {s3_key}")
            write_backup_log(backup_log_file, index_name, output_file, s3_key, md5_checksum, 'FAILED', 'Falha no upload multipart', duration_minutes, index_size, log_lock, graylog_logger)

    except Exception as e:
        end_time = datetime.datetime.now()
        duration_minutes = (end_time - start_time).total_seconds() / 60
        log_stage(index_name, "Erro", f"Erro no backup: {e}")
        write_backup_log(backup_log_file, index_name, output_file if 'output_file' in locals() else 'N/A', s3_key if 's3_key' in locals() else 'N/A', None, 'ERROR', str(e), duration_minutes, index_size, log_lock, graylog_logger)

    finally:
        if 'output_file' in locals() and os.path.exists(output_file):
            try:
                os.unlink(output_file)
                log_stage(index_name, "Limpeza", f"Excluindo {output_file}")
            except OSError as e:
                log_stage(index_name, "Limpeza", f"Erro ao excluir {output_file}: {e}")
        if gz_file and os.path.exists(gz_file):
            try:
                os.unlink(gz_file)
                log_stage(index_name, "Limpeza", f"Excluindo {gz_file}")
            except OSError as e:
                log_stage(index_name, "Limpeza", f"Erro ao excluir {gz_file}: {e}")
        for shard_file in shard_files:
            if os.path.exists(shard_file):
                try:
                    os.unlink(shard_file)
                    log_stage(index_name, "Limpeza", f"Excluindo arquivo temporário {shard_file}")
                except OSError as e:
                    log_stage(index_name, "Limpeza", f"Erro ao excluir arquivo temporário {shard_file}: {e}")

def upload_log_to_s3(s3_client, bucket_name, backup_log_file):
    """Faz upload do arquivo de log para o S3 (simplificado, apenas no final)."""
    log_key = f"{BASE_FOLDER}/Backup_log/{os.path.basename(backup_log_file)}"
    try:
        if not os.path.exists(backup_log_file):
            sys.stdout.write(f"\033[K[ERROR] Arquivo de log {backup_log_file} não encontrado\n")
            sys.stdout.flush()
            return
        if os.path.getsize(backup_log_file) == 0:
            sys.stdout.write(f"\033[K[ERROR] Arquivo de log {backup_log_file} está vazio\n")
            sys.stdout.flush()
            return
        
        md5_checksum = calculate_md5(backup_log_file)
        sys.stdout.write(f"\033[K[INFO] MD5 do log {backup_log_file}: {md5_checksum}\n")
        sys.stdout.flush()

        with open(backup_log_file, 'rb') as file_data:
            file_content = file_data.read()
            s3_client.put_object(
                Bucket=bucket_name,
                Key=log_key,
                Body=file_content,
                ContentMD5=base64.b64encode(hashlib.md5(file_content).digest()).decode('utf-8')
            )
        sys.stdout.write(f"\033[K[INFO] Log carregado para S3: {log_key}\n")
        sys.stdout.flush()
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Erro ao carregar log para o S3: {e}\n")
        sys.stdout.flush()

def upload_large_file_parallel(s3_client, bucket_name, file_path, object_key, part_size=5 * 1024 * 1024, max_workers=10):
    """Faz upload de um arquivo para o S3 usando upload multipart paralelo com SHA256."""
    def log_upload(stage, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sys.stdout.write(f"\033[K[{timestamp}] Upload | {stage}: {message}\n")
        sys.stdout.flush()

    upload_id = None
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo {file_path} não encontrado")

        response = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        upload_id = response['UploadId']
        log_upload("Início", f"Upload multipart iniciado: {object_key}")

        file_size = os.path.getsize(file_path)
        parts = []
        part_ranges = []
        offset = 0
        part_number = 1
        while offset < file_size:
            length = min(part_size, file_size - offset)
            part_ranges.append((part_number, offset, length))
            offset += length
            part_number += 1

        def upload_part(part_number, offset, length):
            with open(file_path, 'rb') as part_file:
                part_file.seek(offset)
                data = part_file.read(length)
                try:
                    response = s3_client.upload_part(
                        Bucket=bucket_name,
                        Key=object_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=data,
                        ChecksumSHA256=base64.b64encode(hashlib.sha256(data).digest()).decode('utf-8')
                    )
                    log_upload("Parte", f"Parte {part_number} enviada com sucesso (ETag: {response['ETag']})")
                    return {'PartNumber': part_number, 'ETag': response['ETag']}
                except ClientError as e:
                    log_upload("Erro", f"Erro ao enviar parte {part_number}: {e}")
                    raise

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(upload_part, pn, off, ln) for pn, off, ln in part_ranges]
            for future in as_completed(futures):
                parts.append(future.result())

        parts.sort(key=lambda x: x['PartNumber'])
        complete_response = s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        time.sleep(5)
        log_upload("Conclusão", f"Upload multipart concluído: {object_key}")
        return complete_response

    except Exception as e:
        log_upload("Erro", f"Erro no upload multipart de {object_key}: {e}")
        if upload_id:
            try:
                s3_client.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)
                log_upload("Abortado", f"Upload multipart abortado: {object_key}")
            except ClientError as abort_error:
                log_upload("Erro", f"Erro ao abortar upload multipart: {abort_error}")
        return None

def main():
    """Função principal para executar o backup de índices."""
    log_lock = threading.Lock()
    
    stream_id = os.getenv('GRAYLOG_STREAM_ID', 'default_stream_id')
    graylog_logger = GraylogAuditLogger(audit_client, stream_id)
    
    graylog_logger.log_to_graylog(
        index_name='',
        backup_file='',
        s3_key='',
        md5sum='',
        status='STARTED',
        details='backup_iniciado',
        duration_minutes='',
        index_size='',
        message='Iniciando backup de índices'
    )
    
    try:
        sys.stdout.write(f"\033[K[INFO] Conectado ao cluster FONTE (Leitura): {client.info()['cluster_name']}\n")
        sys.stdout.flush()
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Falha ao conectar ao cluster FONTE: {e}\n")
        sys.stdout.flush()
        raise

    try:
        audit_info = audit_client.info()
        sys.stdout.write(f"\033[K[INFO] Conectado ao cluster DESTINO LOGS (Escrita): {audit_info['cluster_name']}\n")
        sys.stdout.flush()
    except Exception as e:
        sys.stdout.write(f"\033[K[WARNING] Falha ao conectar ao cluster de LOGS (Auditoria): {e}. Logs remotos podem falhar.\n")
        sys.stdout.flush()

    backed_up_indexes = get_backed_up_indexes_from_s3(s3_client, bucket_name)
    try:
        indexes = get_all_indexes(client)
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Não foi possível listar índices. Abortando: {e}\n")
        sys.stdout.flush()
        return

    filtered_indexes = []
    for idx in indexes:
        name = idx['name']
        if any(name.startswith(prefix) for prefix in TARGET_PREFIXES):
            filtered_indexes.append(idx)
    
    def get_sort_priority(index_dict):
        name = index_dict['name']
        if name.startswith('rev'):
            return 0
        elif name.startswith('win'):
            return 1
        else:
            return 2

    filtered_indexes.sort(key=get_sort_priority)

    sys.stdout.write(f"\033[K[INFO] Total de {len(indexes)} índices no cluster FONTE.\n")
    sys.stdout.write(f"\033[K[INFO] Total de {len(filtered_indexes)} índices filtrados para backup.\n")
    
    for index in filtered_indexes:
        sys.stdout.write(f"\033[K- {index['name']} ({index['docs_count']} docs, {index['size']} bytes)\n")
    sys.stdout.flush()

    non_empty_indexes = []
    summary = {'total': len(filtered_indexes), 'success': 0, 'failed': 0, 'skipped': 0}
    
    for index in filtered_indexes:
        index_name = index['name']
        docs_count = int(index['docs_count'] or 0)
        
        if is_index_active(client, index_name):
            sys.stdout.write(f"\033[K[INFO] Índice {index_name} ativo e do dia atual. Ignorando.\n")
            sys.stdout.flush()
            write_backup_log(backup_log_file, index_name, 'N/A', 'N/A', 'N/A', 'SKIPPED', 'Índice ativo do dia atual, aguardando rotação', 0, index['size'], log_lock, graylog_logger)
            summary['skipped'] += 1
        elif docs_count == 0:
            sys.stdout.write(f"\033[K[INFO] Índice {index_name} vazio (0 documentos). Ignorando.\n")
            sys.stdout.flush()
            write_backup_log(backup_log_file, index_name, 'N/A', 'N/A', 'N/A', 'SKIPPED', 'Índice vazio', 0, index['size'], log_lock, graylog_logger)
            summary['skipped'] += 1
        else:
            non_empty_indexes.append(index)

    sys.stdout.write(f"\033[K[INFO] Iniciando backup de {len(non_empty_indexes)} índices em paralelo...\n")
    sys.stdout.flush()
    with ThreadPoolExecutor(max_workers=14) as executor:
        futures = [executor.submit(backup_and_upload_to_s3, client, index, bucket_name, backed_up_indexes, s3_client, log_lock, graylog_logger) for index in non_empty_indexes]
        for future in as_completed(futures):
            future.result()

    try:
        with open(backup_log_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['status'] == 'SUCCESS':
                    summary['success'] += 1
                elif row['status'] in ('FAILED', 'ERROR'):
                    summary['failed'] += 1
                elif row['status'] == 'SKIPPED':
                    summary['skipped'] += 1
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Erro ao ler log para sumário: {e}\n")
        sys.stdout.flush()

    upload_log_to_s3(s3_client, bucket_name, backup_log_file)

    graylog_logger.log_to_graylog(
        index_name='',
        backup_file='',
        s3_key='',
        md5sum='',
        status='FINISHED',
        details='backup_finalizado',
        duration_minutes='N/A',
        index_size='N/A',
        message='Backup de índices concluído'
    )

    sys.stdout.write("\033[K\n[RESUMO FINAL]\n")
    sys.stdout.write(f"\033[KTotal Processado: {summary['total']}\n")
    sys.stdout.write(f"\033[KSucessos: {summary['success']}\n")
    sys.stdout.write(f"\033[KFalhas: {summary['failed']}\n")
    sys.stdout.write(f"\033[KIgnorados: {summary['skipped']}\n")
    sys.stdout.write(f"\033[KLog disponível em: {backup_log_file}\n")
    sys.stdout.write(f"\033[K[INFO] Backup concluído.\n")
    sys.stdout.flush()

if __name__ == "__main__":
    main()