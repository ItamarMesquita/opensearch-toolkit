#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para realizar backup de índices do OpenSearch e upload para o Amazon S3.
Funcionalidade avançada (Split-by-Date): Lê o timestamp de cada documento em tempo real 
e roteia para arquivos diferentes baseados na data do log (mitigação de Late-Arriving Data).
Segurança (Data Completeness): Implementa auditoria rigorosa de contagem. O backup só 
prosseque se a soma de documentos extraídos (em todos os splits) for EXATAMENTE igual
ao docs.count original do índice.
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

# === Configurações lidas do .env ===
_opensearch_hosts = [h.strip() for h in os.getenv('OPENSEARCH_HOSTS', '').split(',') if h.strip()]
_opensearch_user  = os.getenv('OPENSEARCH_USER', 'admin')
_opensearch_pass  = os.getenv('OPENSEARCH_PASS', 'admin')
_opensearch_ca    = os.getenv('OPENSEARCH_CA_CERT')
_opensearch_cert  = os.getenv('OPENSEARCH_CLIENT_CERT')
_opensearch_key   = os.getenv('OPENSEARCH_CLIENT_KEY')

_aws_key_id       = os.getenv('AWS_ACCESS_KEY_ID')
_aws_secret       = os.getenv('AWS_SECRET_ACCESS_KEY')
_s3_endpoint      = os.getenv('S3_ENDPOINT_URL')
_s3_region        = os.getenv('S3_REGION', 'us-east-1')

_graylog_stream   = os.getenv('GRAYLOG_STREAM_ID')
_staging_dir      = os.getenv('STAGING_DIR', '/opt/staging')
_max_workers      = int(os.getenv('MAX_BACKUP_WORKERS', '4'))

# Nome do bucket S3 para armazenamento dos backups
bucket_name = os.getenv('S3_BUCKET_NAME')

# Prefixo de ano para o caminho no S3
year_prefix = datetime.datetime.now().strftime('%Y')

# Caminho do arquivo de log CSV com timestamp
backup_log_file = f"{_staging_dir}/graylog_backup_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Configuração do cliente OpenSearch
client = OpenSearch(
    hosts=[{'host': h, 'port': 9200} for h in _opensearch_hosts],
    http_auth=(_opensearch_user, _opensearch_pass),
    use_ssl=True,
    verify_certs=True,
    ca_certs=_opensearch_ca,
    client_cert=_opensearch_cert,
    client_key=_opensearch_key,
    timeout=15,
    max_retries=2,
    retry_on_timeout=True,
    maxsize=25,
)

s3_client = boto3.client(
    's3',
    aws_access_key_id=_aws_key_id,
    aws_secret_access_key=_aws_secret,
    endpoint_url=_s3_endpoint,
    region_name=_s3_region,
    config=Config(
        signature_version='s3v4',
        s3={'addressing_style': 'path', 'payload_signing_enabled': True},
        retries={'max_attempts': 5, 'mode': 'standard'},
        connect_timeout=60,
        read_timeout=60
    )
)

class GraylogAuditLogger:
    def __init__(self, client, stream_id):
        self.client = client
        self.stream_id = stream_id
        self._active_index = None
        self._cache_ts = None
        self._ttl = 300

    def _is_write_allowed(self, idx):
        try:
            s = self.client.indices.get_settings(index=idx)
            blocks = s[idx].get('settings', {}).get('index', {}).get('blocks', {})
            if blocks.get('write') == 'true' or blocks.get('read_only') == 'true':
                return False
            if s[idx].get('settings', {}).get('index', {}).get('read_only_allow_delete') == 'true':
                return False
        except Exception:
            pass
        return True

    def _test_write(self, idx):
        test_doc = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "test": True,
            "message": "TESTE ESCRITA – IGNORE",
            "streams": [self.stream_id]
        }
        try:
            resp = self.client.index(index=idx, body=test_doc, refresh="wait_for")
            if resp["result"] in ("created", "updated"):
                self.client.delete(index=idx, id=resp["_id"])
                return True
        except Exception:
            return False

    def find_active_index(self):
        if self._active_index and self._cache_ts and (time.time() - self._cache_ts) < self._ttl:
            return self._active_index
        try:
            resp = self.client.cat.indices(format="json")
        except Exception:
            return None
        candidates = [i["index"] for i in resp if i["index"].startswith("backup_graylog__") and i.get("status") == "open"]
        candidates.sort(key=lambda x: int(re.search(r"__(\d+)", x).group(1)) if re.search(r"__(\d+)", x) else 0, reverse=True)
        for idx in candidates:
            if self._is_write_allowed(idx) and self._test_write(idx):
                self._active_index = idx
                self._cache_ts = time.time()
                return idx
        return None

    def log_to_graylog(self, index_name, backup_file, s3_key, md5sum, status, details, duration_minutes, index_size, message):
        active = self.find_active_index()
        if not active: return
        doc = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
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
            "message": message,
            "origin_system": "graylog_audit"
        }
        try:
            self.client.index(index=active, body=doc)
        except Exception:
            pass

def is_index_active(client, index_name):
    """Verifica se o índice é o ativo de escrita atual checando o alias '_deflector' do Graylog."""
    try:
        try:
            response = client.indices.get_alias(index=index_name)
            if index_name in response:
                aliases = response[index_name].get('aliases', {})
                for alias in aliases:
                    if alias.endswith('_deflector'):
                        return True
        except Exception:
            pass
        # Fallback de segurança na data
        settings = client.indices.get_settings(index=index_name)
        creation_date_ms = settings[index_name]['settings']['index'].get('creation_date')
        if creation_date_ms:
            creation_date = datetime.datetime.fromtimestamp(int(creation_date_ms) / 1000).strftime('%Y-%m-%d')
            current_date = datetime.datetime.now().strftime('%Y-%m-%d')
            if creation_date == current_date:
                return True
        return False
    except Exception:
        return True

def parse_size(size_str):
    units = {'b': 1, 'kb': 1024, 'mb': 1024**2, 'gb': 1024**3, 'tb': 1024**4}
    size_str = size_str.lower()
    match = re.match(r'^(\d*\.?\d+)([a-z]+)?$', size_str)
    if match: return int(float(match.group(1)) * units.get(match.group(2) or 'b', 1))
    try: return int(size_str)
    except ValueError: return 0

def normalize_index_name(index_name):
    match = re.match(r'^(.*?)(?:__\d+)?$', index_name)
    return match.group(1) if match else index_name

def get_all_indexes(client):
    response = client.cat.indices(format='json')
    indexes = []
    for index in response:
        index_name = index['index']
        try:
            settings = client.indices.get_settings(index=index_name)
            replicas = int(settings[index_name]['settings']['index'].get('number_of_replicas', '0'))
        except Exception:
            replicas = 0
        size = parse_size(index.get('store.size', '0'))
        primary_size = size // (replicas + 1) if replicas >= 0 else size
        indexes.append({'name': index_name, 'docs_count': int(index.get('docs.count', '0')), 'size': primary_size})
    return indexes

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def calculate_sha256(data):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)
    return sha256_hash.hexdigest()

def write_backup_log(log_file, index_name, backup_file, s3_key, md5sum, status, details=None, duration_minutes=None, index_size=None, log_lock=None, graylog_logger=None):
    message = f"Backup do índice {index_name}: {status.lower()} ({details or 'N/A'})"
    source = platform.node()
    
    def _write():
        with open(log_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if os.stat(log_file).st_size == 0:
                writer.writerow(['timestamp', 'index_name', 'backup_file', 's3_key', 'md5sum', 'status', 'details', 'duration_minutes', 'index_size', 'source'])
            writer.writerow([
                datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                index_name, backup_file, s3_key, md5sum, status, details or 'N/A', 
                duration_minutes if duration_minutes is not None else 'N/A', 
                index_size if index_size is not None else 'N/A', source
            ])
            
    if log_lock:
        with log_lock: _write()
    else:
        _write()
    
    if graylog_logger:
        graylog_logger.log_to_graylog(index_name, backup_file, s3_key, md5sum, status, details, duration_minutes, index_size, message)

def extract_date_from_doc(doc):
    """Extrai a data do documento de forma performática. Fallback para 'unknown_date'."""
    for field in ['timestamp', '@timestamp', 'date', 'created_at']:
        if field in doc:
            val = str(doc[field])
            if len(val) >= 10 and val[4] == '-' and val[7] == '-':
                return val[:10]
            match = re.search(r'(\d{4}-\d{2}-\d{2})', val)
            if match:
                return match.group(1)
    return "unknown_date"

def log_stage(index_name, stage, message):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sys.stdout.write(f"\033[K[{timestamp}] {index_name} | {stage}: {message}\n")
    sys.stdout.flush()

def extract_shard_by_date(shard_id, index_name, staging_dir, scroll_time, batch_size):
    """Extrai documentos, separa por data em arquivos temporários e RETORNA A CONTAGEM EXATA DE DOCS LIDOS."""
    file_handles = {}
    shard_files_generated = []
    extracted_count = 0
    
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
        
        while hits:
            for doc_wrapper in hits:
                doc = doc_wrapper['_source']
                doc_date = extract_date_from_doc(doc)
                
                if doc_date not in file_handles:
                    shard_file_path = f"{staging_dir}/{index_name}_{doc_date}.jsonl.shard_{shard_id}"
                    file_handles[doc_date] = open(shard_file_path, 'w', encoding='utf-8')
                    shard_files_generated.append(shard_file_path)
                
                file_handles[doc_date].write(json.dumps(doc) + '\n')
                extracted_count += 1
                
            response = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']
            
        return shard_files_generated, extracted_count
    except Exception as e:
        log_stage(index_name, "Erro", f"Falha na extração do shard {shard_id}: {e}")
        raise
    finally:
        for fh in file_handles.values():
            fh.close()

def backup_and_upload_to_s3(client, index, bucket_name, s3_client, log_lock, graylog_logger):
    """Extrai índice particionando por datas, CONFERE CONTAGEM, consolida arquivos e faz uploads paralelos."""
    start_time = datetime.datetime.now()
    index_name = index['name']
    index_size = index['size']
    index_expected_docs = index['docs_count']
    index_base_name = normalize_index_name(index_name)
    all_shard_files = []

    try:
        # Checagem de segurança e recuperação do caminho do S3
        backup_exists = False
        existing_key = None
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if index_name in obj['Key'] and obj['Key'].endswith('.jsonl.gz'):
                            backup_exists = True
                            existing_key = obj['Key']
                            break
                    if backup_exists: break
        except ClientError as e:
            sys.stdout.write(f"\033[K[WARNING] Erro ao verificar duplicata para {index_name}: {e}\n")

        if backup_exists:
            details_msg = f"Backup já existe em {existing_key}"
            log_stage(index_name, "Verificação", f"Backup parcial ou total já existe no S3. Ignorando. ({existing_key})")
            # Agora enviamos o existing_key para a coluna s3_key e o details_msg atualizado
            write_backup_log(backup_log_file, index_name, 'N/A', existing_key, 'N/A', 'SKIPPED', details_msg, 0, index_size, log_lock, graylog_logger)
            return

        settings = client.indices.get_settings(index=index_name)
        num_shards = int(settings[index_name]['settings']['index'].get('number_of_shards', '1'))
        log_stage(index_name, "Extração", f"Iniciando leitura de {index_expected_docs} documentos (Paralelismo: {num_shards} shards)")

        scroll_time = '2m'
        batch_size = 6000
        total_extracted_docs = 0

        def extract_wrapper(shard_id):
            return extract_shard_by_date(shard_id, index_name, _staging_dir, scroll_time, batch_size)

        with ThreadPoolExecutor(max_workers=num_shards) as shard_executor:
            futures = [shard_executor.submit(extract_wrapper, i) for i in range(num_shards)]
            for future in as_completed(futures):
                shard_files, shard_count = future.result()
                all_shard_files.extend(shard_files)
                total_extracted_docs += shard_count

        # =========================================================================
        # AUDITORIA DE COMPLETUDE (DATA COMPLETENESS VALIDATION)
        # =========================================================================
        log_stage(index_name, "Auditoria", f"Analisando integridade: Extraídos {total_extracted_docs} / Esperados {index_expected_docs}")
        if total_extracted_docs != index_expected_docs:
            raise ValueError(
                f"FALHA DE INTEGRIDADE DE DADOS: O índice contém {index_expected_docs} documentos, "
                f"mas a extração capturou apenas {total_extracted_docs}. "
                f"Isso indica perda de dados durante a leitura no cluster OpenSearch (diferença: {index_expected_docs - total_extracted_docs} docs). "
                f"O backup foi interrompido para este índice para evitar geração de arquivos corrompidos/incompletos no S3."
            )
        # =========================================================================

        # Agrupar os arquivos temporários por Data
        files_by_date = {}
        for sf in all_shard_files:
            match = re.search(fr"{index_name}_(.+?)\.jsonl\.shard_\d+", sf)
            if match:
                doc_date = match.group(1)
                files_by_date.setdefault(doc_date, []).append(sf)

        log_stage(index_name, "Separação", f"Dados confirmados. Datas identificadas: {list(files_by_date.keys())}")

        # Processa cada data separadamente
        for doc_date, shard_files_for_date in files_by_date.items():
            expected_gz_name = f"{index_name}_{doc_date}_backup.jsonl.gz"
            if len(expected_gz_name) > 200:
                expected_gz_name = f"{index_name[:100]}_{doc_date}_backup.jsonl.gz"

            output_file = f"{_staging_dir}/{expected_gz_name.replace('.gz', '')}"
            gz_file = f"{_staging_dir}/{expected_gz_name}"
            s3_key = f"{year_prefix}/{index_base_name}/{expected_gz_name}"

            log_stage(index_name, "Consolidação", f"Juntando arquivos para a data {doc_date}")
            with open(output_file, 'w', encoding='utf-8') as out:
                for sf in shard_files_for_date:
                    with open(sf, 'r', encoding='utf-8') as inp:
                        shutil.copyfileobj(inp, out)
                    try:
                        os.unlink(sf)
                    except OSError:
                        pass
            
            for sf in shard_files_for_date:
                if sf in all_shard_files: all_shard_files.remove(sf)

            log_stage(index_name, "Compressão", f"Compactando {doc_date} para {gz_file}")
            try:
                num_cores = os.cpu_count() or 4
                subprocess.run(['pigz', '-1', '-p', str(num_cores), output_file], check=True)
                os.rename(f"{output_file}.gz", gz_file)
            except (subprocess.CalledProcessError, FileNotFoundError):
                with open(output_file, 'rb') as f_in, gzip.open(gz_file, 'wb', compresslevel=1) as f_out:
                    shutil.copyfileobj(f_in, f_out, length=64 * 1024 * 1024)
            finally:
                if os.path.exists(output_file): os.unlink(output_file)

            md5_checksum = calculate_md5(gz_file)
            log_stage(index_name, "Upload", f"Enviando dados de {doc_date} para S3: {s3_key}")
            
            result = upload_large_file_parallel(s3_client, bucket_name, gz_file, s3_key)
            end_time = datetime.datetime.now()
            duration_minutes = (end_time - start_time).total_seconds() / 60

            if result:
                log_stage(index_name, "Upload", f"Upload concluído: {s3_key}")
                write_backup_log(backup_log_file, index_name, gz_file, s3_key, md5_checksum, 'SUCCESS', f"Data ref: {doc_date}", duration_minutes, index_size, log_lock, graylog_logger)
            else:
                log_stage(index_name, "Upload", f"Falha no upload para {s3_key}")
                write_backup_log(backup_log_file, index_name, gz_file, s3_key, md5_checksum, 'FAILED', f"Falha no upload multipart ({doc_date})", duration_minutes, index_size, log_lock, graylog_logger)

            if os.path.exists(gz_file):
                os.unlink(gz_file)

    except Exception as e:
        end_time = datetime.datetime.now()
        duration_minutes = (end_time - start_time).total_seconds() / 60
        log_stage(index_name, "Erro", f"Erro crítico no backup particionado: {e}")
        write_backup_log(backup_log_file, index_name, 'N/A', 'N/A', None, 'ERROR', str(e), duration_minutes, index_size, log_lock, graylog_logger)

    finally:
        for sf in all_shard_files:
            if os.path.exists(sf):
                try: os.unlink(sf)
                except OSError: pass

def upload_log_to_s3(s3_client, bucket_name, backup_log_file):
    log_key = f"Backup_log/{os.path.basename(backup_log_file)}"
    try:
        if not os.path.exists(backup_log_file) or os.path.getsize(backup_log_file) == 0:
            return
        with open(backup_log_file, 'rb') as file_data:
            file_content = file_data.read()
            s3_client.put_object(
                Bucket=bucket_name,
                Key=log_key,
                Body=file_content,
                ContentMD5=base64.b64encode(hashlib.md5(file_content).digest()).decode('utf-8')
            )
        sys.stdout.write(f"\033[K[INFO] Log carregado para S3: {log_key}\n")
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Erro ao carregar log para o S3: {e}\n")

def upload_large_file_parallel(s3_client, bucket_name, file_path, object_key, part_size=5 * 1024 * 1024, max_workers=10):
    upload_id = None
    try:
        response = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        upload_id = response['UploadId']
        file_size = os.path.getsize(file_path)
        parts, part_ranges = [], []
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
                response = s3_client.upload_part(
                    Bucket=bucket_name, Key=object_key, UploadId=upload_id, PartNumber=part_number,
                    Body=data, ChecksumSHA256=base64.b64encode(hashlib.sha256(data).digest()).decode('utf-8')
                )
                return {'PartNumber': part_number, 'ETag': response['ETag']}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(upload_part, pn, off, ln) for pn, off, ln in part_ranges]
            for future in as_completed(futures):
                parts.append(future.result())

        parts.sort(key=lambda x: x['PartNumber'])
        complete_response = s3_client.complete_multipart_upload(
            Bucket=bucket_name, Key=object_key, UploadId=upload_id, MultipartUpload={'Parts': parts}
        )
        return complete_response

    except Exception as e:
        if upload_id:
            try: s3_client.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)
            except ClientError: pass
        return None

def main():
    log_lock = threading.Lock()
    graylog_logger = GraylogAuditLogger(client, _graylog_stream)
    
    graylog_logger.log_to_graylog(index_name='', backup_file='', s3_key='', md5sum='', status='STARTED', details='backup_iniciado', duration_minutes='', index_size='', message='Iniciando backup de índices')
    
    try:
        sys.stdout.write(f"\033[K[INFO] Conectado ao cluster OpenSearch: {client.info()['cluster_name']}\n")
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Falha ao conectar ao cluster OpenSearch: {e}\n")
        raise

    try:
        indexes = get_all_indexes(client)
    except Exception as e:
        sys.stdout.write(f"\033[K[ERROR] Não foi possível listar índices. Abortando: {e}\n")
        return

    non_empty_indexes = []
    summary = {'total': len(indexes), 'success': 0, 'failed': 0, 'skipped': 0}
    for index in indexes:
        index_name = index['name']
        docs_count = int(index['docs_count'] or 0)
        
        if is_index_active(client, index_name):
            sys.stdout.write(f"\033[K[INFO] Índice {index_name} é o ativo de escrita atual. Ignorando.\n")
            write_backup_log(backup_log_file, index_name, 'N/A', 'N/A', 'N/A', 'SKIPPED', 'Índice ativo (Deflector)', 0, index['size'], log_lock, graylog_logger)
            summary['skipped'] += 1
        elif docs_count == 0:
            write_backup_log(backup_log_file, index_name, 'N/A', 'N/A', 'N/A', 'SKIPPED', 'Índice vazio', 0, index['size'], log_lock, graylog_logger)
            summary['skipped'] += 1
        else:
            non_empty_indexes.append(index)

    sys.stdout.write(f"\033[K[INFO] Iniciando backup particionado por data para {len(non_empty_indexes)} índices...\n")
    
    with ThreadPoolExecutor(max_workers=_max_workers) as executor:
        futures = [executor.submit(backup_and_upload_to_s3, client, index, bucket_name, s3_client, log_lock, graylog_logger) for index in non_empty_indexes]
        for future in as_completed(futures):
            future.result()

    try:
        with open(backup_log_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['status'] == 'SUCCESS': summary['success'] += 1
                elif row['status'] in ('FAILED', 'ERROR'): summary['failed'] += 1
                elif row['status'] == 'SKIPPED': summary['skipped'] += 1
    except Exception:
        pass

    upload_log_to_s3(s3_client, bucket_name, backup_log_file)
    graylog_logger.log_to_graylog(index_name='', backup_file='', s3_key='', md5sum='', status='FINISHED', details='backup_finalizado', duration_minutes='N/A', index_size='N/A', message='Backup de índices concluído')

    sys.stdout.write(f"\n\033[K[RESUMO FINAL]\nArquivos/Ações bem-sucedidas: {summary['success']}\nFalhas: {summary['failed']}\nIgnorados: {summary['skipped']}\nLog: {backup_log_file}\n")
    sys.stdout.flush()

if __name__ == "__main__":
    main()