
# OpenSearch Parallel Backup to S3 🛡️📦

Script Python avançado para backup de índices do OpenSearch com upload direto para Amazon S3. Projetado para lidar com grandes volumes de dados e garantir a integridade absoluta das informações.

## 🚀 Funcionalidades de Elite

- **Backup Paralelo:** Utiliza `ThreadPoolExecutor` para processar múltiplos índices simultaneamente, otimizando o tempo de execução.
- **Estratégia Split-by-Date:** Lê o timestamp de cada documento em tempo real e roteia os dados para arquivos baseados na data original do log. Isso resolve o problema de **Late-Arriving Data** (dados que chegam com atraso no cluster).
- **Auditoria de Integridade (Data Completeness):** Implementa uma validação rigorosa. O backup só é considerado válido se a soma dos documentos extraídos for **exatamente igual** à contagem oficial do índice no OpenSearch.
- **Compressão Gzip Dinâmica:** Os dados são comprimidos durante o processo para economizar espaço em disco e acelerar o upload.
- **Segurança Robusta:** Suporte total a SSL/TLS (CA, Certificado de Cliente e Key) e autenticação AWS via `boto3`.
- **Auditoria Externa:** Envio de logs estruturados para o **Graylog** via GELF (UDP) para monitorização em tempo real.

## 🛠️ Pré-requisitos

- Python 3.8+
- Bibliotecas necessárias:
  ```bash
  pip install opensearch-py boto3 python-dotenv
⚙️ Configuração
O script é totalmente configurado via variáveis de ambiente. Crie um ficheiro .env na raiz do projeto:

Snippet de código
# --- OpenSearch ---
OPENSEARCH_HOSTS=seu-cluster-opensearch.com
OPENSEARCH_USER=admin
OPENSEARCH_PASS=sua_senha
OPENSEARCH_CA_CERT=./certs/ca.pem

# --- AWS S3 ---
AWS_ACCESS_KEY_ID=sua_key
AWS_SECRET_ACCESS_KEY=sua_secret
AWS_REGION=us-east-1
S3_BUCKET_NAME=nome-do-seu-bucket

# --- Auditoria (Graylog) ---
GRAYLOG_HOST=seu-graylog.com
GRAYLOG_PORT=12201
GRAYLOG_STREAM_ID=id_da_stream

# --- Performance & Staging ---
MAX_WORKERS=4
STAGING_DIR=/opt/staging
💻 Como Utilizar
Para iniciar o processo de backup de todos os índices que seguem o padrão definido:

Bash
python 8.5-opensearch-backup-parallel.py
O que o script faz:
Conecta-se ao cluster e identifica índices não vazios que não sejam aliases (como o deflector do Graylog).

Extrai os documentos via Scroll API.

Distribui os documentos em arquivos locais comprimidos baseados na data do evento.

Valida se total_extraído == docs.count.

Faz o upload para o S3 seguindo a estrutura: s3://bucket/ano/nome-indice/arquivo.json.gz.

Limpa o diretório de staging e envia o log final para o S3 e Graylog.

📊 Monitorização e Logs
Logs Locais: Um CSV detalhado é gerado a cada execução: backup_opensearch_log_YYYYMMDD_HHMMSS.csv.

Logs Remotos: O status de cada índice (Início, Sucesso, Falha, Upload) é enviado para o Graylog em tempo real.

Relatório no S3: O log da execução é carregado para o bucket após o término do processo.

⚖️ Licença
Este utilitário é fornecido para fins de administração de sistemas e manutenção de infraestrutura. Utilize em ambiente de staging antes de promover para produção.