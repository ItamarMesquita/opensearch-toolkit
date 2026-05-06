
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
