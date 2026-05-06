**Aviso:** O script `backup_to_s3.py` utiliza o diretório `/opt/staging` para processamento temporário. Certifique-se de que há espaço em disco suficiente para o maior índice do seu cluster# 🧰 OpenSearch Toolkit - Graylog v5 Ops

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![OpenSearch](https://img.shields.io/badge/OpenSearch-Supported-blue.svg)](https://opensearch.org/)
[![Status](https://img.shields.io/badge/Status-Active-success.svg)]()

**Branch:** `ops/graylog-v5-legacy-backup`

Bem-vindo ao **OpenSearch Toolkit**. Esta branch contém um conjunto especializado de scripts em Python projetados para a manutenção crítica, monitoramento de performance e ciclos de backup de longa retenção para infraestruturas **Graylog v5**.

Diferente das ferramentas de monitoramento puramente visual, esta versão foca na **estabilidade operacional** e na **integridade dos dados**, automatizando o ciclo de vida dos índices e garantindo que backups compactados sejam enviados com segurança para o armazenamento em nuvem (S3).

## ✨ Principais Funcionalidades

*   **Backup Paralelizado por Shard:** Extração nativa de dados do OpenSearch segmentada por shards para maximizar o throughput de rede e reduzir o tempo de exportação.
*   **Compressão Otimizada (pigz):** Integração com `pigz` para compressão multinúcleo de arquivos JSONL, reduzindo drasticamente o consumo de storage temporário.
*   **Monitoramento HTTP Light:** Dashboard de terminal para acompanhamento de métricas vitais como Heap, CPU, Load e contagem de Shards por nó.
*   **Auditoria Integrada ao Graylog:** O processo de backup não é silencioso; ele reporta status, MD5 e duração em tempo real para um Stream de auditoria no próprio Graylog.
*   **Gestão de Backup Inteligente:** Identificação automática de datas de registros para organização cronológica no S3 e detecção de duplicatas para evitar re-uploads desnecessários.

---

## 📂 Estrutura de Arquivos

| Arquivo | Descrição |
| :--- | :--- |
| `backup_to_s3.py` | Script principal de backup. Realiza extração, compressão via `pigz`, cálculo de MD5 e upload multipart para S3[cite: 1]. |
| `opensearch_monitor.py` | Monitor de nós em tempo real. Exibe status de saúde, uso de disco e alocação de shards via HTTP[cite: 2]. |
| `.env.example` | Template de configuração para credenciais S3, endpoints OpenSearch e certificados SSL[cite: 1, 2]. |
| `requirements.txt` | Lista de dependências (boto3, opensearch-py, requests, python-dotenv). |

---

## 🚀 Como Começar

### 1. Pré-requisitos

*   **Python 3.8+** instalado.
*   **pigz** instalado no sistema (essencial para a performance de compressão)[cite: 1].
*   Acesso de rede aos nós do OpenSearch e ao bucket S3 (ou MinIO).
