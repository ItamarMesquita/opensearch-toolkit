# 🧰 OpenSearch Toolkit - Graylog v6 Monitoring

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![OpenSearch](https://img.shields.io/badge/OpenSearch-Supported-blue.svg)](https://opensearch.org/)
[![Status](https://img.shields.io/badge/Status-Active-success.svg)]()

> **Branch:** `ops/monitoring-graylog-v6`

Bem-vindo ao **OpenSearch Toolkit**. Esta branch contém um conjunto de ferramentas e scripts em Python projetados para facilitar a administração, o monitoramento visual em tempo real e a geração de relatórios de clusters OpenSearch (especialmente focados em infraestruturas integradas ao Graylog v6).

Nesta versão (`ops/monitoring-graylog-v6`), todos os scripts bash legados foram totalmente refatorados para **Python**, garantindo maior portabilidade, processamento robusto de dados nativo e segurança aprimorada através da ocultação de credenciais via variáveis de ambiente (`.env`).

---

## ✨ Principais Funcionalidades

*   **Monitoramento em Tempo Real (Flicker-Free):** Interface de terminal via `colorama` com atualização limpa. Acompanhe a saúde do cluster, consumo de Heap/CPU/Disco e movimentação de shards (estado `RELOCATING`).
*   **Segurança por Design:** Nenhuma credencial, IP ou caminho de certificado fica hardcoded no código. Tudo é gerenciado externamente por um arquivo `.env`.
*   **Geração de Relatórios (CSV e Excel):** 
    *   Extração de ocupação de disco por nó.
    *   Agrupamento de índices (agrupamento dinâmico via Regex) com sumarização de tamanho e saúde exportados diretamente para arquivos `.xlsx`.
*   **Totalmente Genérico:** Adaptável a qualquer ambiente OpenSearch ou Elasticsearch que suporte a API `_cat`.

---

## 📂 Estrutura de Arquivos

| Arquivo | Descrição |
| :--- | :--- |
| `monitor_cluster.py` | Dashboard principal interativo no terminal. Exibe a arte ASCII, status dos nós e realocação de shards. |
| `gerar_disco.py` | Exporta a alocação de disco de todos os nós do cluster para formato CSV. |
| `agrupar_indices.py` | Lê todos os índices, agrupa por prefixo e gera um relatório consolidado em planilhas do Excel. |
| `.env.example` | Template de configuração das variáveis de ambiente necessárias. |
| `requirements.txt` | Lista de dependências Python. |

---

## 🚀 Como Começar

### 1. Pré-requisitos

Certifique-se de ter o Python 3.8+ instalado em seu ambiente. Você precisará dos certificados SSL gerados pelo seu cluster (CA, Cert e Key) para autenticação segura.
