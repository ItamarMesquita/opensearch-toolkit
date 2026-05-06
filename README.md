# OpenSearch Index Manager 🚀

Script Python eficiente para listagem e remoção em massa de índices no OpenSearch com base em prefixos. Ideal para manutenção de clusters e limpeza automatizada de logs.

## 📋 Sobre o Projeto

Este script foi desenvolvido para oferecer uma interface de linha de comando (CLI) simples que permite interagir com o OpenSearch. Ele foca na segurança, utilizando variáveis de ambiente para gerir credenciais e certificados, evitando a exposição de dados sensíveis no código-fonte.

## ✨ Funcionalidades

- **Listagem por Prefixo:** Filtre índices específicos antes de qualquer ação.
- **Modo de Simulação (Dry Run):** Por padrão, o script apenas lista os índices. A eliminação só ocorre com uma flag explícita.
- **Suporte SSL/TLS:** Configurado para aceitar certificados CA, certificados de cliente e chaves privadas.
- **Logs de Auditoria:** Cada remoção bem-sucedida ou falha é registada num ficheiro CSV com carimbo de data/hora (timestamp).
- **Flexibilidade de Host:** Defina os hosts via ficheiro `.env` ou sobreponha-os via linha de comando.

## 🛠️ Pré-requisitos

Antes de começar, certifique-se de que tem o Python 3.8+ instalado e as bibliotecas necessárias:

```bash
pip install opensearch-py python-dotenv