#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para monitoramento do status dos nós do OpenSearch via HTTP.
"""

import os
import time
import datetime
import sys
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAÇÕES ---
OS_HOST = os.getenv("MONITOR_OS_HOST", "http://localhost:9200")
OS_USER = os.getenv("OS_USER", "admin")
OS_PASSWORD = os.getenv("OS_PASSWORD", "admin")
REFRESH_RATE = int(os.getenv("MONITOR_REFRESH_RATE", 2))
REMOVE_DOMAIN = os.getenv("MONITOR_REMOVE_DOMAIN", ".local")

# --- CORES ---
RED = '\033[1;31m'
GREEN = '\033[1;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[1;34m'
BOLD = '\033[1m'
NC = '\033[0m'

def curl_os(endpoint):
    """Realiza a requisição na API do _cat do OpenSearch."""
    try:
        response = requests.get(
            f"{OS_HOST}{endpoint}", 
            auth=HTTPBasicAuth(OS_USER, OS_PASSWORD), 
            timeout=5
        )
        response.raise_for_status()
        text = response.text.replace(REMOVE_DOMAIN, "")
        return [line.split() for line in text.strip().split('\n') if line.strip()]
    except Exception:
        return []

def draw_header():
    timestamp = datetime.datetime.now().strftime('%H:%M:%S')
    sys.stdout.write(f"{BLUE}================================================================================{NC}\n")
    sys.stdout.write(f"          {BOLD}OPENSEARCH MONITOR (HTTP) | {timestamp}{NC}\n")
    sys.stdout.write(f"{BLUE}================================================================================{NC}\n")

def show_nodes():
    sys.stdout.write(f"                            {BOLD}📡 STATUS DOS NÓS{NC}\n")
    sys.stdout.write(f"{BLUE}--------------------------------------------------------------------------------{NC}\n")
    sys.stdout.write(f"{BOLD}{'NODE':<23} | {'HEAP':<6} | {'CPU':<5} | {'LOAD':<6} | {'DISK':<8} | {'SHD':<4} | {'MST':<3}{NC}\n")
    sys.stdout.write("--------------------------------------------------------------------------------\n")

    alloc_data = curl_os("/_cat/allocation?h=node,shards")
    node_data = curl_os("/_cat/nodes?h=name,heap.percent,cpu,load_1m,disk.used_percent,master&s=name")

    if not node_data:
        sys.stdout.write(f"          {RED}⚠️  Sem resposta do host: {OS_HOST}{NC}\n")
        return

    # Mapeamento de shards por nó
    shards_map = {}
    for row in alloc_data:
        if len(row) >= 2 and row[0] != "UNASSIGNED":
            shards_map[row[0]] = row[1]

    # Processamento e exibição dos nós
    for row in node_data:
        if len(row) >= 6:
            name, heap, cpu, load, disk, master = row
            
            try:
                heap_val = float(heap)
                cpu_val = float(cpu)
                disk_val = float(disk)
            except ValueError:
                heap_val = cpu_val = disk_val = 0.0

            c_heap = RED if heap_val >= 85 else (YELLOW if heap_val >= 75 else GREEN)
            c_cpu = RED if cpu_val >= 85 else GREEN
            c_disk = RED if disk_val >= 85 else (YELLOW if disk_val >= 75 else GREEN)

            shd = shards_map.get(name, "0")
            mst = "*" if master == "*" else "-"

            # Formatação de string idêntica ao bash
            sys.stdout.write(
                f"{name:<23} | "
                f"{c_heap}{heap:>5}%{NC} | "
                f"{c_cpu}{cpu:>4}%{NC} | "
                f"{load:>6} | "
                f"{c_disk}{disk:>7}%{NC} | "
                f"{shd:>4} | {mst:<3}\n"
            )

def main():
    try:
        sys.stdout.write("\033[?25l") # Esconde o cursor
        while True:
            sys.stdout.write("\033[H") # Move o cursor para o topo
            draw_header()
            show_nodes()
            sys.stdout.write(f"{BLUE}================================================================================{NC}\n")
            sys.stdout.write("\033[J") # Limpa o resto da tela
            sys.stdout.flush()
            time.sleep(REFRESH_RATE)
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h") # Restaura o cursor
        sys.stdout.write(f"{NC}\n")

if __name__ == "__main__":
    main()