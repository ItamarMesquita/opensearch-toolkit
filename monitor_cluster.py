import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from colorama import Fore, Style, init

# Inicialização
init(autoreset=True)
load_dotenv()

# Configurações via ENV
OS_HOST = os.getenv('OS_HOST')
AUTH = (os.getenv('OS_USER'), os.getenv('OS_PASS'))
VERIFY = os.getenv('SSL_CA')
CERT = (os.getenv('SSL_CERT'), os.getenv('SSL_KEY'))
DOMAIN = os.getenv('REMOVE_DOMAIN', '')
REFRESH = int(os.getenv('REFRESH_RATE', 2))
WIDTH = int(os.getenv('WIDTH', 80))

def call_os(endpoint):
    try:
        url = f"{OS_HOST}{endpoint}"
        response = requests.get(url, auth=AUTH, verify=VERIFY, cert=CERT, timeout=5)
        return response.json()
    except:
        return None

def show_intro():
    os.system('clear')
    print(Fore.CYAN + r"""
                                                                ..;===+.
                                                            .:=iiiiii=+=
                                                         .=i))=;::+)i=+,
                                                      ,=i);)I)))I):=i=;
                                                   .=i==))))ii)))I:i++
                                                 +)+))iiiiiiii))I=i+:'
                                .,:;;++++++;:,.       )iii+:::;iii))+i='
                             .:;++=iiiiiiiiii=++;.    =::,,,:::=i));=+'
                           ,;+==ii)))))))))))ii==+;,       ,,,:=i))+=:
                         ,;+=ii))))))IIIIII))))ii===;.    ,,:=i)=i+
                        ;+=ii)))IIIIITIIIIII))))iiii=+,   ,:=));=,
                      ,+=i))IIIIIITTTTTITIIIIII)))I)i=+,,:+i)=i+
                     ,+i))IIIIIITTTTTTTTTTTTI))IIII))i=::i))i='
                    ,=i))IIIIITLLTTTTTTTTTTIITTTTIII)+;+i)+i`
                    =i))IIITTLTLTTTTTTTTTIITTLLTTTII+:i)ii:'
                   +i))IITTTLLLTTTTTTTTTTTTLLLTTTT+:i)))=,
                   =))ITTTTTTTTTTTLTTTTTTLLLLLLTi:=)IIiii;
                  .i)IIITTTTTTTTLTTTITLLLLLLLT);=)I)))))i;
                  :))IIITTTTTLTTTTTTLLHLLLLL);=)II)IIIIi=:
                  :i)IIITTTTTTTTTLLLHLLHLL)+=)II)ITTTI)i=
                  .i)IIITTTTITTLLLHHLLLL);=)II)ITTTTII)i+
                  =i)IIIIIITTLLLLLLHLL=:i)II)TTTTTTIII)i'
                +i)i)))IITTLLLLLLLLT=:i)II)TTTTLTTIII)i;
              +ii)i:)IITTLLTLLLLT=;+i)I)ITTTTLTTTII))i;
             =;)i=:,=)ITTTTLTTI=:i))I)TTTLLLTTTTTII)i;
           +i)ii::,  +)IIITI+:+i)I))TTTTLLTTTTTII))=,
         :=;)i=:,,    ,i++::i))I)ITTTTTTTTTTIIII)=+'
       .+ii)i=::,,   ,,::=i)))iIITTTTTTTTIIIII)=+
      ,==)ii=;:,,,,:::=ii)i)iIIIITIIITIIII))i+:'
     +=:))i==;:::;=iii)+)=  `:i)))IIIII)ii+'
   .+=:))iiiiiiii)))+ii;
  .+=;))iiiiii)));ii+
 .+=i:)))))))=+ii+
.;==i+::::=)i=;
,+==iiiiii+,
`+=+++;`
    """)
    print(Style.BRIGHT + "Monitor Gerenciado via Python".center(WIDTH))
    time.sleep(2)

def draw_nodes():
    now = time.strftime('%H:%M:%S')
    print(f"{Fore.BLUE}{'='*WIDTH}")
    print(Style.BRIGHT + f" OPENSEARCH MONITOR | {now} ".center(WIDTH))
    print(f"{Fore.BLUE}{'='*WIDTH}")
    
    # Busca dados
    alloc_raw = call_os('/_cat/allocation?format=json') or []
    shards_map = {item['node']: item['shards'] for item in alloc_raw if item['node'] != 'UNASSIGNED'}
    
    nodes = call_os('/_cat/nodes?h=name,heap.percent,cpu,load_1m,disk.used_percent,master&s=name&format=json') or []
    
    print(f"{Style.BRIGHT}{'NODE':<20} | {'HEAP':<7} | {'CPU':<6} | {'LOAD':<6} | {'DISK':<8} | {'SHDS':<5} | {'MST'}")
    print("-" * WIDTH)

    for n in nodes:
        name = n['name'].replace(DOMAIN, '')
        shards = shards_map.get(n['name'], '0')
        
        # Cores de Alerta
        c_heap = Fore.RED if int(n['heap.percent']) >= 85 else Fore.YELLOW if int(n['heap.percent']) >= 75 else Fore.GREEN
        c_cpu = Fore.RED if int(n['cpu']) >= 85 else Fore.GREEN
        c_disk = Fore.RED if int(n['disk.used_percent']) >= 85 else Fore.YELLOW if int(n['disk.used_percent']) >= 75 else Fore.GREEN
        
        mst = '*' if n['master'] == '*' else '-'
        
        print(f"{name:<20} | {c_heap}{n['heap.percent']:>5}%{Fore.RESET} | {c_cpu}{n['cpu']:>4}%{Fore.RESET} | {n['load_1m']:>6} | {c_disk}{n['disk.used_percent']:>6}%{Fore.RESET} | {shards:>5} | {mst}")

def draw_shards():
    print(f"\n{Fore.BLUE}{'-'*WIDTH}")
    print(Style.BRIGHT + "📦 MOVIMENTAÇÃO DE SHARDS (RELOCATING)".center(WIDTH))
    print(f"{Fore.BLUE}{'-'*WIDTH}")
    
    shards = call_os('/_cat/shards?format=json') or []
    relocating = [s for s in shards if s['state'] == 'RELOCATING']
    
    if not relocating:
        print(f"\n{Fore.GREEN}" + "✅ Cluster estável. Nenhum shard em movimento.".center(WIDTH))
    else:
        print(f"{Fore.YELLOW}{'ÍNDICE':<25} | {'SHD':<3} | {'TIPO':<5} | {'DESTINO'}")
        for s in relocating:
            idx = (s['index'][:22] + '..') if len(s['index']) > 22 else s['index']
            print(f"{idx:<25} | {s['shard']:<3} | {s['prirep']:<5} | {s['node']}")
    print(f"{Fore.BLUE}{'='*WIDTH}")

if __name__ == "__main__":
    show_intro()
    try:
        while True:
            print("\033[H", end="") # Reposiciona cursor no topo sem limpar a tela (evita flickering)
            draw_nodes()
            draw_shards()
            time.sleep(REFRESH)
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Monitor finalizado.")