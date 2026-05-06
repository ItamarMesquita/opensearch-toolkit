import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def exportar_alocacao_disco():
    url = f"{os.getenv('OS_HOST_PRINCIPAL')}/_cat/allocation?s=disk.avail:desc&format=json"
    
    # Configuração de segurança e autenticação genérica
    auth = (os.getenv('OS_USER'), os.getenv('OS_PASS'))
    verify = os.getenv('SSL_CA_PATH')
    cert = (os.getenv('SSL_CERT_PATH'), os.getenv('SSL_KEY_PATH'))

    try:
        response = requests.get(url, auth=auth, verify=verify, cert=cert)
        response.raise_for_status()
        
        df = pd.DataFrame(response.json())
        
        os.makedirs(os.getenv('OUTPUT_DIR'), exist_ok=True)
        caminho_saida = os.path.join(os.getenv('OUTPUT_DIR'), "alocacao_disco.csv")
        
        df.to_csv(caminho_saida, index=False, sep=';')
        print(f"✅ Relatório de disco gerado: {caminho_saida}")
        
    except Exception as e:
        print(f"❌ Erro ao processar alocação: {e}")

if __name__ == "__main__":
    exportar_alocacao_disco()