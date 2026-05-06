import os
import re
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def agrupar_indices():
    url = f"{os.getenv('OS_HOST_SECUNDARIO')}/_cat/indices?format=json&h=index,status,health,store.size"
    auth = (os.getenv('OS_USER'), os.getenv('OS_PASS'))
    cert = (os.getenv('SSL_CERT_PATH'), os.getenv('SSL_KEY_PATH'))
    verify = os.getenv('SSL_CA_PATH')

    try:
        response = requests.get(url, auth=auth, verify=verify, cert=cert)
        df = pd.DataFrame(response.json())

        # Lógica de agrupamento genérica (remove sufixos de data ou números)
        df['grupo'] = df['index'].apply(lambda x: re.split(r'(_\d{4}\.\d{2}\.\d{2}|__\d+|_?\d+)$', str(x))[0])

        output_file = os.path.join(os.getenv('OUTPUT_DIR'), "relatorio_indices_agrupados.xlsx")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for nome_grupo, dados in df.groupby('grupo'):
                # Limita nome da aba para compatibilidade com Excel
                nome_aba = str(nome_grupo)[:31]
                dados.to_excel(writer, sheet_name=nome_aba, index=False)
        
        print(f"✅ Relatório Excel consolidado em: {output_file}")

    except Exception as e:
        print(f"❌ Erro ao gerar XLSX: {e}")

if __name__ == "__main__":
    agrupar_indices()