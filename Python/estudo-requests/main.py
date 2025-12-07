import requests
import pandas as pd
import os

url_teste = 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-2020964060'
response = None
nome_pasta = 'dados_gerais'
nome_arquivo = 'populacao_residente_por_municipio_MA.csv'
caminho_completo = os.path.join(nome_pasta,nome_arquivo)
try:
    os.makedirs(nome_pasta, exist_ok=True)
    with requests.get(url_teste, stream=True) as response:
        # Verifica se o status code é de sucesso
        response.raise_for_status()
        print(f"Status code: {response.status_code} - Requisição bem-sucedida!")
        caminho = os.path.join('dados_gerais')
        # Abre o arquivo localmente para escrita binária ('wb')
        with open(caminho_completo, 'wb') as f:
            # Itera sobre o conteúdo da resposta em blocos para economizar memória
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print("Download concluído!")
                    
except requests.exceptions.ConnectionError:
    # Captura erros de rede (DNS, timeout, etc.)
    print("Error de conexão com a URL.")
except requests.exceptions.HTTPError as e:
    # Captura erros HTTP (400, 404, 500, etc)
    if e.response.status_code == 404:
        print("Url não encontrada (Erro 404).")
    else:
        print(f'Error HTTP: {e.response.status_code}')
except requests.exceptions.RequestException as e:
    print(f'Ocorrreu um erro na requisição: {e}')

except Exception as e:
    print(f'Ocorreu um erro inesperado: {e}')
    
try:
    print(caminho_completo)
   # lista_arquivos = os.listdir(caminho_completo)
    #print(lista_arquivos)
    df = pd.read_csv(caminho_completo, sep=';', skiprows=3, skipfooter=16)
    #print(df)
    #print(df.columns)
    df = df.rename(columns={'2022':'populacao_residente'})
    populacao_residente_lista = df['populacao_residente'].values.astype('int')
    print(populacao_residente_lista)
    print(sum(populacao_residente_lista))
    print(df['populacao_residente'].count())
except FileNotFoundError:
    print("Arquivo não encontrado")
except Exception as e:
    print("Erro: ",e)