import os
import requests
import auxiliares

LOGGER_COLETA_GLOBAL = auxiliares.funcoes_auxiliares.configuracao_logger('COLETA', 'log_coleta.log')

urls = {   # Nomes dos arquivos brutos                      Urls
        'população_residente_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1420806406',
        'quantidade_de_homens_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1452430117',
        'quantidade_de_mulheres_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-2081871684',
        'quantidade_de_residentes_urbanos_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/651000649',
        'quantidade_de_residentes_rurais_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/859488008',
        'percentual_de_alunos_alfabetizados_por_municipio.xlsx' : 'https://download.inep.gov.br/alfabetiza_brasil/resultados_e_metas_municipios_2024.xlsx',
        'percentual_de_envelhecimento_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1815440151',
        'idade_mediana_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/71360697',
        'area_territorial_do_municipio_em_km2.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-1946607256',
        'densidade_demografica_por_municipio_em_km2.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/1382453315',
        'populacao_residente_em_favelas_e_comunidades_urbanas_por_municipio.csv' : 'https://sidra.ibge.gov.br/geratabela?format=br.csv&name=tabela9884.csv&terr=NC&rank=-&query=t/9884/n1/all/n6/2108603,2107506,2111201,2111300,2112506,2109908,2103000,2105401,2112209,2105302,2114007,2100055,2102325,2109452/v/allxp/p/all/c86/95251/c2/6794/c58/95253/l/v,p%2Bc86%2Bc2,t%2Bc58',
        'pessoas_de_2_anos_ou_mais_com_deficiencia_por_municipios.csv' : 'https://sidra.ibge.gov.br/geratabela/DownloadSelecaoComplexa/-2107715763',
        'indice_de_desenvolvimento_humano_por_municipio.xlsx' : 'https://www.ipeadata.gov.br/ExibeSerieR.aspx?oper=exportCSVBr&serid=40037&MINDATA=2010&MAXDATA=2010&TNIVID=5&TPAID=21'
        
        #l'area_territorial_municipios.ods' : 'https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/areas_territoriais/2024/AR_BR_RG_UF_RGINT_RGI_MUN_2024.ods'
    }

@auxiliares.funcoes_auxiliares.tempo_de_execucacao(logger=LOGGER_COLETA_GLOBAL)
def coleta_e_salvamento():
    resposta_servidor = None
    try:
        os.makedirs('arquivos-brutos', exist_ok=True)
        for nome_arquivo, url in urls.items():
            try:
                diretorio_arquivo = os.path.join('arquivos-brutos',nome_arquivo)
                with requests.get(url, stream=True) as resposta_servidor:
                    resposta_servidor.raise_for_status()
                    LOGGER_COLETA_GLOBAL.info(f"Iniciando download de '{nome_arquivo}' (Status: {resposta_servidor.status_code}).")
                    with open(diretorio_arquivo, 'wb') as arquivo:
                        for parte_arquivo in resposta_servidor.iter_content(8192):
                            if parte_arquivo:
                                arquivo.write(parte_arquivo)
                    LOGGER_COLETA_GLOBAL.info(f"Download e salvamento de '{nome_arquivo}' concluídos com sucesso em '{diretorio_arquivo}'.")
            except requests.exceptions.RequestException as req_err:
                LOGGER_COLETA_GLOBAL.error(f"Erro ao processar '{nome_arquivo}' de {url}: {req_err}")
            except requests.exceptions.ConnectionError:
                LOGGER_COLETA_GLOBAL.error(f'Error de conexão com a URL')
            except requests.exceptions.HTTPError as error:
                if error.response.status_code == 404:
                    LOGGER_COLETA_GLOBAL.error(f"Não foi possível encontrar a URL: {url}. Error (404).")
                else:
                    LOGGER_COLETA_GLOBAL.error(f"Error: {error}.")
            except Exception as e:
                LOGGER_COLETA_GLOBAL.error(f"Erro genérico ao lidar com '{nome_arquivo}': {e}.")
    except Exception as e:
        LOGGER_COLETA_GLOBAL.critical(f"Erro CRÍTICO na fase inicial de coleta ou criação de diretório: {e}")

if __name__ == '__main__':
    coleta_e_salvamento()