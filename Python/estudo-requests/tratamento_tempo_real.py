import pandas as pd 
import os
import auxiliares
from pathlib import Path

LOGGER_TRATAMENTO_GLOBAL = auxiliares.funcoes_auxiliares.configuracao_logger('TRATAMENTO', 'log_tratamento.log')

class ListaVaziaError(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class tratamento_arquivos():
    def __init__(self):    
        self.logger_tratamento = LOGGER_TRATAMENTO_GLOBAL
        self.diretorio_arquivos_brutos = os.path.join('arquivos-brutos')
        os.makedirs('arquivos-limpos',exist_ok=True)
        try:
            self.lista_arquivos_brutos = os.listdir(self.diretorio_arquivos_brutos)
        except FileNotFoundError:
            self.logger_tratamento.critical(f'ERROR CRÍTICO. O diretório {self.diretorio_arquivos_brutos} não foi encontrado. Execute a coleta de dados primeiro.')
            raise
        if not self.lista_arquivos_brutos:
            self.logger_tratamento.critical(f'Error CRÍTICO. O diretório {self.diretorio_arquivos_brutos} está vazio! Nenhum arquivo para processar.')
            raise
        
    @auxiliares.funcoes_auxiliares.tempo_de_execucacao(logger=LOGGER_TRATAMENTO_GLOBAL)
    def tratamento_arquivos_csv_IBGE_SIDRA(self):
        self.logger_tratamento.info('-'*120)
        for arquivo in self.lista_arquivos_brutos:
            if arquivo.endswith('.csv'):
                try:
                    diretorio_arquivo = os.path.join(self.diretorio_arquivos_brutos,arquivo)
                    self.logger_tratamento.info(f"Iniciando a leitura e tratamento do arquivo: {arquivo}.")
                    df = pd.read_csv(diretorio_arquivo, header = 2, skipfooter=12, sep=';',engine='python')

                    self.logger_tratamento.info("Iniciando a remoção das colunas com valores Nulos e formatação do dataset.")
                    coluna_de_texto_principal = df.columns[0]
                    mascara_ibge = df[coluna_de_texto_principal].astype(str).str.contains("IBGE", na=False)
                    try:
                        indice_corte = mascara_ibge.idxmax()
                        if not mascara_ibge.any():
                            self.logger_tratamento.info("A string 'IBGE' não foi encontrada no DataFrame. Nenhuma linha foi removida.")
                            df_final = df.copy()
                        else:
                            df_final = df.iloc[:indice_corte].copy()
                            self.logger_tratamento.info(f"SUCESSO: Linha '{indice_corte}' ('Fonte: IBGE') e as subsequentes foram removidas.")
                    except Exception as e:
                        self.logger_tratamento.error(f"Erro ao tentar localizar o índice de corte: {e}. Mantendo o DataFrame original.")
                        df_final = df.copy()

                    palavras_na_coluna = ['forma de declaração da idade','unidade da federação e município','município']
                    for coluna in df.columns:
                        if coluna.lower() in palavras_na_coluna:
                            df_final = df_final.drop(coluna, axis = 1)

                    df_final['Cód.'] = df_final['Cód.'].astype(str).str.strip()
                    mascara_municipio = (df_final['Cód.'].str.len() == 7)

                    try:
                        primeiro_rotulo_indice = mascara_municipio.idxmax()
                        posicao_corte = df_final.index.get_loc(primeiro_rotulo_indice)
                        if mascara_municipio.any():
                            df_final = df_final.iloc[posicao_corte:].copy()
                            df_final.reset_index(drop=True, inplace=True)
                            codigo_encontrado = df_final.iloc[0]['Cód.']
                            self.logger_tratamento.info(f"SUCESSO: Linhas descartadas antes do primeiro município. Novo DataFrame começa com o Cód. '{codigo_encontrado}'.")
                        else:
                            self.logger_tratamento.info("Atenção: Nenhum código de município de 7 dígitos encontrado. O DataFrame foi mantido intacto.")
                    except Exception as e:
                        self.logger_tratamento.error(f"Erro ao tentar encontrar o índice do primeiro município: {e}. O DataFrame foi mantido intacto.")
                        
                    self.logger_tratamento.info("Finalização do processo de remoção das colunas com valores Nulos e formatação do dataset.")

                    self.logger_tratamento.info("Iniciando a conversão de tipos. Buscando colunas 'ano' ou 'idade' para 'Int64'.")
                    
                    colunas_convertidas = 0
                    for coluna in df_final.columns:
                        palavras_na_coluna = coluna.lower().split(sep=' ')
                        if 'ano' in palavras_na_coluna or 'idade' in palavras_na_coluna:
                            df_final[coluna] = pd.to_numeric(df_final[coluna], errors='coerce')
                            df_final.dropna(subset=[coluna], inplace=True)
                            dtype_anterior = df_final[coluna].dtype
                            try:
                                df_final[coluna] = df_final[coluna].astype('Int64')
                                self.logger_tratamento.info(
                                f"SUCESSO: Coluna '{coluna}' (tipo anterior: {dtype_anterior}) convertida para 'Int64' (suporte a nulos)."
                                )
                                colunas_convertidas += 1
                            except Exception as e:
                                self.logger_tratamento.error(
                                    f"FALHA: Não foi possível converter '{coluna}' para Int64. Erro: {e}"
                                )
                    self.logger_tratamento.info(
                        f"Fim da verificação de tipo. Total de {colunas_convertidas} coluna(s) convertida(s)."
                    )
                    
                    df_final = df_final.rename(columns={df_final.columns[-1] : arquivo.replace('.csv','')})
                    self.logger_tratamento.info(f"Finalização da leitura e tratamento do arquivo: {arquivo}.")

                    nome_arquivo_formatado_e_limpo = arquivo.replace('.csv','_limpo')
                    diretorio_salvamento = Path(f'arquivos-limpos/{nome_arquivo_formatado_e_limpo}.csv')
                    df_final.to_csv(path_or_buf=diretorio_salvamento, sep=';', index=False)
                    self.logger_tratamento.info(f"Arquivo {nome_arquivo_formatado_e_limpo} salvo com sucesso na pasta: arquivos-limpos")
                    self.logger_tratamento.info('-'*120)
                except pd.errors.ParserError as e:
                    self.logger_tratamento.critical(f'ERROR CRÍTICO. Falha ao acessar o arquivo: {arquivo}. Causa: Formatação nos parâmetros do Pandas. {e}')
                    
    @auxiliares.funcoes_auxiliares.tempo_de_execucacao(logger=LOGGER_TRATAMENTO_GLOBAL)
    def tratamento_arquivo_xlsx_INEP(self):
        for arquivo in self.lista_arquivos_brutos:
            if arquivo.endswith('.xlsx'):
                try:
                    diretorio_arquivo = os.path.join(self.diretorio_arquivos_brutos,arquivo)
                    df = pd.read_excel(diretorio_arquivo)
                    print(df.head(5))
                    print(df.columns)
                except pd.errors.ParserError as e:
                    self.logger_tratamento.critical(f'ERROR CRÍTICO. Falha ao acessar o arquivo: {arquivo}. Causa: Formatação nos parâmetros do Pandas. {e}')
if __name__ == '__main__':
    #tratamento_arquivos().tratamento_arquivos_csv_IBGE_SIDRA()
    tratamento_arquivos().tratamento_arquivos_xlsx_INEP()

