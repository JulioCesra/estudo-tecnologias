import logging
import time
from functools import wraps

class funcoes_auxiliares():
    def __init__(self):
        pass

    @staticmethod
    def tempo_de_execucacao(logger):
        def decorador(funcao):
            @wraps(funcao) 
            def wrapper(*args, **kwargs):
                logger.info(f'Iniciando a função {funcao.__name__}...')
                tempo_inicio = time.time()
                resultado = funcao(*args, **kwargs)
                tempo_final = time.time()
                tempo_decorrido = tempo_final - tempo_inicio
                log_msg = f'Tempo de execução da função {funcao.__name__}: {tempo_decorrido:.2f} segundos'
                logger.info(log_msg)
                return resultado
            return wrapper
        return decorador 

    @staticmethod
    def configuracao_logger(nome_logger, nome_arquivo):
        FORMATO_LOGGER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        LEVEL = logging.INFO
        logger = logging.getLogger(nome_logger)
        logger.setLevel(LEVEL)
        logger.propagate = False
        file_handler = logging.FileHandler(nome_arquivo, mode='a')
        file_handler.setFormatter(FORMATO_LOGGER)
        logger.addHandler(file_handler)
        return logger