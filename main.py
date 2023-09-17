import logging
import argparse
from utils.utils import cargar_repos, procesa_repo, hacer_repo, procesa_repos


def argumentos_validos():
    parser = argparse.ArgumentParser()

    parser.add_argument("-ll", "--LogLevel", help="Nivel de log", default="INFO")
    parser.add_argument("-dl", "--DirLogs", help="Directorio donde se ubicaran los logs", default="./logs/")

    return parser


def dame_nivel_log(level):
    niveles = {'DEBUG': logging.DEBUG,
               'INFO': logging.INFO,
               'WARNING': logging.WARNING,
               'ERROR': logging.ERROR,
               'CRITICAL': logging.CRITICAL}
    return niveles[level]


def configura_logs(args):
    logger_repos = logging.getLogger(__name__)

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('file.log', 'a')
    c_handler.setLevel(dame_nivel_log(args.LogLevel))
    f_handler.setLevel(dame_nivel_log(args.LogLevel))

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger_repos.addHandler(c_handler)
    logger_repos.addHandler(f_handler)
    logger_repos.setLevel(dame_nivel_log(args.LogLevel))

    return logger_repos


def print_cabecera():
    logger.info('')
    logger.info('')
    logger.info('----------------------------------------------------------')
    logger.info(' \t Comienza procesamiento de logs para mostaza repos')
    logger.info('----------------------------------------------------------')


if __name__ == '__main__':
    parser_arg = argumentos_validos()
    args = parser_arg.parse_args()
    print(args)

    logger = configura_logs(args)
    print_cabecera()

    config_repos = cargar_repos(args, logger)

    procesa_repos(config_repos, args, logger)
