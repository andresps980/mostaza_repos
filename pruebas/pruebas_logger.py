import logging
import argparse


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


def configura_logs():
    logger = logging.getLogger(__name__)

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
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    logger.setLevel(dame_nivel_log(args.LogLevel))

    return logger


if __name__ == '__main__':

    parser_arg = argumentos_validos()
    args = parser_arg.parse_args()
    print(args)

    logger = configura_logs()

    logger.info('')
    logger.info('')
    logger.info('----------------------------------------------------------')
    logger.info(' \t Comienza procesamiento de logs para mostaza repos')
    logger.info('----------------------------------------------------------')

    name = 'FEDERICO'
    logger.debug(f'This is a debug message {name}')
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')
    logger.critical('This is a critical message')

    a = 5
    b = 0

    try:
        c = a / b
    except Exception as e:
        logger.error("Exception occurred", exc_info=True)

    try:
        c = a / b
    except Exception as e:
        logger.exception("Exception occurred")
