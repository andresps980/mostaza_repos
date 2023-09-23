import json
import csv
import os
from datetime import datetime, timedelta
from mostaza_repos.types_repo.types_repo import Pase, FilaRepo, Repo


def clave_valor(cadena):
    lista = cadena.split("=")
    if len(lista) != 2:
        return ""
    else:
        return lista[1]


def get_dia_hora(cadena):
    cadena_splitted = cadena.split()
    dia = cadena_splitted[0]
    hora = cadena_splitted[1]
    return dia, hora


def get_dia_hora_listas(cadena):
    dia_hora = get_dia_hora(cadena)
    dia_split = dia_hora[0].split('-')
    hora_split = dia_hora[1].split(':')
    return dia_split, hora_split


def lectura_configuracion(repo):
    repo_obj = Repo()
    repo_obj.nombre_repo = repo['nombreRepo']
    repo_obj.campaign_id = repo['CampaignID']
    repo_obj.tipo_repo = int(repo['repos'])
    repo_obj.cad_fecha_comienzo = repo['fechaComienzo']
    repo_obj.cad_fecha_fin = repo['fechaFin']

    cad_fecha_inicio = get_dia_hora_listas(repo['fechaComienzo'])
    cad_fecha_fin = get_dia_hora_listas(repo['fechaFin'])

    repo_obj.fecha_comienzo = datetime(int(cad_fecha_inicio[0][0]),
                                       int(cad_fecha_inicio[0][1]),
                                       int(cad_fecha_inicio[0][2]),
                                       int(cad_fecha_inicio[1][0]),
                                       int(cad_fecha_inicio[1][1]),
                                       int(cad_fecha_inicio[1][2]),
                                       00000)

    repo_obj.fecha_fin = datetime(int(cad_fecha_fin[0][0]),
                                  int(cad_fecha_fin[0][1]),
                                  int(cad_fecha_fin[0][2]),
                                  int(cad_fecha_fin[1][0]),
                                  int(cad_fecha_fin[1][1]),
                                  int(cad_fecha_fin[1][2]),
                                  00000)

    lista_dias_pases = repo['pases_dia']
    for dia in lista_dias_pases:
        dia_pase = dia['dia']

        lista_pases = dia['pases']
        lista_pases_final = []
        for pase in lista_pases:
            pase_final = Pase()
            pase_final.orden = pase['orden']
            pase_final.cad_fecha_comienzo = dia_pase + " " + pase['comienzo']
            pase_final.cad_fecha_fin = dia_pase + " " + pase['fin']

            cad_fecha_inicio = get_dia_hora_listas(pase_final.cad_fecha_comienzo)
            cad_fecha_fin = get_dia_hora_listas(pase_final.cad_fecha_fin)

            pase_final.fecha_comienzo = datetime(int(cad_fecha_inicio[0][0]),
                                                 int(cad_fecha_inicio[0][1]),
                                                 int(cad_fecha_inicio[0][2]),
                                                 int(cad_fecha_inicio[1][0]),
                                                 int(cad_fecha_inicio[1][1]),
                                                 int(cad_fecha_inicio[1][2]),
                                                 00000)

            pase_final.fecha_fin = datetime(int(cad_fecha_fin[0][0]),
                                            int(cad_fecha_fin[0][1]),
                                            int(cad_fecha_fin[0][2]),
                                            int(cad_fecha_fin[1][0]),
                                            int(cad_fecha_fin[1][1]),
                                            int(cad_fecha_fin[1][2]),
                                            00000)
            lista_pases_final.append(pase_final)

        file_repo_obj = FilaRepo()
        file_repo_obj.pases = lista_pases_final
        repo_obj.filas[dia_pase] = file_repo_obj
        if repo_obj.max_pases < len(lista_pases_final):
            repo_obj.max_pases = len(lista_pases_final)

    return repo_obj


def encuentra_pase(repo_obj, new_date, dia):
    filas_repo = repo_obj.filas.get(dia)

    if filas_repo is None:
        return None

    for pase in filas_repo.pases:
        if pase.fecha_comienzo <= new_date <= pase.fecha_fin:
            return pase


def extrae_datos(linea):
    linea_info_split = linea.split('&')
    timestamp = ""
    user_id = ""
    campaign_id = ""
    for cad in linea_info_split:
        if 'timestamp' in cad:
            timestamp = int(clave_valor(cad))
        elif 'user_id' in cad:
            user_id = clave_valor(cad)
        elif 'CampaignID' in cad:
            campaign_id = clave_valor(cad)

    return timestamp, user_id, campaign_id


def procesa_repo(repo_obj, args, logger):
    # TODO meter map-reduce
    if repo_obj.tipo_repo == 1:
        # Campos interesantes en lineas de logs, campo 11?:
        #   Fecha inicial
        #   Time Stamp
        #   CampaignID
        #   messageType
        #   user_id
        logs_dir = args.DirLogs
        contenido = os.listdir(logs_dir)
        cont = 1
        for fichero in contenido:
            logger.info(f'Procesando fichero {fichero} numero {cont} de {len(contenido)}')
            cont += 1

            if os.path.isfile(os.path.join(logs_dir, fichero)) and fichero.endswith('.log'):
                with open(logs_dir + fichero, 'r') as fichero_log:
                    cont_linea = 0
                    for linea in fichero_log.readlines():
                        cont_linea += 1
                        # Filtro 1: Sino es este tipo de mensaje no lo procesamos
                        if 'messageType=2' in linea:
                            linea_splitted = linea.split()

                            # Fechas
                            dia = linea_splitted[0]
                            hora = linea_splitted[1]
                            dia_split = dia.split('-')
                            hora_split = hora.split(':')

                            try:
                                new_date = datetime(int(dia_split[0]),
                                                    int(dia_split[1]),
                                                    int(dia_split[2]),
                                                    int(hora_split[0]),
                                                    int(hora_split[1]),
                                                    int(hora_split[2]),
                                                    00000)

                                # Filtro 2: Si la traza no esta en el periodo de validez de la campaña no la procesamos
                                if new_date < repo_obj.fecha_comienzo or new_date > repo_obj.fecha_fin:
                                    logger.debug(f'Linea no procesada, filtro 2, numero linea:  {cont_linea}')
                                    continue

                                # Filtro 3: Si la traza no esta dentro del periodo de validez de algun pase no la
                                # procesamos
                                pase = encuentra_pase(repo_obj, new_date, dia)
                                if pase is None:
                                    logger.debug(f'Linea no procesada, filtro 3, numero linea:  {cont_linea}')
                                    continue

                                linea_info = ""
                                for cad in linea_splitted:
                                    # Filtro 4: Solo nos interesa este componente de la traza
                                    if 'messageType=2' in cad:
                                        linea_info = cad
                                        break

                                if len(linea_info) > 0:
                                    # Buscando el campo con la informacion
                                    timestamp, user_id, campaign_id = extrae_datos(linea_info)

                                    if timestamp != "" and user_id != "" and campaign_id != "":
                                        # Filtro 5: Solo nos interesa si la traza pertenece a la campaña del informe
                                        if int(campaign_id) == repo_obj.campaign_id:
                                            pase.addImpactosDia(1)
                                            if pase.usuarios.get(user_id) is None:
                                                pase.usuarios[user_id] = 1
                                            else:
                                                pase.usuarios[user_id] += 1
                                        # else:
                                        #     print("Linea no procesada!!!")
                                        #     print(linea_info)

                            except Exception as excep:
                                logger.error(f"Exception procesando logs, linea {cont_linea}", exc_info=True)

                        # else:
                        #    print("Linea no procesada!!!")
                        #    print(linea)

    else:
        logger.info("Error, Version 1 solo procesamos repos tipo 1. nombre repo no valido: " + repo_obj.nombre_repo)


def hacer_repo(repo_obj, args, logger):
    now = datetime.now()

    lista_titulos = ["FECHA", "HORA INICIAL", "impactos_dia", "impactos_acum", "usuarios_dia", "usuarios_dia_dif",
                     "unicos_acum"]
    lista_titulos_final = lista_titulos * repo_obj.max_pases
    lista_pases_temp = ["", "", "", "", "", ""]
    lista_pases = [""]
    datos_prev_pases = {}
    for count in range(repo_obj.max_pases):
        lista_pases.append("Pase " + str(count + 1))
        lista_pases += lista_pases_temp
        datos_prev_pases[count + 1] = None

    ubicacion_repo_final = args.OutputDir + now.strftime("%d-%m-%Y_%H-%M-%S_") + repo_obj.nombre_repo
    with open(ubicacion_repo_final, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(lista_pases)
        writer.writerow(lista_titulos_final)

        # Ahora añadimos los datos de cada dia por fila y pase...
        temp_date = repo_obj.fecha_comienzo
        while temp_date <= repo_obj.fecha_fin:
            dia_ini = temp_date.strftime("%Y-%m-%d")
            datos = repo_obj.filas.get(dia_ini)
            fila_datos = []
            if datos is None:
                # datos_prev_pases sera el anterior guardado para cada pase...
                for count in range(repo_obj.max_pases):
                    dato = datos_prev_pases.get(count + 1)
                    rellenar_pase(dato, dia_ini, fila_datos)
                writer.writerow(fila_datos)
            else:
                orden_actual = 0
                for dato in datos.pases:
                    orden_actual += 1
                    # Comprobamos si hay que saltar pases antes de rellenar este
                    for index_pase in range(orden_actual, dato.orden):
                        orden_actual += 1
                        rellenar_pase(datos_prev_pases.get(index_pase), dia_ini, fila_datos)

                    # Dia del repo
                    fila_datos.append(dia_ini)
                    # Hora del pase
                    fila_datos.append(dato.fecha_comienzo.strftime("%H-%M-%S"))
                    existe_prev = datos_prev_pases.get(dato.orden)
                    # impactos_dia
                    fila_datos.append(str(dato.impactos_dia))
                    # impactos_acum
                    if not existe_prev is None:
                        dato.impactos_acumulados = existe_prev.impactos_acumulados + dato.impactos_dia
                        fila_datos.append(str(dato.impactos_acumulados))
                    else:
                        fila_datos.append(str(dato.impactos_dia))
                        dato.impactos_acumulados = dato.impactos_dia

                    # usuarios_dia
                    fila_datos.append(str(len(dato.usuarios)))
                    # usuarios_dia_dif
                    if not existe_prev is None:
                        list_actual = dato.usuarios.keys()
                        list_anterior = existe_prev.usuarios.keys()
                        difference = list(set(list_actual) - set(list_anterior))
                        dato.usuarios_dia_dif = len(difference)
                        fila_datos.append(str(dato.usuarios_dia_dif))
                    else:
                        fila_datos.append(str(len(dato.usuarios)))
                        dato.usuarios_dia_dif = len(dato.usuarios)

                    # unicos_acum
                    if not existe_prev is None:
                        dato.unicos_acumulados = existe_prev.unicos_acumulados + dato.usuarios_dia_dif
                        fila_datos.append(str(dato.unicos_acumulados))
                    else:
                        fila_datos.append(str(dato.usuarios_dia_dif))
                        dato.unicos_acumulados = dato.usuarios_dia_dif

                    # Actualizamos el dato para los siguientes acumulados
                    datos_prev_pases[dato.orden] = dato

                # Rellenamos los pases que no existen en la fila actual con los datos previos
                for index_pase in range(orden_actual + 1, repo_obj.max_pases + 1):
                    rellenar_pase(datos_prev_pases.get(index_pase), dia_ini, fila_datos)

                writer.writerow(fila_datos)

            temp_date += timedelta(days=1)


def rellenar_pase(dato, dia_ini, fila_datos):
    if dato is None:
        rellenar_pase_vacio(dia_ini, fila_datos)
    else:
        rellenar_pase_prev(dato, dia_ini, fila_datos)


def rellenar_pase_prev(dato, dia_ini, fila_datos):
    # Dia del repo
    fila_datos.append(dia_ini)
    # No hay hora del pase
    fila_datos.append("N/A")
    # impactos_dia
    fila_datos.append(str(0))
    # impactos_acum
    fila_datos.append(str(dato.impactos_acumulados))
    # usuarios_dia
    fila_datos.append(str(0))
    # usuarios_dia_dif
    fila_datos.append(str(0))
    # unicos_acum
    fila_datos.append(str(dato.unicos_acumulados))


def rellenar_pase_vacio(dia_ini, fila_datos):
    # Dia del repo
    fila_datos.append(dia_ini)
    # No hay hora del pase
    fila_datos.append("N/A")
    # impactos_dia
    fila_datos.append(str(0))
    # impactos_acum
    fila_datos.append(str(0))
    # usuarios_dia
    fila_datos.append(str(0))
    # usuarios_dia_dif
    fila_datos.append(str(0))
    # unicos_acum
    fila_datos.append(str(0))


def cargar_repos(args, logger):
    with open("config/config.json") as file:
        dic = json.load(file)

    list_repos = dic['repos']
    config_repos = []

    try:
        for repo in list_repos:
            logger.info('\tCREANDO CONFIG REPO')
            logger.info('\tNombre repo: ' + repo['nombreRepo'])
            logger.info('\tCampaña: ' + str(repo['CampaignID']))

            repo_obj = lectura_configuracion(repo)
            config_repos.append(repo_obj)

    except Exception as e:
        logger.error("Exception tratando la configuración", exc_info=True)
    finally:
        logger.info('Fin lectura configuración')
        return config_repos


def procesa_repos(config_repos, args, logger):
    # TODO Cambiar esto para lanzar cada repo en un proceso...
    for repo_obj in config_repos:
        try:
            procesa_repo(repo_obj, args, logger)
            hacer_repo(repo_obj, args, logger)
        except Exception as e:
            logger.error(f"Error procesando repo {repo_obj.nombre_repo}", exc_info=True)
        finally:
            logger.info(f"Fin procesando repo {repo_obj.nombre_repo}")
