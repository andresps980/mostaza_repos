import os
import requests
import shutil

if __name__ == '__main__':

    try:
        directorio = "./logs/"
        shutil.rmtree(directorio)
        os.mkdir(directorio)
    except FileNotFoundError:
        os.mkdir(directorio)

    with open("logsandres_pixel-logs.txt", 'r') as fichero_log:
        urls = fichero_log.readlines()
        for url in urls:
            url = url.rstrip()
            if len(url) > 0:
                try:
                    lista_temp = url.split("/")
                    nombre_archivo = lista_temp[len(lista_temp) - 1]
                    response = requests.request("GET", url)
                    with open(directorio + nombre_archivo, 'w') as fichero_final_log:
                        fichero_final_log.write(response.text)
                except Exception as e:
                    print(f'Excepcion en archivo {nombre_archivo} mensaje {e}')

