# ------------------------------------------------------------------------------------------------
#   PROYECTO:       Requerimiento Chapter de data, data rangers
#   AUTOR:          Jhonatan Rodríguez
#   CREACION:       29 abril 2024 - 01 mayo 2024
#   DESCRIPCION:    Script encargado de realizar la integración, procesamiento y consulta de
#                   información en formato CSV, por medio de cargas en micro batches, teniendo 
#                   en cuenta que la memoria se ocupe solo cuando se ejecuta cada micro batch.
#
# ------------------------------------------------------------------------------------------------
#
#   OBSERVACIONES E INSTRUCCIONES:
#
#   1. El nombramiento de los archivos tienen la siguiente estructura "2012-2" año - hora.    
#   2. El proceso de lectura de cada archivo se hace por separado no todos al mismo tiempo para 
#      no ocupar espacio en memoria
#       - No cargar el archivo validations.csv
#       - Proceso por archivo
#   3. Almacenamiento en Postgresql
#   4. Hacer serguimiento: recuento, valor medio, mínimo, máximo en el campo price
#       - No hacerlo sobre consultas a la base de datos
#   5. Instalar las librerías necesarias para su ejecución
#   6. El script de la base de datos está en un archivo del proyecto llamado script.sql el cual debe
#      de ejecutarse o crearse en un motor de base de datos PostgreSQL antes de ejecutar el proyecto.
#
# ------------------------------------------------------------------------------------------------


# ---------------------------------- IMPORTACIÓN DE LIBRERIAS ------------------------------------
import os                                   # Permite interactuar con el Sistema Operativo
import pandas as pd                         # Permite el manejo, análisis y procesamiento de datos
from datetime import datetime               # Proporciona clases para manipular fechas y horas
import logging                              # Define funciones y clases para implementar un sistema logging
import configparser                         # Permite la lectura de las variables del Config.ini
import argparse                             # Permite la lectura de argumentos enviados al llamarse el main.py
import psycopg2                             # Librería que permite la conexión a la base de datos Postgresql
from sqlalchemy import create_engine        # Permite interactuar con bases de datos relacionales
import time                                 # Permite utilizar funciones relacionadas con el tiempo
import psutil                               # Permite utilizar las funciones y métodos del sistema
# -------------------------------- FIN IMPORTACIÓN DE LIBRERIAS ----------------------------------


# ------------------------------------------ VARIABLES -------------------------------------------
fecha = datetime.now().strftime("%Y-%m-%d")                # Fecha actual

#Lectura de archivo config.ini
#-----------------------------#
config = configparser.RawConfigParser()   
configFilePath = r'config.ini'
config.read(configFilePath)
args_var = config["variables"]
data = args_var['data']
args_conn = config["conexion"]
global user
user = args_conn['username']
global passw
passw = args_conn['password']
global host
host = args_conn['hostname']
global port
port = args_conn['puerto']
global dbname
dbname = args_conn['dbname']
global tabla_prices
tabla_prices = args_conn['tabla_prices']
global tabla_seguimiento
tabla_seguimiento = args_conn['tabla_seguimiento']
global tabla_monitoreo
tabla_monitoreo = args_conn['tabla_monitoreo']


# Leer argumentos enviados #
#--------------------------#
parser = argparse.ArgumentParser()
# add arguments to the parser
parser.add_argument("ruta_externa")
# lectura de los argumentos
args_sends = parser.parse_args()
ruta_externa = str(args_sends.ruta_externa)
print("      ==> Ruta CSV a ser leído: " + ruta_externa)
archivo = os.path.basename(ruta_externa)
print("      ==> Archivo CSV: " + archivo)
id_archivo = archivo.split(".")[0]

## COLUMNAS y VARIABLES PARA EL MANEJO DE ERRORES
estado_error = 0
global fecha_modificacion_fuente
fecha_modificacion_fuente = "NA"
global data_error
data_error = ""

## SEGUIMIENTO
global data_seguimiento
## ESTABLECER VALORES GLOABLES DE RECUENTO Y DE VALOR MEDIO, MÍNIMO, MAXIMO
columnas_seguimiento = ['id_archivo', 'recuento', 'media', 'minima', 'maxima', 'descripcion']
## CREAR UN DATAFRAME VACIÓ CON NOMBRES DE COLUMNAS
data_seguimiento = pd.DataFrame(columns=columnas_seguimiento)
## Variable de inicio process
global inicio_process
inicio_process = time.time()



# ---------------------------------------- FIN VARIABLES -----------------------------------------#


# ------------------------------------------ FUNCIONES -------------------------------------------
# Estadisticas
#------------------------#
def estadistica():    
    # Obtener el uso de la CPU (%)
    uso_cpu = psutil.cpu_percent()    
    #print("==============> uso_cpu: ", uso_cpu, "%")
    # Obtener el uso de la memoria (en bytes)
    uso_memoria = psutil.virtual_memory().used
    #print("==============> uso_memoria: ", uso_memoria, "bytes")
    # Obtener el uso del disco (en bytes)
    uso_disco = psutil.disk_usage('/').used
    #print("==============> uso_disco: ", uso_disco, "bytes")
    print("=== system ===========> uso_cpu: ", uso_cpu, "% | uso_memoria: ", uso_memoria, "bytes", "uso_disco: ", uso_disco, "bytes")

def estadisticas_inicio():    
    print("            ==> ESTADISTICAS EN EJECUCIÓN")
    # Obtener el PID (identificador del proceso actual)
    pid = os.getpid()
    ## FUNCIÓN DE OTRAS ESTADISTICAS
    estadistica()
    return pid

def estadisticas_fin(pid):    
    ## FUNCIÓN DE OTRAS ESTADISTICAS
    estadistica()
    # Obtener el uso de la memoria del proceso (en bytes)
    uso_memoria_proceso = psutil.Process(pid).memory_info().rss
    print("=== system ===========> uso_memoria_proceso: ", round(uso_memoria_proceso,1), "bytes")


# Devuelve la hora actual
#------------------------#
def horaActual():
  return datetime.now().time().strftime('%H:%M:%S')

#Replace de valores no necesarios control de errores
#--------------------------------------------------#
def replace_converter(cadena):
    try:
        cadena = cadena.replace("'", "")
        cadena = cadena.replace(";", "")
        cadena = cadena.replace('"', "")
        str_cadena = cadena
        return str_cadena
    except Exception:
        #Some code if needed
        return ''

#Guardar datos de seguimiento
#---------------------------#
def save_data_seguimiento(nueva_fila, data, globali):
    ## INSTANCIAMOS EL DATAFRAME DE SEGUIMIENTO        
    global data_seguimiento        

    if (globali == 0):
        ## APPEND AL DATAFRAME
        data = pd.concat([data, pd.DataFrame([nueva_fila])], ignore_index=True)
        # AGREGAR FILA DATAFRAME SEGUIMIENTO     
        nueva_fila_global = {'id_archivo' : str(nueva_fila['id_archivo']),'recuento': data['user_id'].count(), 'media': round(data['price'].mean(), 1), 'minima': round(data['price'].min(), 1), 'maxima': round(data['price'].max(), 1), 'descripcion' : nueva_fila['row']}
        save_data_seguimiento(nueva_fila_global, None, 1)
        return data        

    if (globali == 1):
        ## APPEND AL DATAFRAME
        data_seguimiento = pd.concat([data_seguimiento, pd.DataFrame([nueva_fila])], ignore_index=True)
        print("            ==> DATA DE SEGUIMIENTO")
        print(data_seguimiento)
        print("            ==> VALORES GLOBALES PRE-SEGUIMIENTO")

#Conectar BASE DE DATOS
#---------------------#
def conectar_bd():
    try:
        ## CONECTAR CON LA BASE DE DATOS        
        connection = psycopg2.connect(user=user,
                                  password=passw,
                                  host=host,
                                  port=port,
                                  database=dbname)
        print("            ==> CONEXIÓN REALIZADA")
        return connection
    except psycopg2.Error as e:
        print("            ==> Error al conectar a la base de datos:", e)
        return None

#DesConectar BASE DE DATOS
#------------------------#
def desconectar_bd(connection):
    if connection is not None:
        connection.close()
        print("            ==> CONEXIÓN CERRADA")
    else:
        print("            ==> No hay conexión para cerrar")

# ---------------------------------------- FIN FUNCIONES -----------------------------------------


# --------------------------------------------- MAIN ---------------------------------------------
def initialize():   
    '''
        Establecer ultima fecha modificación archivo fuente
    '''
    try:    
        print("         ==> FUNCION INITIALIZE()")
        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        pid = estadisticas_inicio()
        
        global fecha_modificacion_fuente
        print("            ==> Fecha Modificación ARCHIVO FUENTE")
        ## TOMAR DEL ARCHIVO CON LA RUTA LA FECHA Y HORA DE MODIFICACIÓN
        time_ts = os.path.getmtime(ruta_externa)        
        time_ts = datetime.fromtimestamp(time_ts)
        fecha_modificacion_fuente = str(time_ts.strftime('%Y-%m-%d %H:%M'))
        print("            ==> "+ fecha_modificacion_fuente)

        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        estadisticas_fin(pid)
    except Exception as e:
        print("            ==> NO SE HA PODIDO ESTABLECER LA ULTIMA FECHA DE ACTUALIZACIÓN DEL ARCHIVO FUENTE")
        print("            ==> ------------------------------------------------------------------------------")
        global estado_error
        if (estado_error== 0):         
            error= "ERROR : " + str(e)
            error= replace_converter(error)
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"FAIL",fecha +" "+ horaActual(),error]
            estado_error=1
            print("            ==> " + str(e))            
            ingest_monitoring(data_error, estado_error)            
    print("         ==> FIN FUNCION INITIALIZE()")


def extract():
    '''
        Load dataset from CSV
    '''
    try:
        print("         ==> FUNCIÓN EXTRACT()")
        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        pid = estadisticas_inicio()
        
        print("            ==> Load dataset from CSV")
        filename_CSV=str(ruta_externa)
        if os.path.isfile(filename_CSV):   
            print("            ==> Existe el ARCHIVO") 
            ## LECTURA DEL ARCHIVO CSV
            df = pd.read_csv(filename_CSV, sep=',', skiprows=1)
            print("            ==> LECTURA EXITOSA DE ARCHIVO CSV")

        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        estadisticas_fin(pid)
    except Exception as e:
        print("            ==> NO SE HA PODIDO CARGAR EL ARCHIVO")
        print("            ==> ---------------------------------")
        if (estado_error== 0): 
            error= "ERROR : " + str(e)
            error= replace_converter(error)
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"FAIL",fecha +" "+ horaActual(),error]
            estado_error=1
            print("            ==> " + str(e))
            ingest_monitoring(data_error)
    ## RETORNAMOS DATAFRAME
    print("         ==> FIN FUNCIÓN EXTRACT()")
    return df


def transform(datos):
    '''
        Manipulación de los datos
    '''
    try:
        print("         ==> FUNCIÓN TRANSFORM()")
        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        pid = estadisticas_inicio()

        ## CAMBIO EN EL NOMBRE DE LAS COLUMNAS        
        datos.columns = ["fecha", "price", "user_id"]
        datos.columns
        df = datos.reindex(['user_id', 'price', 'fecha'], axis=1)
        print("            ==> CAMBIO DE NOMBRES")
        ## NAN DE PRICE ELIMINARLOS
        df = df.dropna(subset=['price'])
        print("            ==> ELIMINAR NULOS")        
        # AGREGAR FILA DATAFRAME SEGUIMIENTO
        nueva_fila = {'id_archivo' : id_archivo,'recuento': df['user_id'].count(), 'media': round(df['price'].mean(), 1), 'minima': round(df['price'].min(), 1), 'maxima': round(df['price'].max(), 1), 'descripcion' : "Datos Globales - csv"}
        save_data_seguimiento(nueva_fila, None, 1)        

        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        estadisticas_fin(pid)
    except Exception as e:
        print("            ==> ERROR EN MANIPULACIÓN DE DATOS")
        print("            ==> ------------------------------")
        if (estado_error== 0): 
            error= "ERROR : " + str(e)
            error= replace_converter(error)
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"FAIL",fecha +" "+ horaActual(),error]
            estado_error=1
            print("            ==> " + str(e))
            ingest_monitoring(data_error)
    ## RETORNAMOS DATAFRAME TRANSFORMADO
    print("         ==> FIN FUNCIÓN TRANSFORM()")
    return df


def load(datos_transformados, user, passw, host, port, dbname):
    '''
        Write SQL table
    '''
    try:
        print("         ==> FUNCIÓN LOAD()")
        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        pid = estadisticas_inicio()

        ## CONECTAR CON LA BASE DE DATOS        
        connection = conectar_bd()
        ## CREAR CURSOR
        cursor = connection.cursor()
        ## INSTANCIAMOS EL DATAFRAME DE SEGUIMIENTO GLOBAL Y ESPECIFICO
        global data_seguimiento
        ## CREAR UN DATAFRAME VACIÓ CON NOMBRES DE COLUMNAS PARA ALMACENAR
        columnas_data = ['id_archivo', 'user_id', 'price', 'row']
        data_especific = pd.DataFrame(columns=columnas_data)
        control=0
        ## CICLO QUE PERMITE RECORRER CADA FILA DE LA DATA
        for indice_fila, fila in datos_transformados.iterrows():
            print("            ==> LECTURA FILA: " + str(indice_fila))
            datos = [fila['user_id'], fila['price'], fila['fecha'], fecha +" "+ horaActual()]
            ## INSERCIÓN EN BASE DE DATOS TABLA PRINCIPAL
            print(datos)
            cursor.execute("INSERT INTO " + tabla_prices + " (user_id, price, fecha, fecha_ingesta) VALUES (%s, %s, %s, %s)", datos)            
            connection.commit()
            print("            ==> REGISTRO GUARDADO: " + str(indice_fila))            
            # Agregar la fila al DataFrame
            nueva_fila = {'id_archivo' : id_archivo, 'user_id': fila['user_id'], 'price': fila['price'], 'row': "row: 0 - " + str(control +1)}
            #print(nueva_fila)
            data_especific = save_data_seguimiento(nueva_fila, data_especific, 0)
            control+=1
            print()

        ## CERRAR CURSOS Y CONEXIÓN
        cursor.close()
        desconectar_bd(connection)

        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        estadisticas_fin(pid)
    except Exception as e:
        print("            ==> ERROR EN WRITE SQL TABLE PRICES")
        print("            ==> ------------------------------")
        if (estado_error== 0): 
            error= "ERROR : " + str(e)
            error= replace_converter(error)
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"FAIL",fecha +" "+ horaActual(),error]
            estado_error=1
            print("            ==> " + str(e))
            ingest_monitoring(data_error)
    print("         ==> FIN FUNCIÓN LOAD()")


def trackingdb(user, passw, host, port, dbname):
    '''
        CONSULTA BD - SEGUIMIENTO ESTADISTICA
    '''
    try:
        print("         ==> FUNCIÓN TRACKINGDB()")
        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        pid = estadisticas_inicio()

        ## CONECTAR CON LA BASE DE DATOS        
        connection = conectar_bd()
        ## CREAR CURSOR
        cursor = connection.cursor()
        ## ESTABLECER LOS FILTROS DE LA CONSULTA
        query=""
        if (id_archivo== "validation"):
            query = "SELECT COUNT(0) as recuento, ROUND(AVG(CAST(price AS decimal)),1) as media, MIN(price), MAX(price) FROM " + tabla_prices + ";"
        else :
            annio = id_archivo.split("-")[0]
            mes = id_archivo.split("-")[1]
            query = "SELECT COUNT(0) as recuento, ROUND(AVG(CAST(price AS decimal)),1) as media, MIN(price), MAX(price) FROM " + tabla_prices + " WHERE EXTRACT(YEAR FROM fecha) = " + str(annio) + " AND EXTRACT(MONTH FROM fecha) =  " + str(mes) + " ;"

        ## INSTANCIAMOS EL DATAFRAME DE SEGUIMIENTO GLOBAL Y ESPECIFICO
        global data_seguimiento        
        # Executing a SQL query
        #cursor.execute("SELECT user_id, price, fecha FROM prices WHERE EXTRACT(YEAR FROM fecha) = " + str(annio) + " AND EXTRACT(MONTH FROM fecha) =  " + str(mes) + " ;")
        cursor.execute(query)
        ## RESULTADO DE LA CONSULTA
        record = cursor.fetchall()
        # AGREGAR FILA DATAFRAME SEGUIMIENTO
        nueva_fila = {'id_archivo' : id_archivo,'recuento': record[0][0], 'media': record[0][1], 'minima': record[0][2], 'maxima': record[0][3], 'descripcion' : "Datos Globales - postgres"}
        save_data_seguimiento(nueva_fila, None, 1)        

        ## CERRAR CURSOS Y CONEXIÓN
        cursor.close()
        desconectar_bd(connection)

        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        estadisticas_fin(pid)
    except Exception as e:
        print("            ==> ERROR EN WRITE SQL TABLE")
        print("            ==> ------------------------------")
        if (estado_error== 0): 
            error= "ERROR : " + str(e)
            error= replace_converter(error)
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"FAIL",fecha +" "+ horaActual(),error]
            estado_error=1
            print("            ==> " + str(e))
            ingest_monitoring(data_error)
    print("         ==> FIN FUNCIÓN TRACKINGDB()")


def save_tracking():
    '''
        Save data TRACKING
    '''
    try:
        print("         ==> FUNCIÓN SAVE TRACKING()")
        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        pid = estadisticas_inicio()

        ## CONECTAR CON LA BASE DE DATOS        
        engine = create_engine('postgresql://'+user+':'+passw+'@localhost:'+port+'/'+dbname)
        print("            ==> CONEXIÓN REALIZADA A POSTGRESQL CON ENGINE")        
        ## INSERTAR LOS DATOS DEL DATAFRAME EN PostgreSQL
        data_seguimiento.to_sql(tabla_seguimiento, con=engine, if_exists='append', index=False)
        print("            ==> SAVE TRACKING IN POSTGRESQL")        

        ## EJECUTAR ESTADISTICAS DE EJECUCIÓN
        estadisticas_fin(pid)
    except Exception as e:
        print("            ==> ERROR EN WRITE SQL TABLE TRACKING")
        print("            ==> ------------------------------")
        if (estado_error== 0): 
            error= "ERROR : " + str(e)
            error= replace_converter(error)
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"FAIL",fecha +" "+ horaActual(),error]
            estado_error=1            
            print("            ==> " + str(e))
            ingest_monitoring(data_error)
    print("         ==> FIN FUNCIÓN SAVE TRACKING()")    


def ingest_monitoring(data_error, estado_error):
    '''
        MANEJO CONTROL DE ERRORES
    '''
    try:
        print("         ==> FUNCIÓN INGEST MONITORING()")
        columns_ingest = ["id", "ruta_archivo", "fecha_modificacion_fuente", "estado", "fecha_ingesta", "error"]
        if (estado_error== 0): 
            print("         => MANEJO CONTROL DE ERRORES")
            data_error = [id_archivo,ruta_externa,fecha_modificacion_fuente,"SUCCESS",fecha +" "+ horaActual(),"None"]

        ## CREAR DATAFRAME DE INGESTA PARA FAIL OR SUCCESS
        dfestado = pd.DataFrame([data_error], columns=columns_ingest)
        print(dfestado)
        ## CONECTAR CON LA BASE DE DATOS        
        engine = create_engine('postgresql://'+user+':'+passw+'@localhost:'+port+'/'+dbname)
        print("            ==> CONEXIÓN REALIZADA A POSTGRESQL CON ENGINE")        
        ## INSERTAR LOS DATOS DEL DATAFRAME EN PostgreSQL
        dfestado.to_sql(tabla_monitoreo, con=engine, if_exists='append', index=False)
        print("            ==> SAVE INGEST MONITORING IN POSTGRESQL")        
    except Exception as e:
        print("            ERROR EN SAVE INGEST MONITORING")
        print("            ------------------------------")
        print("            ==> " + str(e))
    print("         ==> FIN FUNCIÓN INGEST MONITORING()")    


def main():
    ## PROCESO PRINCIPAL ETL
    ## FUNCIÓN INICIALIZADORA
    initialize()
    ## FUNCIÓN PARA EXTRAER LA DATA
    datos_extraidos = extract()    
    ## FUNCIÓN PARA TRANSFORMAR LA DATA Y ESTABLECER VALORES DE SEGUIMIENTO
    datos_transformados = transform(datos_extraidos)
    print("      ==> DATA TRANSFORMADA")
    print(datos_transformados)
    ## FUNCIÓN PARA CARGAR LA DATA
    load(datos_transformados, user, passw, host, port, dbname)
    ## FUNCIÓN DE SEGUIMIENTO
    trackingdb(user, passw, host, port, dbname)
    ## FUNCIÓN GUARDAR SEGUIMIENTO
    save_tracking()
    ## FUNCIÓN GUARDAR INGESTA MONITOREO
    ingest_monitoring(data_error, estado_error)
    print("      ==> Proceso ETL completado")  
    fin = time.time()
    tiempo_transcurrido = fin - inicio_process
    print("=== system ===========> tiempo_transcurrido: ", round(tiempo_transcurrido, 1), "segundos")    
 

if __name__ == "__main__":
    main()