#Librerias a usar
from flask import Flask, Response
from flask import jsonify, request
from flask_cors import CORS

import psycopg2
import json
import os
import datetime
import pandas as pd
import numpy as np

app = Flask(__name__)
CORS(app)

#Para ejecutar el código colocar los siguientes comandos en la terminal:
#export FLASK_APP=main.py
#flask run

#export FLASK_PORT=8080

# ====================== Connect to DB ====================== #
# Endpoint => URL
@app.route('/demografia', methods=['GET'])  # desde Postman http://127.0.0.1:5000/demografia
def read_from_db():
    '''
    Servicio web sencillo que retorna la información leída de una transacción con Postgres mediante conexión típica de Python.
    '''
    setup_file = open('setup.ini', 'r')
    setup_params = {}

    try:
        ###############################################################
        # Challenge No. 1: Crear un archivo llamado utilities.py y colocar allá una función que genere la conexión a la BD.
        # Importar el archivo, e invocar luego la función cada vez que se requiera conexión a la BD, como es el caso de este endpoint.
        ###############################################################
        setup_vars = setup_file.readlines()
        for var in setup_vars:
            line = var.split(':')
            setup_params[ line[0] ] = line[1].replace('\n', '')

        connection = psycopg2.connect(user=setup_params['db_user'],
                                        password=setup_params['db_psword'],
                                        host=setup_params['db_host'],
                                        port=setup_params['db_port'],
                                        database=setup_params['db_name'])

        query_ = "SELECT id AS id_persona, doc AS documento, name AS nombre, age AS edad, \
                college AS universidad \
                FROM people ORDER BY name;" 
        cursor = connection.cursor()
        cursor.execute(query_) 
        people = cursor.fetchall() 

        data = {} 
        for person in people: 
            key = person[0] 
            data[key] = {}
            data[key]["documento"] = person[1]
            data[key]["nombre"] = person[2]
            data[key]["edad"] = person[3]
            data[key]["universidad"] = person[4]

        return jsonify({ 'personas': data })
    except:
        return Response(status=500)


# ====================== Read CSV ====================== #
@app.route('/procesos', methods=['GET'])
def read_from_csv():
    '''
    Servicio web que toma la información tomada de un CSV en local y la retorna como respuesta
    '''
    df = pd.read_csv("data.csv") 
    
    #mecanismo para iterar fila a fila el dataframe
    for index, row in df.iterrows():
        print(row)
    
    # ejemplos de funciones útiles con DataFrames de Pandas
    print(df.head())
    print(df.shape)
    print(df[df["test_grade"] > 8])

    process = df.to_dict()
    print(process)

    ###############################################################
    # Challenge No. 2: Incrementar el tamaño del archivo data.csv a por lo menos 20 filas.
    # En el Diccionario generado a partir del DataFrame en la línera 86, se le debe calcular la transpuesta del DataFrame antes de hacer la conversión
    ###############################################################
       

    return jsonify({ 'procesos': process })

# ====================== Pandas & Numpy ====================== #
@app.route('/estadisticas')
def get_statistics():
    '''
    Obtención de estadísticas a partir de la generación de arreglos en Numpy a partir de columnas de un DataFrame.
    '''
    df = pd.read_csv("data.csv") 
    np_array = np.array( df["test_grade"].tolist() )

    statistics = {}
    statistics['average'] = str(np.average( np_array ))
    statistics['max'] = str(np.amax( np_array ))
    statistics['min'] = str(np.amin( np_array ))
    statistics['mean'] = str(np.mean( np_array ))
    statistics['std_dev'] = str(np.std( np_array ))
    
    return jsonify({ 'estadisticas': statistics })


# ====================== PG in Pandas ====================== #
@app.route('/demografia_pandas')
def read_pg_pandas():
    setup_file = open('setup.ini', 'r')
    setup_params = {}

    try:
        setup_vars = setup_file.readlines()
        for var in setup_vars:
            line = var.split(':')
            setup_params[ line[0] ] = line[1].replace('\n', '')

        connection = psycopg2.connect(user=setup_params['db_user'],
                                        password=setup_params['db_psword'],
                                        host=setup_params['db_host'],
                                        port=setup_params['db_port'],
                                        database=setup_params['db_name'])

        query_ = "SELECT doc AS documento, name AS nombre, age AS edad, college AS universidad \
                FROM people ORDER BY name;" 
        
        df_pg = pd.read_sql_query(query_, connection)
        print(df_pg)

        ###############################################################
        # Challenge No. 3: Aplicar los cambios de los dos primeros challenges en este servicio, de tal manera que el código
        # sea más sencillo, y se retornen los datos en la manera más apropiada para su lectura.
        ###############################################################

        people = df_pg.to_dict()
        return jsonify({ 'personas': people })
    except:
        return Response(status=500)


# ====================== Merge in Pandas ====================== #
@app.route('/data_completa')
def read_data():
    setup_file = open('setup.ini', 'r')
    setup_params = {}

    try:
        setup_vars = setup_file.readlines()
        for var in setup_vars:
            line = var.split(':')
            setup_params[ line[0] ] = line[1].replace('\n', '')

        connection = psycopg2.connect(user=setup_params['db_user'],
                                        password=setup_params['db_psword'],
                                        host=setup_params['db_host'],
                                        port=setup_params['db_port'],
                                        database=setup_params['db_name'])

        query_ = "SELECT doc AS doc, name AS nombre, age AS edad, college AS universidad \
                FROM people ORDER BY name;" 
        
        df_pg = pd.read_sql_query(query_, connection)
        df_csv = pd.read_csv("data.csv") 
        print(df_pg)
        print(df_csv)

        df_total = pd.merge(df_pg, df_csv, on='doc') #how='left'
        print(df_total)
        print(df_total.shape)

        ###############################################################
        # Challenge No. 4: Corregir el tipo de dato del campo 'doc' que está siendo utilizado para mezclar los dos DataFrames.
        # Verificar que se genere un DataFrame final con los datos cruzados correctamente.
        ###############################################################
        
        full_data = df_total.to_dict()
        return jsonify({ 'datos_completos': full_data })
    except:
        return Response(status=500)