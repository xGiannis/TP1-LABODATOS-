#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 11:33:24 2024

@author: Estudiante
"""

# Importamos bibliotecas
import pandas as pd
from inline_sql import sql, sql_val

archivo_completo=  "lista-sedes-completos.csv"

archivo_basico = "lista-sedes-basicos.csv"

archivo_secciones= "lista-secciones.csv"

archivo_migraciones = "datos_migraciones.csv"

datos_basicos= pd.read_csv("lista-sedes-basicos.csv")

datos_secciones = pd.read_csv(archivo_secciones)

datos_completos= pd.read_csv(archivo_completo)

datos_migraciones = pd.read_csv(archivo_migraciones)

#%%

#Armado de las tablas del Modelo Relacional:

#PAIS:

#Primero saco todos los valores que no se pueden convertir a decimal de la columna 2000
consulta_sql2 = """
                 SELECT * 
                 FROM datos_migraciones
                 WHERE TRY_CAST("2000 [2000]" AS DECIMAL) IS NOT NULL;
                """

datos_migraciones2 = sql^consulta_sql2

#Tomo las inmigraciones y las emigraciones y sumo.
    
consulta_sql = """
                SELECT DISTINCT "Country Origin Code" AS codigo, SUM(CAST("2000 [2000]" AS DECIMAL)) AS emigraciones00
                FROM datos_migraciones2
                GROUP BY "Country Origin Code";
               """
               
emigraciones00 = sql^consulta_sql


consulta_sql = """
                SELECT DISTINCT "Country Dest Code" AS codigo, SUM(CAST("2000 [2000]" AS DECIMAL)) AS inmigraciones00
                FROM datos_migraciones2
                GROUP BY "Country Dest Code";
               """
               
inmigraciones00 = sql^consulta_sql


consulta_sql = """
                SELECT DISTINCT i.codigo, (i.inmigraciones00 + e.emigraciones00) AS flujo_mundo
                FROM inmigraciones00 AS i
                INNER JOIN emigraciones00 AS e
                ON i.codigo = e.codigo;
               """

migraciones00 = sql^consulta_sql

#Ahora hay que poner el flujo migratorio con ARG.

consulta_sql = """
                SELECT DISTINCT "Country Origin Code" AS codigo, SUM(CAST("2000 [2000]" AS DECIMAL)) AS emigraciones00ARG
                FROM datos_migraciones2
                WHERE "Country Dest Code" = 'ARG'
                GROUP BY "Country Origin Code";
               """
               
emigraciones00ARG = sql^consulta_sql  

consulta_sql = """
                SELECT DISTINCT "Country Dest Code" AS codigo, SUM(CAST("2000 [2000]" AS DECIMAL)) AS inmigraciones00ARG
                FROM datos_migraciones2
                WHERE "Country Origin Code" = 'ARG'
                GROUP BY "Country Dest Code";
               """
               
inmigraciones00 = sql^consulta_sql

consulta_sql = """
                SELECT DISTINCT i.codigo, (i.inmigraciones00ARG + e.emigraciones00ARG) AS flujo_ARG
                FROM inmigraciones00ARG AS i
                INNER JOIN emigraciones00ARG AS e
                ON i.codigo = e.codigo;
               """

migraciones00ARG = sql^consulta_sql

#Ahora voy a armar una tabla que tenga Codigo, Pais y region geografica



consulta_sql = """
                SELECT DISTINCT pais_iso_3, pais_castellano AS nombre_pais, region_geografica
                FROM datos_completos;
               """

info_pais = sql^consulta_sql

#Finalmente armo la relacion PAIS.

consulta_sql = """
                SELECT DISTINCT i.nombre_pais, i.region_geografica, m1.flujo_mundo, m2.flujo_ARG
                FROM info_pais as i
                INNER JOIN migraciones00 AS m1
                ON i.pais_iso_3 = m1.codigo
                INNER JOIN migraciones00ARG AS m2
                ON i.pais_iso_3 = m2.codigo;
               """

Pais = sql^consulta_sql