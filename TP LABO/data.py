#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 11:33:24 2024

Integrantes:
Sebastian Manuel Souto,
Gian Lucca Sanza,
Goldfarb Bruno.

En el siguiente código realizamos la limpieza de los datos originales y
la resolución de los ejercicios.
"""

#%%
# Importamos bibliotecas
import os
from pathlib import Path
import pandas as pd
from inline_sql import sql, sql_val
import numpy as np
import matplotlib.pyplot as plt # Para graficar series multiples
from   matplotlib import ticker   # Para agregar separador de miles
import seaborn as sns           # Para graficar histograma


# Cambiar el directorio de trabajo al del archivo actual
ruta_base = Path(__file__).parent

os.chdir(ruta_base)



archivo_completo=  "lista-sedes-completos.csv"

#Este archivo es archivo_completo pero solucionando el error que tenia una fila que no nos permitia
#visualizarla (Republica de Chile).

archivo_completo_copia=  "lista-sedes-completos - copia.csv"

archivo_basico = "lista-sedes-basicos.csv"

archivo_secciones= "lista-secciones.csv"

archivo_migraciones = "datos_migraciones.csv"

datos_basicos= pd.read_csv(archivo_basico)

datos_secciones = pd.read_csv(archivo_secciones)

datos_completos= pd.read_csv(archivo_completo_copia)

datos_migraciones = pd.read_csv(archivo_migraciones)



#%%
#Armado de las tablas del Modelo Relacional:

############PAIS#############:

#Primero cambio los valores de flujo donde hay '..' por '0'.
                
consulta_sql20 = """
                  SELECT "Country Origin Name", "Country Origin Code",
                         "Migration by Gender Name", "Migration by Gender Code",
                         "Country Dest Name", "Country Dest Code", REPLACE ("1960 [1960]", '..', '0') AS "1960 [1960]" , REPLACE ("1970 [1970]", '..', '0') AS "1970 [1970]",
                         REPLACE("1980 [1980]",'..','0') AS "1980 [1980]", REPLACE("1990 [1990]",'..','0') AS "1990 [1990]", REPLACE ("2000 [2000]", '..', '0') AS "2000 [2000]"
                  FROM datos_migraciones;
                 """


datos_migraciones2 = sql^consulta_sql20


#Tomo las inmigraciones y las emigraciones y resto.
    
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
                SELECT DISTINCT i.codigo, (i.inmigraciones00 - e.emigraciones00) AS flujo_mundo
                FROM inmigraciones00 AS i
                INNER JOIN emigraciones00 AS e
                ON i.codigo = e.codigo
                ORDER BY i.codigo;
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
               
inmigraciones00ARG = sql^consulta_sql

consulta_sql = """
                SELECT DISTINCT i.codigo, (i.inmigraciones00ARG - e.emigraciones00ARG) AS flujo_ARG
                FROM inmigraciones00ARG AS i
                INNER JOIN emigraciones00ARG AS e
                ON i.codigo = e.codigo;
               """

migraciones00ARG = sql^consulta_sql

#ahora hago una tabla que tenga flujo migratorio de 1960 hasta 2000 (para el punto I ii)

#calculo las emigraciones
consulta_sql = """
                SELECT DISTINCT "Country Origin Code" AS codigo, SUM(CAST("1960 [1960]" AS DECIMAL)) AS emigraciones60,
                                SUM(CAST("1970 [1970]" AS DECIMAL)) AS emigraciones70,
                                SUM(CAST("1980 [1980]" AS DECIMAL)) AS emigraciones80,
                                SUM(CAST("1990 [1990]" AS DECIMAL)) AS emigraciones90,
                                SUM(CAST("2000 [2000]" AS DECIMAL)) AS emigraciones00
                FROM datos_migraciones2
                GROUP BY "Country Origin Code";
               """
               
emigraciones = sql^consulta_sql

#calculo la inmigraciones
consulta_sql = """
                SELECT DISTINCT "Country Dest Code" AS codigo, SUM(CAST("1960 [1960]" AS DECIMAL)) AS inmigraciones60,
                                SUM(CAST("1970 [1970]" AS DECIMAL)) AS inmigraciones70,
                                SUM(CAST("1980 [1980]" AS DECIMAL)) AS inmigraciones80,
                                SUM(CAST("1990 [1990]" AS DECIMAL)) AS inmigraciones90,
                                SUM(CAST("2000 [2000]" AS DECIMAL)) AS inmigraciones00
                FROM datos_migraciones2
                GROUP BY "Country Dest Code";
               """
               
inmigraciones = sql^consulta_sql


#calculo el flujo migratorio
consulta_sql = """
                SELECT DISTINCT i.codigo, 
                                (i.inmigraciones60 - e.emigraciones60) AS flujo_mundo60,
                                (i.inmigraciones70 - e.emigraciones70) AS flujo_mundo70,
                                (i.inmigraciones80 - e.emigraciones80) AS flujo_mundo80,
                                (i.inmigraciones90 - e.emigraciones90) AS flujo_mundo90,
                                (i.inmigraciones00 - e.emigraciones00) AS flujo_mundo00
                                
                FROM inmigraciones AS i
                INNER JOIN emigraciones AS e
                ON i.codigo = e.codigo
                ORDER BY i.codigo;
               """

migraciones = sql^consulta_sql

#hago la suma de los flujos migratorios por cada decada
consulta_sql = """
                SELECT DISTINCT codigo, (flujo_mundo60 + flujo_mundo70 + flujo_mundo80 + flujo_mundo90 +flujo_mundo00) flujo60_00
                FROM migraciones
                ORDER BY codigo ASC;
               """
               
flujo_total = sql^consulta_sql 

#Ahora voy a armar una tabla que tenga Codigo, Pais y region geografica

consulta_sql = """
                SELECT DISTINCT pais_iso_3, pais_castellano AS nombre_pais, region_geografica
                FROM datos_completos;
               """

info_pais = sql^consulta_sql

#Finalmente armo la relacion PAIS.

consulta_sql = """
                SELECT DISTINCT i.nombre_pais, i.region_geografica, m1.flujo_mundo, m2.flujo_ARG, f.flujo60_00
                FROM info_pais as i
                INNER JOIN migraciones00 AS m1
                ON i.pais_iso_3 = m1.codigo
                INNER JOIN migraciones00ARG AS m2
                ON i.pais_iso_3 = m2.codigo
                INNER JOIN flujo_total AS f
                ON i.pais_iso_3 = f.codigo
                ORDER BY i.nombre_pais;
               """

Pais = sql^consulta_sql
# %%

# ######################SEDES##################:

#Los atributos son sede_id y nombre_pais. 

consulta_sql = """
                SELECT DISTINCT sede_id, pais_castellano AS nombre_pais
                FROM datos_completos
               """

sedes = sql^consulta_sql



#%%
###########REDES SOCIALES#################
#Para armar la tabla de Redes Sociales hay que dividir los valores originales que hay 
#en datos completos, para cumplir con las formas normales.


consulta_sql = """
                SELECT DISTINCT redes_sociales, sede_id
                FROM datos_completos
               """

redes_sociales0 = sql^consulta_sql


#Aca separamos el texto de las celdad de redes sociales con "//", ya que ese es el formato en
#el csv para dividir cada red social de cada sede.
redes_sociales0['redes_sociales'] = redes_sociales0['redes_sociales'].str.split(' // ')

redes_sociales01 = redes_sociales0.explode('redes_sociales').reset_index(drop=True)

consulta_sql = """
                SELECT DISTINCT sede_id, redes_sociales
                FROM redes_sociales01
                WHERE redes_sociales IS NOT NULL 
                    AND TRIM(redes_sociales) != '';
               """

redes_sociales = sql^consulta_sql




# %%
# ############SECCIONES ##################:

#hay que hacer la tabla de secciones y también la tabla de dividida_en

consulta_sql = """
                SELECT DISTINCT sede_desc_castellano
                FROM datos_secciones;
               """
secciones = sql^consulta_sql

consulta_sql = """
                SELECT DISTINCT sede_id, sede_desc_castellano
                FROM datos_secciones;
               """

dividida_en = sql^consulta_sql

#%%

##########EJERCICIO H#########

#%%
#i)

#Primero hago una tabla de paises con la cantidad de sedes
consulta_sql = """
                SELECT DISTINCT nombre_pais, COUNT(nombre_pais) AS cant_sedes
                FROM sedes
                GROUP BY nombre_pais
                ORDER BY nombre_pais;
               """

cantidad_sedes = sql^consulta_sql



#Ahora una tabla de sedes con la cantidad de secciones (ademas guardo el pais de la sede)
consulta_sql = """
                SELECT DISTINCT s1.nombre_pais, s1.sede_id, s2.cant_secciones
                FROM sedes AS s1
                LEFT JOIN (SELECT DISTINCT sede_id, COUNT(sede_id) AS cant_secciones
                      FROM dividida_en
                      GROUP BY sede_id) AS s2
                ON s1.sede_id = s2.sede_id
                ORDER BY s1.nombre_pais;
               """
               
cant_seccionesXsede = sql^consulta_sql

#hay sedes que no tienen secciones, debo reemplazar los nulls por 0s.

consulta_sql = """
                SELECT DISTINCT nombre_pais, sede_id, COALESCE(cant_secciones, 0) AS cant_secciones
                FROM cant_seccionesXsede
                ORDER BY nombre_pais;
               """

cant_seccionesXsede2 = sql^consulta_sql

#use ese comando para reemplazar los nulls por 0, nose porq no funciona replace.

#Ahora saco el promedio de secciones por sede.
consulta_sql = """
                SELECT DISTINCT nombre_pais, AVG(cant_secciones) AS secciones_promedio
                FROM cant_seccionesXsede2
                GROUP BY nombre_pais
                ORDER BY nombre_pais;
               """

avg_secciones = sql^consulta_sql


#Finalmente los joins de las tablas que hice para obtener el resultado.

consulta_sql = """
                SELECT DISTINCT cs.nombre_pais, cs.cant_sedes, a.secciones_promedio, p.flujo_mundo
                FROM cantidad_sedes AS cs
                INNER JOIN avg_secciones AS a
                ON cs.nombre_pais = a.nombre_pais
                LEFT JOIN Pais AS p
                ON cs.nombre_pais = p.nombre_pais
                ORDER BY cant_sedes DESC, cs.nombre_pais;
               """

resultado = sql^consulta_sql




#%%
#ii)
#Selecciono region geografica y calculo el promedio del flujo con argentina.
consultaSQL = """
                Select distinct region_geografica, AVG(flujo_ARG) as flujo_promedio_arg 
                From Pais
                Group by region_geografica
                """
regionYflujo = sql^consultaSQL

#Selecciono region geografica y cuento cuantos países tienen sedes argentinas
paisesConSedes = sql^"""
                Select distinct region_geografica, COUNT(nombre_pais) AS paises_con_sedes
                From Pais
                Group by region_geografica;
                """

#Con los datos conseguidos anteriormente hago los joins para obtener la tabla pedida.
consulta_sql = """
                SELECT r.region_geografica, p.paises_con_sedes, r.flujo_promedio_arg 
                FROM regionYflujo AS r
                INNER JOIN paisesConSedes AS p
                ON r.region_geografica = p.region_geografica
                ORDER BY r.flujo_promedio_arg DESC;
               """

flujoPorRegionYSedes = sql^consulta_sql

#%%
#iii)

#Selecciono nombre del pais, sede id y red social.
consulta_sql = """
                SELECT DISTINCT  s.nombre_pais, s.sede_id, r.redes_sociales
                FROM sedes AS s
                INNER JOIN  redes_sociales AS r
                ON s.sede_id = r.sede_id
                ORDER BY s.nombre_pais ASC;
               """

paisSedesRedes = sql^consulta_sql

#Armo una tabla con nombre de pais y una columna que diga que red social usa.
consulta_sql = """
                SELECT DISTINCT  nombre_pais, 
                                CASE 
                                    WHEN redes_sociales LIKE '%facebook%' THEN 'facebook'
                                    WHEN redes_sociales LIKE '%instagram%' THEN 'instagram'
                                    WHEN redes_sociales LIKE '%twitter%' THEN 'twitter'
                                    WHEN redes_sociales LIKE '%linkedin%' THEN 'linkedin'
                                    WHEN redes_sociales LIKE '%flickr%' THEN 'flickr'
                                    WHEN redes_sociales LIKE '%youtube%' THEN 'youtube'
                                    WHEN redes_sociales LIKE '%gmail%' THEN 'gmail'
                                    ELSE 'desconocida'
                                END AS red_social
                FROM paisSedesRedes
                ORDER BY nombre_pais ASC;
               """

paisesConRedes = sql^consulta_sql

#Finalmente cuento cuantas redes sociales utiliza cada país.

consulta_sql = """
                SELECT DISTINCT nombre_pais, count(nombre_pais) AS cant_redes
                FROM paisesConRedes
                WHERE red_social != 'desconocida'
                GROUP BY nombre_pais
                ORDER BY nombre_pais ASC;
               """
               
cantRedesPais = sql^consulta_sql

#%%
#iv)

#Seleccionamos todo de la tabla redes_sociales y sedes
consulta_sql="""
SELECT *
FROM redes_sociales as r 
INNER JOIN sedes as s
ON s.sede_id = r.sede_id
"""

redes_paises = sql^consulta_sql

#Ahora seleccionamos el nombre del pais, la sede, a que red social pertenece el URL y el URl. 
#Tal vez hay alguna mejor forma de hacer esto
consulta_sql = """
                SELECT DISTINCT  nombre_pais as Pais,sede_id as Sede, 
                                CASE 
                                    WHEN redes_sociales LIKE '%facebook%' THEN 'Facebook'
                                    WHEN redes_sociales LIKE '%instagram%' THEN 'Instagram'
                                    WHEN redes_sociales LIKE '%twitter%' THEN 'Twitter'
                                    WHEN redes_sociales LIKE '%linkedin%' THEN 'Linkedin'
                                    WHEN redes_sociales LIKE '%flickr%' THEN 'Flickr'
                                    WHEN redes_sociales LIKE '%youtube%' THEN 'Youtube'
                                    WHEN redes_sociales LIKE '%gmail%' THEN 'Gmail'
                                    ELSE 'desconocida'
                                END AS red_social,
                                redes_sociales as URL
                FROM redes_paises
                ORDER BY nombre_pais ASC;
               """


redesxpaisurl=sql^consulta_sql


#%% 

#!!!!!!!!!!!!!!!!!!!VISUALIZACION!!!!!!!!!!!!!!!!!!!!!!!!

#i)


#i)
#cantidad de sedes por region geografica:
sedesxregionbien=sql^"""
SELECT COUNT(s.nombre_pais) as cant_sedes, region_geografica
FROM sedes as s
INNER JOIN Pais as ip
ON s.nombre_pais = ip.nombre_pais
GROUP BY region_geografica
ORDER BY cant_sedes
"""



#%%
fig, ax = plt.subplots()

plt.rcParams['font.family'] = 'sans-serif'           

#Usamos unos colores seleccionados manualmente para ser constante con cada región.
colores = ['blue', "#A0C4FF", '#00FF00', '#8B4513', '#FFB3BA', '#40E0D0', '#800080', '#FF0000', '#FFF700']

sns.barplot(data=sedesxregionbien, x='region_geografica', y='cant_sedes',palette=colores,
            legend='full',errorbar=None,edgecolor="black",linewidth=2.5)



ax.set_title('Sedes x Region Geografica')
ax.set_xlabel('Región', fontsize='medium')                       
ax.set_ylabel('Cantidad', fontsize='medium')    
#ax.set_xlim(0, 11)
#ax.set_ylim(0, 250)
plt.xticks(rotation=90)
plt.grid(True,linestyle="--",linewidth=0.5)



#%%

#ii)

colores = ['#40E0D0', 'blue', '#FF0000', '#8B4513', '#800080', '#FFF700','#FFB3BA','#00FF00', "#A0C4FF"]

#esto es para poder ordenar el boxplot segun la mediana.
medianas = Pais.groupby('region_geografica')['flujo_mundo'].median().sort_values(ascending=False)

ax = sns.boxplot(x="region_geografica", 
                 y="flujo60_00",  
                 data=Pais,
                 order = medianas.index,
                 palette = colores)

ax.set_title('Flujo Migratorio Por Región')
ax.set_xlabel('Región Geográfica')
ax.set_ylabel('Flujo Migratorio')

plt.xticks(rotation=90)
plt.grid(True,linestyle="--",linewidth=0.5)


#%%
#iii)
consultaSQL = """
                Select c.cant_sedes, m.flujo_ARG
                From cantidad_sedes AS c
                INNER JOIN Pais AS m
                ON c.nombre_pais = m.nombre_pais
                """

sedes_flujo = sql^consultaSQL

#Aca remarcamos la sección de la izquieda donde se juntan los puntos:
sedes_flujo['color'] = ['red' if x == 1 else '#0d99ff' for x in sedes_flujo['cant_sedes']]

sns.scatterplot(data=sedes_flujo, x='cant_sedes', y='flujo_ARG', hue='color', palette=['#0d99ff', 'red'], s=50, legend=False)

plt.xlabel('Cantidad de sedes', fontsize='medium')
plt.ylabel('Flujo migratorio', fontsize='medium')
plt.title('Flujo migratorio en relación a la cantidad de sedes')
plt.show()

#%%
#mismo gráfico enfocado en los paises con 1 sede

sns.scatterplot(data=sedes_flujo, x='cant_sedes', y='flujo_ARG', color = 'red', s=50, legend=False)

plt.xlabel('Cantidad de sedes', fontsize='medium')
plt.ylabel('Flujo migratorio', fontsize='medium')
plt.title('Flujo migratorio en los paises con 1 sede')
plt.xlim(0.5,1.5)
plt.ylim(-6000, 5000)
plt.show()

#%%

#ACA PONEMOS CODIGO PARA UTILIZAR EN EL INFORME


resultadoAbreviadoHi=resultado.head()

resultadoAbreviadoHii= cantRedesPais.iloc[0:6]

#%%

#GQM PARA MIGRACIONES 


consulta_sql = """
                SELECT DISTINCT COUNT("2000 [2000]") AS cant_de_puntos
                FROM datos_migraciones 
                WHERE "2000 [2000]" = '..'
               """

gqm_migraciones = sql^consulta_sql
