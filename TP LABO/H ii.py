# H ii)
import pandas as pd
from inline_sql import sql, sql_val
camino = 'C:/Users/Bruno Goldfarb/Downloads/TP LDD/'
camion = '/home/Estudiante/Descargas/LDDTP1PR/'
datos_migraciones = pd.read_csv(camino + "datos_migraciones.csv")

datos_completos = pd.read_csv(camino + "lista-sedes-datos.csv", on_bad_lines= 'skip')

datos_basicos = pd.read_csv(camino + "lista-sedes.csv")

datos_secciones = pd.read_csv(camino + "lista-secciones.csv")

#%%----------- busco paises con sedes por region

# busco promedio por region geografica de migrantes en paises con sedes durante los 2000

# promedioMigrantesDosmil = sql^"""
#                               Select 'Country Origin Code', 'Country Dest Code', '2000[2000]'
#                               From datos_migraciones
#                               Where 'Migration by Gender' = 'Total' AND 'Country Origin Code' = 'ARG'
#                               """
# print(promedioMigrantesDosmil)

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
                SELECT DISTINCT i.codigo, (i.inmigraciones00 - e.emigraciones00) AS flujo_mundo
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
               
inmigraciones00ARG = sql^consulta_sql

consulta_sql = """
                SELECT DISTINCT i.codigo, (i.inmigraciones00ARG - e.emigraciones00ARG) AS flujo_ARG
                FROM inmigraciones00ARG AS i
                INNER JOIN emigraciones00ARG AS e
                ON i.codigo = e.codigo;
                """

migraciones00ARG = sql^consulta_sql

paisesConSedes = sql^"""
                Select distinct pais_iso_3, region_geografica
                From datos_completos
                """
        
consultaSQL = """
              Select region_geografica, count(pais_iso_3) AS paises_con_sedes
              From paisesConSedes
              Group by region_geografica
              
              """
              
sedesPorRegion = sql^consultaSQL
# print(sedesPorRegion)


# consultaSQL = """
#                 Select distinct m.codigo, r.region_geografica, AVG(m.flujo_ARG)
#                 From migraciones00ARG as m
#                 INNER JOIN paisesConSedes as r
#                 ON r.pais_iso_3 = m.codigo
#                 Group by m.codigo
#                 """

paisesConSedesPorRegion = sql^consultaSQL


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

consultaSQL = """
                Select distinct region_geografica, AVG(flujo_ARG) as flujo_promedio 
                From Pais
                Group by region_geografica
                """
regionYflujo = sql^consultaSQL

consultaSQL = """
                Select distinct r.region_geografica, r.flujo_promedio, p.paises_con_sedes
                From regionYflujo AS r
                INNER JOIN paisesConSedesPorRegion AS p
                On r.region_geografica = p.region_geografica
                Group by r.region_geografica, r.flujo_promedio, p.paises_con_sedes
                order by r.flujo_promedio DESC
                """
flujoPorRegionYSedes = sql^consultaSQL