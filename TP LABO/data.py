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

dbasic= pd.read_csv("lista-sedes-basicos.csv")

dsecciones = pd.read_csv(archivo_secciones)

dcompleto= pd.read_csv(archivo_completo)