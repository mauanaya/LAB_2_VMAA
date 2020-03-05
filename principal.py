# -- ------------------------------------------------------------------------------------ -- #
# -- proyecto: Microestructura y Sistemas de Trading - Laboratorio 2 - Behavioral Finance
# -- archivo: principal.py - flujo principal del proyecto
# -- mantiene: mauanaya
# -- repositorio: https://github.com/mauanaya/LAB_2_VMAA
# -- ------------------------------------------------------------------------------------ -- #

import funciones as fn
#%%

datos = fn.f_leer_archivo(param_archivo='archivo_tradeview_1.xlsx')
fn.f_pip_size(param_ins='audusd')
datos = fn.f_columnas_tiempo(param_data=datos)
datos = fn.f_columnas_pips(param_data=datos)
