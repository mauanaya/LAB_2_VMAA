# -- ------------------------------------------------------------------------------------ -- #
# -- proyecto: Microestructura y Sistemas de Trading - Laboratorio 2 - Behavioral Finance
# -- archivo: principal.py - flujo principal del proyecto
# -- mantiene: mauanaya
# -- repositorio: https://github.com/mauanaya/LAB_2_VMAA
# -- ------------------------------------------------------------------------------------ -- #

import funciones as fn
#%%
datos = fn.f_leer_archivo(param_archivo='archivo_tradeview_1.xlsx')
#%%
fn.f_pip_size(param_ins='audusd')
#%%
datos = fn.f_columnas_tiempo(param_data=datos)
#%%
datos = fn.f_columnas_pips(param_data=datos)
#%%
stats = fn.f_estadisticas_ba(param_data=datos)
#%%
datos = fn.f_capital_acum(param_data=datos)

#%%
df_profit_diario = fn.f_profit_diario(param_data=datos)["df"]
df_profit_diario_c = fn.f_profit_diario(param_data=datos)["df_c"]
df_profit_diario_v = fn.f_profit_diario(param_data=datos)["df_v"]
df_sp = fn.f_profit_diario(param_data=datos)["sp"]
#%%
df_mad = fn.f_estadisticas_mad(param_data=df_profit_diario, 
                                        param_data_1=df_profit_diario_c,
                                        param_data_2=df_profit_diario_v)
#%%
df_disposition_effect = fn.f_be_de(param_data=datos)


