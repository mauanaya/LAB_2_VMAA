# -- ------------------------------------------------------------------------------------ -- #
# -- proyecto: Microestructura y Sistemas de Trading - Laboratorio 2 - Behavioral Finance
# -- archivo: funciones.py - procesamiento de datos
# -- mantiene: mauanaya
# -- repositorio: https://github.com/mauanaya/LAB_2_VMAA
# -- ------------------------------------------------------------------------------------ -- #

import pandas as pd
import numpy as np
import math as math

#%% FUNCION: leer archivo
def f_leer_archivo(param_archivo):
    """"
    Parameters
    ----------
    param_archivos : str : nombre de archivo a leer

    Returns
    -------
    df_data : pd.DataFrame : con informacion contenida en archivo leido

    Debugging
    ---------
    param_archivo =  'trading_historico.xlsx'

    """
    # Leer archivo de datos y guardarlo en Data Frame
    df_data = pd.read_excel('Archivos/' + param_archivo, sheet_name='Hoja1')

    # Convertir a minusculas el nombre de las columnas
    df_data.columns = [list(df_data.columns)[i].lower() for i in range(0, len(df_data.columns))]
    
    # Asegurar que ciertas columnas son tipo numerico
    numcols =  ['s/l', 't/p', 'commission', 'openprice', 'closeprice', 'profit', 'size', 'swap', 'taxes']
    df_data[numcols] = df_data[numcols].apply(pd.to_numeric)

    return df_data

#%% FUNCION: 
def f_pip_size(param_ins):
    """"
    Parameters
    ----------
    param_ins : str : nombre de instrumento 

    Returns
    -------
    pips_inst : 

    Debugging
    ---------
    param_ins =  'trading_historico.xlsx'

    """
    
    ""
    
    # encontrar y eliminar un guion bajo
    inst = param_ins.replace('_', '')
    inst = param_ins.replace('-2', '')
    
    # transformar a minusculas
    inst = inst.lower()
    
    #lista de pips por instrumento
    pips_inst =  {'audusd' : 10000, 'gbpusd': 10000, 'xauusd': 10, 'eurusd': 10000, 'xaueur': 10,
                  'nas100usd': 10, 'us30usd': 10, 'mbtcusd':100, 'usdmxn': 10000, 'eurjpy':100, 
                  'gbpjpy':100, 'usdjpy':100, 'btcusd':10, 'eurgbp':10000, 'usdcad':10000,}
    
    return pips_inst[param_ins]

#%% FUNCION:
def f_columnas_tiempo(param_data):

#convertir columna de 'closetime' y 'opentime' utilizando pd.to_datetime
    param_data['closetime'] = pd.to_datetime(param_data['closetime'])
    param_data['opentime'] = pd.to_datetime(param_data['opentime'])
    
    param_data['tiempo'] = [(param_data.loc[i, 'closetime'] - param_data.loc[i, 'opentime']).delta / 1*np.exp(9)
    for i in range(0, len(param_data['closetime']))]
    
    return param_data
    
#%% FUNCION:
def f_columnas_pips(param_data):
    param_data['pips'] = np.zeros(len(param_data['type']))
    for i in range(0,len(param_data['type'])):
        if param_data['type'][i] == 'buy':
            param_data['pips'][i] = (param_data.closeprice[i] - param_data.openprice[i])*f_pip_size(param_ins=param_data['symbol'][i])
        else:
            param_data['pips'][i] = (param_data.openprice[i] - param_data.closeprice[i])*f_pip_size(param_ins=param_data['symbol'][i])
    
    param_data['pips_acm'] = np.zeros(len(param_data['type']))
    param_data['profit_acm'] = np.zeros(len(param_data['type']))    
    param_data['pips_acm'][0] = param_data['pips'][0]
    param_data['profit_acm'][0] = param_data['profit'][0]
            
    for i in range(1,len(param_data['pips'])):
         param_data['pips_acm'][i] = param_data['pips_acm'][i-1] + param_data['pips'][i]
         param_data['profit_acm'][i] = param_data['profit_acm'][i-1] + param_data['profit'][i]
        
    return param_data
    

    
#%% FUNCION:
def f_estadisticas_ba(param_data):
    
    medida = ['Ops totales', 'Ganadoras', 'Ganadoras_c', 'Ganadoras_v', 'Perdedoras', 'Perdedoras_c', 'Perdedoras_v', 
              'Media(profit)','Media(pips)', 'r_efectividad', 'r_proporcion', 'r_efectividad_c','r_efectividad_v']
    descripcion = ['Operaciones Totales', 'Operaciones Ganadoras', 'Operaciones Ganadoras de Compra', 'Operaciones Ganadoras de Venta',
                   'Operaciones Perdedoras', 'Operaciones Perdedoras de Compra', 'Operaciones Perdedoras de Venta', 'Mediana de Profit de Operaciones', 
                   'Media de Pips de Operaciones', 'Ganadoras Totales / Operaciones Totales', 'Perdedoras Totales / Ganadoras Totales',
                   'Ganadoras Compras / Operaciones Totales', 'Ganadoras Ventas / Operaciones Totales']
   
    zero_data = np.zeros(shape=(len(descripcion),3))
    df_1_tabla = pd.DataFrame(zero_data, columns = ['medida', 'valor', 'descripcion'])
    
    df_1_tabla['valor'][0] = len(param_data['profit'])
    df_1_tabla['valor'][1] = param_data['profit'].gt(0).sum()
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'buy' and param_data['profit'][i] > 0 :
            x = x+1
    df_1_tabla['valor'][2] = x
    
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'sell' and param_data['profit'][i] > 0 :
            x = x+1
    df_1_tabla['valor'][3] = x
    df_1_tabla['valor'][4] = df_1_tabla['valor'][0] - df_1_tabla['valor'][1]
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'buy' and param_data['profit'][i] < 0 :
            x = x+1
    df_1_tabla['valor'][5] = x
    
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'sell' and param_data['profit'][i] < 0 :
            x = x+1
    df_1_tabla['valor'][6] = x
    df_1_tabla['valor'][7] = param_data.profit.median()
    df_1_tabla['valor'][8] = param_data.pips.median()
    df_1_tabla['valor'][9] = df_1_tabla['valor'][0] / df_1_tabla['valor'][1]
    df_1_tabla['valor'][10] = df_1_tabla['valor'][1] / df_1_tabla['valor'][4]
    df_1_tabla['valor'][11] = df_1_tabla['valor'][0] / df_1_tabla['valor'][2] 
    df_1_tabla['valor'][12] = df_1_tabla['valor'][0] / df_1_tabla['valor'][3] 
    
    for i in range(0,len(medida)):
        df_1_tabla['medida'][i] = medida[i]
        df_1_tabla['descripcion'][i] = descripcion[i]
        
    return df_1_tabla


        
#%% FUNCIÓN:

def f_columnas_capital_acum(param_data):
    param_data['capital_acm'] = np.zeros(len(param_data['type']))
    param_data['capital_acm'][0] = 5000+param_data['profit'][0]
      
    param_data['rendimiento_log'] = np.zeros(len(param_data['type']))
    param_data['rendimiento_log'][0] = math.log(param_data['capital_acm'][0] / 5000)
      
    for i in range(1,len(param_data['pips'])):
         param_data['capital_acm'][i] = param_data['capital_acm'][i-1] + param_data['profit'][i]
         param_data['rendimiento_log'][i] = math.log(param_data['capital_acm'][i] / param_data['capital_acm'][i-1])

    return param_data  

#%%
def f_estadisticas_mad(param_data):
    rf = .08/12
    metrica = ['sharpe', 'sortino_c','sortino_v','drawdown_capi_c','drawdown_capi_v',
              'drawdown_pips_c','drawdown_pips_v','information_r']
    descripcion = ['Sharpe Ratio', 'Sortino Ratio para Posiciones','Sortino Ratio para Posiciones de Venta',
                   'DrawDown de Capital', 'DrawDown de Pips','DrawUp de Pips','Information Ratio']
   
    zero_data = np.zeros(shape=(len(descripcion),3))
    df_estadisticas = pd.DataFrame(zero_data, columns = ['metrica', 'valor', 'descripcion'])
    
    for i in range(0,len(metrica)):
        df_estadisticas['metrica'][i] = metrica[i]
        df_estadisticas['descripcion'][i] = descripcion[i]

    df_estadisticas['valor'][0] = (param_data['rendimiento_log'].mean()-rf) / param_data['rendimiento_log'].std()
    df_estadisticas['valor'][1] = 
    df_estadisticas['valor'][2] = 
    df_estadisticas['valor'][3] = 
    df_estadisticas['valor'][4] = 
    df_estadisticas['valor'][5] = 
    df_estadisticas['valor'][6] = 
    df_estadisticas['valor'][7] = 
    
