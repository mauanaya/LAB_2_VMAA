# -- ------------------------------------------------------------------------------------ -- #
# -- proyecto: Microestructura y Sistemas de Trading - Laboratorio 2 - Behavioral Finance
# -- archivo: funciones.py - procesamiento de datos
# -- mantiene: mauanaya
# -- repositorio: https://github.com/mauanaya/LAB_2_VMAA
# -- ------------------------------------------------------------------------------------ -- #

import pandas as pd
import numpy as np

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
                  'nas100usd': 10, 'us30usd': 10, 'mbtcusd':100, 'usdmxn': 10000, 'eurjpy':10000, 
                  'gbpjpy':10000, 'usdjpy':10000, 'btcusd':10, 'eurgbp':10000, 'usdcad':10000,}
    
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
    df_1_tabla = pd.DataFrame(columns= 'medida', 'valor', 'descripcion')
    medida = ['Ops totales', 'Ganadoras', 'Ganadoras_c', 'Ganadoras_v', 'Perdedoras', 'perdedoras_c', 'Perdedoras_v', 
              'Media(profit)','Media(pips)', 'r_efectividad', 'r_proporcion', 'r_efectividad_c','r_efectividad_v']
    descripcion = ['Operaciones totales', 'operaciones ganadoras', 'operaciones ganadoras de compra', 'operaciones ganadoras de venta',
                   'operaciones perdedoras', 'operaciones perdedoras de compra', 'operaciones perdedoras de venta', 'mediana de profit de operaciones', 
                   'media de pips de operaciones', 'ganadoras totales / operaciones totales', 'perdedoras totales / operaciones totales',
                   'perdedoras totales / ganadoras totales', 'ganadoras compra / operaciones totales', 'ganadoras ventas / operaciones totales']
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
    df_1_tabla['valor'][9] = df_1_tabla['valor'][1] / df_1_tabla['valor'][0]
    df_1_tabla['valor'][10] = df_1_tabla['valor'][4] / df_1_tabla['valor'][1]
    df_1_tabla['valor'][11] = df_1_tabla['valor'][2] / df_1_tabla['valor'][0] 
    df_1_tabla['valor'][12] = df_1_tabla['valor'][3] / df_1_tabla['valor'][0] 
    
    for i in range(0,len(medida)):
        df_1_tabla['medida'][i] = medida[i]
        df_1_tabla['descripcion'][i] = descripcion[i]
    
        


    
   
    
