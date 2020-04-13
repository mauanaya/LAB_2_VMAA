# -- ------------------------------------------------------------------------------------ -- #
# -- proyecto: Microestructura y Sistemas de Trading - Laboratorio 2 - Behavioral Finance
# -- archivo: funciones.py - procesamiento de datos
# -- mantiene: mauanaya
# -- repositorio: https://github.com/mauanaya/LAB_2_VMAA
# -- ------------------------------------------------------------------------------------ -- #

import pandas as pd
import numpy as np
import math as math
from  datetime import date, timedelta
import yfinance as yf 
from oandapyV20 import API                                
import oandapyV20.endpoints.instruments as instruments
import preciosmasivos as pm

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
    param_archivo =  'nombre del archivo.xlsx'

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
    
    # encontrar y eliminar diferentes "typos" que dificultan la lectura
    inst = param_ins.replace('_', '')

    
    # transformar a minusculas
    inst = inst.lower()
    
    #lista de pips por instrumento
    pips_inst =  {'audusd' : 10000, 'gbpusd': 10000, 'xauusd': 10, 'eurusd': 10000, 'xaueur': 10,
                  'nas100usd': 10, 'us30usd': 10, 'mbtcusd':100, 'usdmxn': 10000, 'eurjpy':100, 
                  'gbpjpy':100, 'usdjpy':100, 'btcusd':10, 'eurgbp':10000, 'usdcad':10000, 'spx500usd':10}
    
    return pips_inst[param_ins]

#%% FUNCION:
def f_columnas_tiempo(param_data):
    """
    Parameters
    ----------
    param_data : DataFrame con historico de la cuenta
    
    Returns
    -------
    param_data : DataFrame original con columna 'tiempo' agregada 
    
    Debugging
    ---------
    param_data = f_leer_archivo("archivo_tradeview_1.xlsx")
    """

    #convertir columna de 'closetime' y 'opentime' utilizando pd.to_datetime
    param_data['closetime'] = pd.to_datetime(param_data['closetime'])
    param_data['opentime'] = pd.to_datetime(param_data['opentime'])
    
    #se calcula el tiempo que cada operación estuvo abierta y se agrega como 'tiempo' 
    param_data['tiempo'] = [(param_data.loc[i, 'closetime'] - param_data.loc[i, 'opentime']).delta / 1*np.exp(9)
    for i in range(0, len(param_data['closetime']))]
    
    return param_data
    
#%% FUNCION:
def f_columnas_pips(param_data):
    """
    Parameters
    ----------
    datos : DataFrame original con la columna 'tiempo'
    Returns
    -------
    param_data : DataFrame original con columnas 'pips' y 'pips acumulados' agregadas
    
    Debugging
    ---------
    datos =  f_leer_archivo("archivo_tradeview_1.xlsx")
    
    """
    
    param_data['pips'] = np.zeros(len(param_data['type']))
    
    #el siguiente ciclo buscará el tipo de operación (buy o sell) y calculará los pips por operación
    for i in range(0,len(param_data['type'])):
        if param_data['type'][i] == 'buy':
            param_data['pips'][i] = (param_data.closeprice[i] - param_data.openprice[i])*f_pip_size(param_ins=param_data['symbol'][i])
        else:
            param_data['pips'][i] = (param_data.openprice[i] - param_data.closeprice[i])*f_pip_size(param_ins=param_data['symbol'][i])
            
    #se crean columnas llenas de 0's para después reemplazarlas con la información correspondiente
    param_data['pips_acm'] = np.zeros(len(param_data['type']))
    param_data['profit_acm'] = np.zeros(len(param_data['type']))    
    param_data['pips_acm'][0] = param_data['pips'][0]
    param_data['profit_acm'][0] = param_data['profit'][0]

        #el siguiente ciclo llenará las columnas 'pips acumulados' y 'profit acumulados'        
    for i in range(1,len(param_data['pips'])):
         param_data['pips_acm'][i] = param_data['pips_acm'][i-1] + param_data['pips'][i]
         param_data['profit_acm'][i] = param_data['profit_acm'][i-1] + param_data['profit'][i]
        
    return param_data
    

    
#%% FUNCION:
def f_estadisticas_ba(param_data):
    """
    Parameters
    ----------
    datos : DataFrame original con columnas agregadas hasta este momento (arriba generadas)
    
    Returns
    -------
    Diccionario 'stats': contiene 2 DataFrames
    Dos DataFrames:
    df_1_tabla : DataFrame con estadísticas básicas de la cuenta
    df_2_ranking : DataFrame con un ranking de efectividad por divisa
        
    Debugging
    ---------
    datos = f_leer_archivo("archivo_tradeview_1.xlsx")
    """
    #variables con elementos que se usarán para el llenado de un DataFrame
    medida = ['Ops totales', 'Ganadoras', 'Ganadoras_c', 'Ganadoras_v', 'Perdedoras', 'Perdedoras_c', 'Perdedoras_v', 
              'Media(profit)','Media(pips)', 'r_efectividad', 'r_proporcion', 'r_efectividad_c','r_efectividad_v']
    descripcion = ['Operaciones Totales', 'Operaciones Ganadoras', 'Operaciones Ganadoras de Compra', 'Operaciones Ganadoras de Venta',
                   'Operaciones Perdedoras', 'Operaciones Perdedoras de Compra', 'Operaciones Perdedoras de Venta', 'Media de Profit de Operaciones', 
                   'Media de Pips de Operaciones', 'Ganadoras Totales / Operaciones Totales', 'Perdedoras Totales / Ganadoras Totales',
                   'Ganadoras Compras / Operaciones Totales', 'Ganadoras Ventas / Operaciones Totales']
    
    #se genera una tabla 'vacía' para poder realizar un nuevo DataFrame
    zero_data = np.zeros(shape=(len(descripcion),3))
    #se asignan los titulos de cada columna
    df_1_tabla = pd.DataFrame(zero_data, columns = ['Medida', 'Valor', 'Descripcion'])
    
    #a partir de aquí se empezará a hacer el llenado de cada posición de la columna 'Valor'
    #cuenta el número de operaciones
    df_1_tabla['Valor'][0] = len(param_data['profit'])
    
    #cuenta las operaciones con profit mayor a 0, es decir, ganadoras
    df_1_tabla['Valor'][1] = param_data['profit'].gt(0).sum()
    
    #cuenta las operaciones con profit mayor a 0 para el tipo de operación 'buy'
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'buy' and param_data['profit'][i] > 0 :
            x = x+1
    df_1_tabla['Valor'][2] = x
   
    #cuenta las operaciones con profit mayor a 0 para el tipo de operación 'sell'
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'sell' and param_data['profit'][i] > 0 :
            x = x+1
    df_1_tabla['Valor'][3] = x
    
    #cuenta las operaciones con un profit negativo, es decir, operaciones perdedoras
    df_1_tabla['Valor'][4] = df_1_tabla['Valor'][0] - df_1_tabla['Valor'][1]
    
    #cuenta las operaciones perdedoras del tipo 'buy'
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'buy' and param_data['profit'][i] < 0 :
            x = x+1
    df_1_tabla['Valor'][5] = x
    
    #cuenta las operaciones perdedoras del tipo 'sell'
    x = 0
    for i in range(0,len(param_data['type'])): 
        if param_data['type'][i] == 'sell' and param_data['profit'][i] < 0 :
            x = x+1
    df_1_tabla['Valor'][6] = x
    
    #calcula el promedio del profit
    df_1_tabla['Valor'][7] = param_data.profit.median()
    #calcula el promedio de pips
    df_1_tabla['Valor'][8] = param_data.pips.median()
    #calcula un ratio de efectivdad para operaciones ganadoras
    df_1_tabla['Valor'][9] = df_1_tabla['Valor'][0] / df_1_tabla['Valor'][1]
    #calcula un ratio de proporcion tomando en cuenta las operaciones perdedoras
    df_1_tabla['Valor'][10] = df_1_tabla['Valor'][1] / df_1_tabla['Valor'][4]
    #calcula un ratio de efectivdad para operaciones ganadoras de tipo buy
    df_1_tabla['Valor'][11] = df_1_tabla['Valor'][0] / df_1_tabla['Valor'][2]
    #calcula un ratio de efectivdad para operaciones ganadoras de tipo sell
    df_1_tabla['Valor'][12] = df_1_tabla['Valor'][0] / df_1_tabla['Valor'][3] 
    
    #ciclo que llena las columnas medida y descripción con el contenido del arreglo creado en las líneas 161 y 163
    for i in range(0,len(medida)):
        df_1_tabla['Medida'][i] = medida[i]
        df_1_tabla['Descripcion'][i] = descripcion[i]

    #se compilan los valores únicos de los pares de divisas con los que se operó
    symbols = param_data["symbol"].unique().tolist()
    zero_data = np.zeros(shape=(len(symbols),2))
    
    #se define el nombre de las columnas del DataFrame que se generará
    df_2_ranking = pd.DataFrame(zero_data, columns = ['Symbol', 'Rank'])    
    
    #se asginan los diferentes valores únicos que se encontraron de los pares de divisas
    for i in range(0,len(symbols)):
        df_2_ranking['Symbol'][i] = symbols[i]
    
    #se genera un ciclo para contar las operaciones ganadoras y guardarlas
    for i in range(0,len(df_2_ranking['Symbol'])):
        win =  0
        total = 0
        for k in range (0,len(param_data['symbol'])):
            if df_2_ranking['Symbol'][i] == param_data['symbol'][k]:
                total = total + 1
                if param_data['profit'][k] > 0:
                    win = win + 1
        
        #el ranking está dado por la cantidad de veces que se ganó vs la cantidad de veces que se operó
        df_2_ranking['Rank'][i] = win / total
        df_2_ranking = df_2_ranking.sort_values(by = 'Rank', ascending = False)
    
    #se crea un diccionario con 2 llaves para poder accesar a los DataFrames 'df_1_tabla' o 'df_2_ranking'
    dicci = {'df_2_ranking':df_2_ranking, 'df_1_tabla':df_1_tabla}
        
    return dicci


        
#%% FUNCIÓN:
def f_capital_acum(param_data):
    """
    Parameters
    ----------
    datos : DataFrame original con columnas agregadas hasta este momento (arriba generadas)
    Returns
    -------
    datos : DataFrame original con columna 'capital_acm' agregada
    
    Debugging
    ---------
    datos = f_leer_archivo("archivo_tradeview_1.csv")
    """
    capital = 5000
    
    #se crea una columna 'capital_acm' y se llena con ceros
    param_data['capital_acm'] = np.zeros(len(param_data['type']))
    #se toma en cuenta el profit anterior y se suma con el actual para reemplazar los ceros y obtener el capital acumulado de la cuenta
    param_data['capital_acm'][0] = capital + param_data['profit'][0]
    
    #se calculan los pips resultantes por operación
    for i in range(1,len(param_data['pips'])):
         param_data['capital_acm'][i] = param_data['capital_acm'][i-1] + param_data['profit'][i]

    return param_data  

#%% FUNCION
def f_profit_diario(param_data):
    """
    Parameters
    ----------
    datos : DataFrame original con columnas agregadas hasta este momento (arriba generadas)
  
    Returns
    -------
    datos : 4 DataFrames
    df_profit_diario: DataFrame con información como el profit diario, capital acumulado, 
    rendimientos logaritmicos tanto de la cuenta como del benchmark y el 'Tracking error'.
    
    df_profit_diario_c: DataFrame con misma información (profit diario, capital acumulado, 
    rendimientos logaritmicos) pero sólo para las operaciones tipo 'buy'.
    
    df_profit_diario_v: DataFrame con misma información (profit diario, capital acumulado, 
    rendimientos logaritmicos) pero sólo para las operaciones tipo 'sell'.
    
    df_sp: DataFrame con información descargada del indice tomado como benchmark (S&P 500)
  
    Debugging
    ---------
    datos = f_leer_archivo("archivo_tradeview_1.xlsx")
    """
    capital = 5000
    
    s_date = param_data["closetime"][0].date()   # start date
    e_date = param_data["closetime"][len(param_data["closetime"])-1].date()  # end date
    
    delta = e_date - s_date       #timedelta
    
    #se descargam precios historicos del Benchmark, directamente de Yahoo Finance
    sp = yf.download('^gspc', start=s_date, end=e_date, progress=False)
    sp.head()
    sp = sp.reset_index()
     
    #se obtienen los rendimientos logaritmicos del Benchmark: S&P 500
    sp['Rendimientos Log'] = np.zeros(len(sp['Adj Close']))
    for i in range (1,len(sp['Adj Close'])):
        sp['Rendimientos Log'][i] = np.log(sp['Adj Close'][i]/sp['Adj Close'][i-1])
        
    #se genera una tabla vacía de 6 columnas y con longitud de los días que se operó
    zero_data = np.zeros(shape=(delta.days+1,6))
    #se asignan los nombres de las columnas del DataFrame que contendrá calculos e información en términos diarios
    df_profit_d = pd.DataFrame(zero_data, columns = ['Timestamp','Profit Diario',
                                                     'Capital Acumulado', 'Rendimientos Log','Rend Log SP',
                                                     'Tracking Error'])
        
    #se genera una tabla vacía de 4 columnas y con longitud de los días que se operó
    zero_data = np.zeros(shape=(delta.days+1,4))
    #se asignan los nombres de las columnas del DataFrame que contendrá calculos e información en términos diarios para operaciones de tipo 'buy'
    df_profit_dc = pd.DataFrame(zero_data, columns = ['Timestamp','Profit Diario',
                                                     'Capital Acumulado', 'Rendimientos Log'])
    #se asignan los nombres de las columnas del DataFrame que contendrá calculos e información en términos diarios para operaciones de tipo 'sell'
    df_profit_dv = pd.DataFrame(zero_data, columns = ['Timestamp','Profit Diario',
                                                     'Capital Acumulado', 'Rendimientos Log'])
    
    #se asignan las fechas correspondientes a cada operación
    for i in range(0,delta.days+1):
        df_profit_d["Timestamp"][i]= s_date + timedelta(days=i)
        df_profit_dc["Timestamp"][i]= s_date + timedelta(days=i)
        df_profit_dv["Timestamp"][i]= s_date + timedelta(days=i)
        
    #este ciclo compilará todas las operaciones que se hicieron en cada día  
    for i in range (0,len(df_profit_d["Timestamp"])):
        a = 0
        b = 0
        c = 0
        
        #se comparan las fechas del archivo historico contra las generadas en las líneas 347:350
        for k in range (0,len(param_data["closetime"])):
            if df_profit_d["Timestamp"][i] == param_data["closetime"][k].date():
                a = a + param_data["profit"][k]
                
                #se asignan las operaciones por día para el tipo 'buy'
                if param_data['type'][k] == 'buy':
                    b = b + param_data["profit"][k]
                
                #se asignan las operaciones por día para el tipo 'sell'    
                elif param_data['type'][k] == 'sell':
                    c = c + param_data["profit"][k]
                    
        #se asigna la información correspondiente a cada DataFrame generado 
        df_profit_d["Profit Diario"][i] = a #DataFrame con profit y otros datos generales diarios
        df_profit_dc["Profit Diario"][i] = b #DataFrame con profit y otros datos diarios para operaciones 'buy'
        df_profit_dv["Profit Diario"][i] = c #DataFrame con profit y otros datos diarios para operaciones 'sell'
    
    #este ciclo agregará una columna con los rendimientos logarítmicos del benchmark
    for i in range (0,len(df_profit_d["Timestamp"])):
        for k in range (0,len(sp["Date"])):
            if df_profit_d["Timestamp"][i] == sp["Date"][k].date():
                df_profit_d["Rend Log SP"][i] = sp["Rendimientos Log"][k]
                
    #se reacomoda el índice para que las fechas estén en orden cronológico
    df_profit_d = df_profit_d.sort_values(by=['Timestamp'])
    df_profit_d = df_profit_d.reset_index(drop=True)
    
    #se reacomoda el índice para que las fechas estén en orden cronológico
    df_profit_dc = df_profit_dc.sort_values(by=['Timestamp'])
    df_profit_dc = df_profit_dc.reset_index(drop=True)

    #se reacomoda el índice para que las fechas estén en orden cronológico    
    df_profit_dv = df_profit_dv.sort_values(by=['Timestamp'])
    df_profit_dv = df_profit_dv.reset_index(drop=True)
    
    #se define el primer elemento del DataFrame con información general diaria
    df_profit_d['Capital Acumulado'][0] = capital + df_profit_d['Profit Diario'][0]
    df_profit_d['Rendimientos Log'][0] = np.log(df_profit_d['Capital Acumulado'][0]/capital)
    
    #se define el primer elemento del DataFrame con información general diaria para operaciones 'buy'    
    df_profit_dc['Capital Acumulado'][0] = capital + df_profit_dc['Profit Diario'][0]
    df_profit_dc['Rendimientos Log'][0] = np.log(df_profit_dc['Capital Acumulado'][0]/capital)
    
    #se define el primer elemento del DataFrame con información general diaria para operaciones 'sell'
    df_profit_dv['Capital Acumulado'][0] = capital + df_profit_dv['Profit Diario'][0]
    df_profit_dv['Rendimientos Log'][0] = np.log(df_profit_dv['Capital Acumulado'][0]/capital)
            
    #este ciclo llenará los valores correspondientes a las columnas 'Capital Acumulado', 'Rendimientos Log' y 'Tracking Error'
    #de cada DataFrame antes mencionado.
    for i in range(1,len(df_profit_d["Profit Diario"])):
         df_profit_d['Capital Acumulado'][i] = df_profit_d['Capital Acumulado'][i-1] + df_profit_d['Profit Diario'][i]
         df_profit_d['Rendimientos Log'][i] = np.log(df_profit_d['Capital Acumulado'][i]/df_profit_d['Capital Acumulado'][i-1])
         df_profit_d['Tracking Error'][i] = df_profit_d["Rendimientos Log"][i]-df_profit_d["Rend Log SP"][i]
         
         df_profit_dc['Capital Acumulado'][i] = df_profit_dc['Capital Acumulado'][i-1] + df_profit_dc['Profit Diario'][i]
         df_profit_dc['Rendimientos Log'][i] = np.log(df_profit_dc['Capital Acumulado'][i]/df_profit_dc['Capital Acumulado'][i-1])
         
         df_profit_dv['Capital Acumulado'][i] = df_profit_dv['Capital Acumulado'][i-1] + df_profit_dv['Profit Diario'][i]
         df_profit_dv['Rendimientos Log'][i] = np.log(df_profit_dv['Capital Acumulado'][i]/df_profit_dv['Capital Acumulado'][i-1])
         
    m = 0
    c = 0
    #debido a que Forex no opera los sábados, se debe eliminar de los días contemplados. 
    #en esta cuenta el día 1 es martes, por lo tanto el día 5 es sábado)
    while m != 5:
        m = df_profit_d["Timestamp"][c].weekday()
        c = c + 1
    
    lista = []
    
    for i in range (c-1,len(df_profit_d["Timestamp"])+1,7):
        lista.append(i)
        
    #se asigna como indice, la lista generada arriba con los días adecuados para cada operación
    df_profit_d = df_profit_d.drop(df_profit_d.index[lista])
    df_profit_dc = df_profit_dc.drop(df_profit_dc.index[lista])
    df_profit_dv = df_profit_dv.drop(df_profit_dv.index[lista])

    df_profit_d = df_profit_d.reset_index(drop=True)
    df_profit_dc = df_profit_dc.reset_index(drop=True)
    df_profit_dv =  df_profit_dv.reset_index(drop=True) 
         
    #se genera un diccionario con 4 llaves que corresponden a cada DataFrame mencionado anteriormente           
    dicci = {"df":df_profit_d ,"df_c":df_profit_dc,"df_v":df_profit_dv,"sp":sp}
        
    return dicci     
            


#%%
def f_estadisticas_mad(param_data,param_data_1,param_data_2):
    """
    Parameters
    ----------
    datos : DataFrame original con columnas agregadas hasta este momento (arriba generadas)
    
    Returns
    -------
    datos : DataFrame (df_mad) con diferentes medidas de atribucion al desempeño
    Debugging
    ---------
    datos = 'f_leer_archivo("archivo_tradeview_1.csv")
    
    """
    rf = .08/300
    mar = .30/300
    
    #variables con elementos que se usarán para el llenado de un DataFrame
    metrica = ['Sharpe', 'Sortino_c','Sortino_v','Drawdown_cap',
               'Drawup_cap','Information_r']
    descripcion = ['Sharpe Ratio', 'Sortino Ratio para Posiciones de Compra',
                   'Sortino Ratio para Posiciones de Venta','DrawDown de Capital',
                   'DrawUp de Capital','Information Ratio']
   
    #se genera un tabla vacía, con 3 columnas
    zero_data = np.zeros(shape=(len(descripcion),3))
    #se asignan los nombres de cada columna
    df_mad = pd.DataFrame(zero_data, columns = ['Métrica', 'Valor', 'Descripción'])
    
    #este ciclo llena la columna metrica con cada elemento dentro de las variables metrica y descripción
    for i in range(0,len(metrica)):
        df_mad['Métrica'][i] = metrica[i]
        df_mad['Descripción'][i] = descripcion[i]
    
    
    MAX = 0
    fecha_i = 0
    fecha_f = 0
    #el siguiente ciclo calculará el DrawDown de la cuenta. Es decir, obtendrá la mayor pérdida flotante que se tuvo en la cuenta 
    for i in range(0,len(param_data['Capital Acumulado'])-1):
        k = i+1
        maximo = 0
        fecha_inicial = 0
        fecha_final = 0
        while param_data['Capital Acumulado'][i] > param_data['Capital Acumulado'][k]:
            if param_data['Capital Acumulado'][i] - param_data['Capital Acumulado'][k] > maximo:
                maximo = param_data['Capital Acumulado'][i] - param_data['Capital Acumulado'][k]
                fecha_inicial = param_data['Timestamp'][i]
                fecha_final = param_data['Timestamp'][k]
            k = k+1
        if maximo > MAX:
            MAX = maximo
            fecha_i = fecha_inicial
            fecha_f = fecha_final

    MIN = 0
    fecha_i_up = 0
    fecha_f_up = 0
    #el siguiente ciclo calculará el DrawUp de la cuenta. Es decir, obtendrá la mayor ganancia flotante que se tuvo en la cuenta
    for i in range(0,len(param_data['Capital Acumulado'])-1):
        k = i+1
        minimo = 0
        fecha_inicial_up = 0
        fecha_final_up = 0
        while k < len(param_data['Capital Acumulado']) and param_data['Capital Acumulado'][k] > param_data['Capital Acumulado'][i]:
            if param_data['Capital Acumulado'][k] - param_data['Capital Acumulado'][i] > minimo:
                minimo = param_data['Capital Acumulado'][k] - param_data['Capital Acumulado'][i]
                fecha_inicial_up = param_data['Timestamp'][i]
                fecha_final_up = param_data['Timestamp'][k]
            k = k+1
        if minimo > MIN:
            MIN = minimo
            fecha_i_up = fecha_inicial_up
            fecha_f_up = fecha_final_up

    #se llena el elemento 0 del DataFrame con el ratio Sharpe
    df_mad['Valor'][0] = (param_data['Rendimientos Log'].mean() - rf)/ param_data['Rendimientos Log'].std()
    #se llena el elemento 1 del DataFrame con el ratio Sortino, sólo para operaciones tipo 'buy'
    df_mad['Valor'][1] = (param_data['Rendimientos Log'].mean() - mar)/param_data_1['Rendimientos Log'].lt(mar).std()
    #se llena el elemento 2 del DataFrame con el ratio Sortino, sólo para operaciones tipo 'sell'
    df_mad['Valor'][2] = (param_data['Rendimientos Log'].mean() - mar)/param_data_2['Rendimientos Log'].lt(mar).std()
    #se llena el elemento 3 del DataFrame con el DrawDown
    df_mad['Valor'][3] = ' Fecha Inicial: ' + str(fecha_i) + ' Fecha Final: ' + str(fecha_f) + ' DrawDown: ' + str(MAX)
    #se llena el elemento 4 del DataFrame con el DrawUp
    df_mad['Valor'][4] = ' Fecha Inicial: ' + str(fecha_i_up) + ' Fecha Final: ' + str(fecha_f_up) + ' DrawUp: ' + str(MIN)
    #se llena el elemento 5 del DataFrame con el information ratio
    df_mad['Valor'][5] = (param_data['Rendimientos Log'].mean() - param_data['Rend Log SP'].mean()) / param_data['Tracking Error'].std()    
    
    return df_mad

#%% FUNCION:
#def f_be_de(param_data):
    """
    Parameters
    ----------
    datos : DataFrame original con columnas agregadas hasta este momento (arriba generadas)
    
    Returns
    -------
    datos : DataFrame con diferentes medidas de atribucion al desempeño
    Debugging
    ---------
    datos = 'f_leer_archivo("archivo_tradeview_1.csv")
    
    """
#    capital = 5000
#    
#    #diccionaro con valores de tamaño de pips por par de divisa
#    pips_inst =  {'audusd' : 10000, 'gbpusd': 10000, 'xauusd': 10, 'eurusd': 10000, 'xaueur': 10,
#                  'nas100usd': 10, 'us30usd': 10, 'mbtcusd':100, 'usdmxn': 10000, 'eurjpy':100, 
#                  'gbpjpy':100, 'usdjpy':100, 'btcusd':10, 'eurgbp':10000, 'usdcad':10000, 'spx500usd':10}
#    #diccionario con los nombres "reales" de los pares de divisas utilizados en la cuenta
#    dix_instrumentos =  {'audusd' : 'AUD_USD', 'gbpusd': 'GBP_USD', 'xauusd': 'XAU_USD', 'eurusd': 'EUR_USD', 'xaueur': 'XAU_EUR',
#                      'nas100usd': 'NAS100_USD', 'us30usd': 'US30_USD', 'mbtcusd': 'MTBC_USD', 'usdmxn': 'USD_MXN', 'eurjpy': 'EUR_JPY', 
#                      'gbpjpy': 'GBP_JPY', 'usdjpy': 'USD_JPY', 'btcusd': 'BTC_USD',  'eurgbp': 'EUR_GBP', 'usdcad': 'USD_CAD', 'spx500usd':'SPX500_USD'}
#    #token para descargar precios desde Oanda
#    OA_Ak = 'cda5127f163305921b7ac4fbeb513c08-25deeea97c3ca2fc2bac1d50093a7c68'
#    #granularidad (frecuencia) de los precios
#    OA_Gn = "H1"
#        
#def f_be_de(param_data):
#    
#    pips_inst =  {'audusd' : 10000, 'gbpusd': 10000, 'xauusd': 10, 'eurusd': 10000, 'xaueur': 10,
#              'nas100usd': 10, 'us30usd': 10, 'mbtcusd':100, 'usdmxn': 10000, 'eurjpy':100, 
#              'gbpjpy':100, 'usdjpy':100, 'btcusd':10, 'eurgbp':10000, 'usdcad':10000}
#    
#    dix_instrumentos =  {'audusd' : 'AUD_USD', 'gbpusd': 'GBP_USD', 'xauusd': 'XAU_USD', 'eurusd': 'EUR_USD', 'xaueur': 'XAU_EUR',
#                      'nas100usd': 'NAS_100_USD', 'us30usd': 'US_30_USD', 'mbtcusd': 'MTB_USD', 'usdmxn': 'USD_MXN', 'eurjpy': 'EUR_JPY', 
#                      'gbpjpy': 'GBP_JPY', 'usdjpy': 'USD_JPY', 'btcusd': 'BTC_USD',  'eurgbp': 'EUR_GBP', 'usdcad': 'USD_CAD'}

#%%
def f_be_de(param_data):
    
    pips_inst =  {'audusd' : 10000, 'gbpusd': 10000, 'xauusd': 10, 'eurusd': 10000, 'xaueur': 10,
              'nas100usd': 10, 'us30usd': 10, 'mbtcusd':100, 'usdmxn': 10000, 'eurjpy':100, 
              'gbpjpy':100, 'usdjpy':100, 'btcusd':10, 'eurgbp':10000, 'usdcad':10000}
    
    dix_instrumentos =  {'audusd' : 'AUD_USD', 'gbpusd': 'GBP_USD', 'xauusd': 'XAU_USD', 'eurusd': 'EUR_USD', 'xaueur': 'XAU_EUR',
                      'nas100usd': 'NAS_100_USD', 'us30usd': 'US_30_USD', 'mbtcusd': 'MTB_USD', 'usdmxn': 'USD_MXN', 'eurjpy': 'EUR_JPY', 
                      'gbpjpy': 'GBP_JPY', 'usdjpy': 'USD_JPY', 'btcusd': 'BTC_USD',  'eurgbp': 'EUR_GBP', 'usdcad': 'USD_CAD'}


    OA_Ak = '630eb7b990acb33732bc201e28bf0d80-52218bf769c37309bee9440932b0f008'
    OA_Gn = "M1"
    
    param_data['capital_ganadora'] = np.zeros(len(param_data['closetime']))
    param_data['capital_perdedora'] = np.zeros(len(param_data['closetime']))
    param_data["instrumento_perdedora"] = np.zeros(len(param_data['closetime']))
    param_data["sentido_perdedora"] = np.zeros(len(param_data['closetime']))
    param_data["volumen_perdedora"] = np.zeros(len(param_data['closetime']))
    param_data["capital_flotante"] = np.zeros(len(param_data['closetime']))
    
    param_data = param_data.sort_values(by="closetime")
    param_data = param_data.reset_index(drop=True)
    
    param_data['capital_acm'][0] = 5000+param_data['profit'][0]
            
    for i in range(1,len(param_data['pips'])):
         param_data['capital_acm'][i] = param_data['capital_acm'][i-1] + param_data['profit'][i]

    
    
    for i in range(0,len(param_data["closetime"])):
        maxperdida = 0
        b=0
        if param_data['profit'][i] > 0:
            for j in range(i+1,len(param_data["closetime"])):
                if param_data['opentime'][j] < param_data['closetime'][i] and param_data['closetime'][i] < param_data['closetime'][j]:
                        OA_In = dix_instrumentos[param_data["symbol"][j]]                # Instrumento
                        try:
                            fini = pd.to_datetime(param_data['closetime'][i])  # Fecha inicial
                            ffin = pd.to_datetime(param_data['closetime'][i])  # Fecha final
                            print(fini,ffin)
                            df_pe = pm.f_precios_masivos(p0_fini=fini, p1_ffin=ffin, p2_gran=OA_Gn,
                                     p3_inst=OA_In, p4_oatk=OA_Ak, p5_ginc=4900)
                            
        
                            a = (pd.to_numeric(df_pe['Close'][0]) - param_data['openprice'][j]) * param_data['size'][j] * 10 * pips_inst[param_data['symbol'][j]]
                        except:
                            a = 0
                        b = b + a
                        if a < maxperdida:
                                maxperdida = a
                                indxmaxperdida = j
        if i == 0:
            
            param_data["capital_flotante"][i] = param_data["profit"][i] + b + 5000
        
        else:
            param_data["capital_flotante"][i] = param_data["profit"][i] + b + param_data["capital_acm"][i-1]
        
        if maxperdida < 0:                         
            param_data["capital_ganadora"][i] =  param_data["profit"][i]
            param_data["capital_perdedora"][i] =  maxperdida
            param_data["instrumento_perdedora"][i] = param_data["symbol"][indxmaxperdida]
            param_data["sentido_perdedora"][i] = param_data["type"][indxmaxperdida]
            param_data["volumen_perdedora"][i] = param_data["size"][indxmaxperdida]
            
    
    param_data =  param_data[param_data.capital_ganadora != 0]   
    param_data = param_data.reset_index(drop=True) 
    
    param_data = param_data[['closetime','symbol','size','type','capital_ganadora',
                            'instrumento_perdedora','volumen_perdedora','sentido_perdedora',
                            'capital_perdedora','capital_flotante']]
    
    param_data['ratio_cp_capital_acm'] = np.zeros(len(param_data['capital_ganadora']))
    param_data['ratio_cg_capital_acm'] = np.zeros(len(param_data['capital_ganadora']))
    param_data['ratio_cp_cg'] = np.zeros(len(param_data['capital_ganadora']))     
    
    for i in range (0,len(param_data['capital_ganadora'])):
        param_data['ratio_cp_capital_acm'][i] = abs(param_data['capital_perdedora'][i] / param_data['capital_flotante'][i]) 
        param_data['ratio_cg_capital_acm'][i] = param_data['capital_ganadora'][i] / param_data['capital_flotante'][i]
        param_data['ratio_cp_cg'][i] = abs(param_data['capital_perdedora'][i] / param_data['capital_ganadora'][i])
    
    zero_data = np.zeros(shape=(1,4))
    df = pd.DataFrame(zero_data, columns = ['Ocurrencias','Status_Quo','Aversion_Perdida', 
                                            'Sensibilidad_Decreciente'])
        
    df['Ocurrencias'][0] = len(param_data["capital_ganadora"])
    
    a = 0
    b = 0
    for i in range (0,len(param_data["capital_ganadora"])):
        if param_data["ratio_cp_capital_acm"][i] < param_data['ratio_cg_capital_acm'][i]:
            a = a+1
        if param_data["ratio_cp_cg"][i] > 1.5:
            b = b+1
            
    df['Status_Quo'][0] = a/df['Ocurrencias'][0]
    df['Aversion_Perdida'][0] = b/df['Ocurrencias'][0]
    
    o = df['Ocurrencias'][0] - 1
    
    if param_data["capital_flotante"][o] > param_data["capital_flotante"][0] and \
    (param_data["capital_ganadora"][o] > param_data["capital_ganadora"][0] or param_data["capital_perdedora"][o] > param_data["capital_perdedora"][0])\
     and param_data["ratio_cp_cg"][o] > 1.5: 
        df['Sensibilidad_Decreciente'][0] = "Si"
    else:
        df['Sensibilidad_Decreciente'][0] = "No"
        
      
        
        

    
    param_data['ocurrencias'] = np.zeros(len(param_data['capital_ganadora']))
    
    for i in range (0,len(param_data['capital_ganadora'])):
        param_data['ocurrencias'][i] = "Ocurrencia_" + str(i+1)
        
    param_data = param_data.set_index('ocurrencias')
    
    param_data = param_data.rename(columns={"closetime": "timestamp", "symbol": "instrumento_ganadora",
                                            "size": "volumen_ganadora", "type": "sentido_ganadora"})
    
    diccionario = {"Ocurrencias":param_data ,"DataFrame":df}
        
    return diccionario 










