# -- ------------------------------------------------------------------------------------ -- #
# -- proyecto: Microestructura y Sistemas de Trading - Laboratorio 2 - Behavioral Finance
# -- archivo: visualizaciones.py - visualizacion de datos
# -- mantiene: mauanaya
# -- repositorio: https://github.com/mauanaya/LAB_2_VMAA
# -- ------------------------------------------------------------------------------------ -- #
import plotly.express as px
import principal as p

#%% GRAFICA: Ranking de pares de divisas
rank = p.stats['df_2_ranking']
fig = px.pie(rank)
fig.show()


