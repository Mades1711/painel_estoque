from datetime import datetime
import datetime as dt
import seaborn as sns
import base64
import io
from app import app
from flask import render_template
from flask_caching import Cache
import pyodbc
import pandas as pd
from decouple import config
import matplotlib
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings('ignore')
matplotlib.use('Agg')


ultima_movimentacao = """SELECT 
    X.ZZ6_RECEIT,
    X.STATUS,
    X.total,
    X.[data hora],
    X.TIPO
FROM (
    SELECT 
        ZZ6_RECEIT,
        CASE TRIM(ZZ6_ALTERA)
            WHEN 'Translado Laboratório-Loja' THEN 'Montagem finalizada'
            WHEN 'Translado Laboratório Externo -> Loja' THEN 'Montagem finalizada'
            WHEN 'Recebimento da loja p/ estoque (pós-venda)' THEN 'Recebimento Estoque'
            WHEN 'Entrada Laboratório' THEN 'Entrada Laboratório'
            WHEN 'Aguardando Compra das Lentes' THEN 'Aguardando Compra das Lentes'
            WHEN 'Compra realizada.' THEN 'Compra realizada'
            WHEN 'Translado estoque -> Laboratorio Externo ' THEN 'Translado estoque -> Laboratorio Externo'
            when 'Aguardando armação do cliente' THEN 'Aguardando armação'
            ELSE ZZ6_ALTERA 
        END AS STATUS,
        COUNT(DISTINCT ZZ6_RECEIT) AS total,
        CONVERT(DATETIME, CONCAT(ZZ6_DATA, ' ', ZZ6_HORA)) AS [data hora],
        ZZ4_TIPO AS TIPO,
        ROW_NUMBER() OVER (
            PARTITION BY ZZ6_RECEIT
            ORDER BY ZZ6_DATA DESC, ZZ6_HORA DESC
        ) AS row_num
    FROM ZZ6010 Z6
    LEFT OUTER JOIN ZZ4010 ZZ4 ON ZZ4_RECEIT = ZZ6_RECEIT AND ZZ4.D_E_L_E_T_ = ''
    LEFT OUTER JOIN ZZ5010 ZZ5 ON ZZ5_RECEIT = ZZ4_RECEIT AND ZZ5.D_E_L_E_T_ = ''
    WHERE ZZ6_ALTERA IN (
        'Recebimento da loja p/ estoque (pós-venda)',
        'Translado estoque -> Laboratorio Externo',
        'Translado Laboratório-Loja',
        'Translado Laboratório Externo -> Loja',
        'Compra realizada.',
        'Recebimento da loja p/ estoque (pós-venda)',
        'Aguardando Compra das Lentes',
        'Aguardando armação do cliente'
    )
    AND Z6.D_E_L_E_T_ = ''
    AND ZZ6_DATA BETWEEN '2023-01-01' AND GETDATE()
    AND ZZ5_CODPRO NOT LIKE '0001076%'
    AND ZZ5_CODPRO NOT LIKE '0001075%'
    AND ZZ5_CODPRO NOT LIKE '0001077%'
    AND ZZ5_CODPRO NOT LIKE '0001078%'
    AND ZZ4_TIPO <> 'S'
    GROUP BY 
        ZZ6_RECEIT,
        ZZ6_ALTERA,
        ZZ4_TIPO,
        ZZ6_DATA,
        ZZ6_HORA,
        ZZ6_FILIAL,
        ZZ4_DTPREV
) X
WHERE X.row_num = 1
ORDER BY X.ZZ6_RECEIT"""

montagens_por_dia = """SELECT 
    X.[data],
    SUM(X.total) AS total
FROM (
    SELECT 
        ZZ6_RECEIT,
        COUNT(DISTINCT ZZ6_RECEIT) AS total,
        CONVERT(DATE, ZZ6_DATA) AS [data],
        ROW_NUMBER() OVER (
            PARTITION BY ZZ6_RECEIT
            ORDER BY ZZ6_DATA DESC, ZZ6_HORA DESC
        ) AS row_num
    FROM ZZ6010 Z6
    LEFT OUTER JOIN ZZ4010 ZZ4 ON ZZ4_RECEIT = ZZ6_RECEIT AND ZZ4.D_E_L_E_T_ = ''
    LEFT OUTER JOIN ZZ5010 ZZ5 ON ZZ5_RECEIT = ZZ4_RECEIT AND ZZ5.D_E_L_E_T_ = ''
    WHERE ZZ6_ALTERA IN (
        'Translado Laboratório-Loja',
        'Translado Laboratório Externo -> Loja'
    )
    AND Z6.D_E_L_E_T_ = ''
    AND ZZ6_DATA BETWEEN {datainicial} AND {datafinal}
    AND ZZ5_CODPRO NOT LIKE '0001076%'
    AND ZZ5_CODPRO NOT LIKE '0001075%'
    AND ZZ5_CODPRO NOT LIKE '0001077%'
    AND ZZ5_CODPRO NOT LIKE '0001078%'
    AND ZZ4_TIPO = 'M'
    GROUP BY 
        ZZ6_RECEIT,
        ZZ6_ALTERA,
        ZZ4_TIPO,
        ZZ6_DATA,
        ZZ6_HORA,
        ZZ6_FILIAL,
        ZZ4_DTPREV
) X
WHERE X.row_num = 1
GROUP BY X.[data]
ORDER BY X.[data]
"""

movimentacao_dia = """SELECT
    CASE TRIM(ZZ6_ALTERA)
        WHEN 'Translado Laboratório-Loja' THEN 'Montagem finalizada'
        WHEN 'Translado Laboratório Externo -> Loja' THEN 'Montagem finalizada'
        WHEN 'Recebimento da loja p/ estoque (pós-venda)' THEN 'Recebimento Estoque'
        WHEN 'Entrada Laboratório' THEN 'Entrada Laboratório'
        WHEN 'Aguardando Compra das Lentes' THEN 'Aguardando Compra das Lentes'
        WHEN 'Compra realizada.' THEN 'Compra realizada'
        WHEN 'Translado estoque -> Laboratorio Externo ' THEN 'Translado estoque -> Laboratorio Externo'
        ELSE (ZZ6_ALTERA)
    END AS 'MOVIMENTAÇÃO',
    COUNT(DISTINCT(ZZ6_RECEIT)) AS 'total',
    ZZ4_TIPO as 'tipo'
FROM ZZ6010 Z6
LEFT OUTER JOIN ZZ4010 ZZ4 ON ZZ4_RECEIT = ZZ6_RECEIT AND ZZ4.D_E_L_E_T_ = ''
LEFT OUTER JOIN ZZ5010 ZZ5 ON ZZ5_RECEIT = ZZ4_RECEIT AND ZZ5.D_E_L_E_T_ = ''
WHERE ZZ6_ALTERA IN (
        'Recebimento da loja p/ estoque (pós-venda)',
        'Translado estoque -> Laboratorio Externo',
        'Translado Laboratório-Loja',
        'Translado Laboratório Externo -> Loja',
        'Compra realizada.',
        'Recebimento da loja p/ estoque (pós-venda)',
        'Aguardando Compra das Lentes'
    )
    AND Z6.D_E_L_E_T_ = ''
    AND ZZ6_DATA = {datainicial}
    AND ZZ5_CODPRO NOT LIKE '0001076%'
    AND ZZ5_CODPRO NOT LIKE '0001075%'
    AND ZZ5_CODPRO NOT LIKE '0001077%'
    AND ZZ5_CODPRO NOT LIKE '0001078%'
    AND Z6.D_E_L_E_T_ = ''
    AND ZZ4_TIPO <>'S'
GROUP BY
    CASE TRIM(ZZ6_ALTERA)
        WHEN 'Translado Laboratório-Loja' THEN 'Montagem finalizada'
        WHEN 'Translado Laboratório Externo -> Loja' THEN 'Montagem finalizada'
        WHEN 'Recebimento da loja p/ estoque (pós-venda)' THEN 'Recebimento Estoque'
        WHEN 'Entrada Laboratório' THEN 'Entrada Laboratório'
        WHEN 'Aguardando Compra das Lentes' THEN 'Aguardando Compra das Lentes'
        WHEN 'Compra realizada.' THEN 'Compra realizada'
        WHEN 'Translado estoque -> Laboratorio Externo ' THEN 'Translado estoque -> Laboratorio Externo'
        ELSE (ZZ6_ALTERA)
    END,
    ZZ4_TIPO,
    ZZ6_DATA,
    ZZ6_HORA,
    ZZ6_FILIAL,
    ZZ4_DTPREV,
    ZZ6_RECEIT,
    ZZ4_TIPO
"""

os_atrasadas = """SELECT 
    X.ZZ6_RECEIT as 'OS',
    X.STATUS,
    X.[DT última movimentação],
	X.[DT Prevista],
	X.[Dias atrasados]
FROM (
    SELECT 
        ZZ6_RECEIT,
        CASE TRIM(ZZ6_ALTERA)
            WHEN 'Translado Laboratório-Loja' THEN 'Montagem finalizada'
            WHEN 'Translado Laboratório Externo -> Loja' THEN 'Montagem finalizada'
            WHEN 'Recebimento da loja p/ estoque (pós-venda)' THEN 'Recebimento Estoque'
            WHEN 'Entrada Laboratório' THEN 'Entrada Laboratório'
            WHEN 'Aguardando Compra das Lentes' THEN 'Aguardando Compra das Lentes'
            WHEN 'Compra realizada.' THEN 'Compra realizada'
            WHEN 'Translado estoque -> Laboratorio Externo ' THEN 'Translado estoque -> Laboratorio Externo'
            WHEN 'Armação enviada pela loja para montagem no laboratorio' THEN 'Armação enviada para montagem'
            ELSE ZZ6_ALTERA 
        END AS STATUS,
        CONVERT(DATETIME, CONCAT(ZZ6_DATA, ' ', ZZ6_HORA)) AS [DT última movimentação],
        ROW_NUMBER() OVER (
            PARTITION BY ZZ6_RECEIT
            ORDER BY ZZ6_DATA DESC, ZZ6_HORA DESC
        ) AS row_num,
		convert(date,ZZ4_DTPREV) AS 'DT Prevista',
		DATEDIFF(DAY,CONVERT(DATE,ZZ4_DTPREV),CONVERT(DATE,ZZ6_DATA)) as 'Dias atrasados'
    FROM ZZ6010 Z6
    LEFT OUTER JOIN ZZ4010 ZZ4 ON ZZ4_RECEIT = ZZ6_RECEIT AND ZZ4.D_E_L_E_T_ = ''
    LEFT OUTER JOIN ZZ5010 ZZ5 ON ZZ5_RECEIT = ZZ4_RECEIT AND ZZ5.D_E_L_E_T_ = ''
    WHERE
    Z6.D_E_L_E_T_ = ''
    --AND ZZ6_DATA BETWEEN '2023-01-01' AND GETDATE()
    AND ZZ5_CODPRO NOT LIKE '0001076%'
    AND ZZ5_CODPRO NOT LIKE '0001075%'
    AND ZZ5_CODPRO NOT LIKE '0001077%'
    AND ZZ5_CODPRO NOT LIKE '0001078%'
    --AND ZZ6_ALTERA in ('Recebimento da loja p/ estoque (pós-venda)','Entrada Laboratório','Aguardando Compra das Lentes','Compra realizada.','Translado estoque -> Laboratorio Externo ','Armação enviada pela loja para montagem no laboratorio')
	and ZZ4_DTPREV > 20221201
    and zz4_status <> 'CL'
	AND DATEDIFF(DAY,CONVERT(DATE,ZZ4_DTPREV),CONVERT(DATE,ZZ6_DATA))>0
    GROUP BY 
        ZZ6_RECEIT,
        ZZ6_ALTERA,
        ZZ4_TIPO,
        ZZ6_DATA,
        ZZ6_HORA,
        ZZ6_FILIAL,
        ZZ4_DTPREV
) X
WHERE X.row_num = 1

ORDER BY X.[Dias atrasados] DESC
"""

cache = Cache(app, config={'CACHE_TYPE': 'simple',"CACHE_DEFAULT_TIMEOUT": 10})
now = dt.datetime.now()
end_of_day = dt.datetime(now.year, now.month, now.day, 23, 59, 59)
time_remaining = (end_of_day - now).total_seconds()
app.config['CACHE_DEFAULT_TIMEOUT'] = time_remaining

def Connect():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={config('MSSQL_HOST')};"
        f"DATABASE={config('MSSQL_DATABASE')};"
        f"UID={config('MSSQL_USER')};"
        f"PWD={config('MSSQL_PASS')};"
        )
    return conn

@cache.memoize()
def consulta_ultima_movimentacao():
    try:
      df= pd.read_sql(ultima_movimentacao,Connect())
      df['data hora'] = pd.to_datetime(df['data hora'])
      df['mes_ano'] = df['data hora'].dt.strftime('%m%Y')
      return df
    except:
        df = cache.get('consulta_ultima_movimentacao')
        print('chegou aqui')
        return df

@cache.memoize()
def consulta_dia():
    try:
       datainicial = dt.datetime.today().strftime('%Y%m%d')
       df= pd.read_sql(movimentacao_dia.format(datainicial=datainicial),Connect())
       df= df.groupby(['MOVIMENTAÇÃO','tipo']).agg({'total': 'sum'}).reset_index()
       return df
    except:
        df = cache.get('consulta_dia')
        return df

@cache.memoize()
def grafico_montagem():
    try:
      data_hoje = dt.datetime.today()
      hoje = pd.to_datetime(data_hoje).day
      datafinal = dt.datetime.today().strftime('%Y%m%d')
      trinta_dias = data_hoje- dt.timedelta(days=31)
      datainicial = trinta_dias.strftime('%Y%m%d')
      df = pd.read_sql(montagens_por_dia .format(datainicial=datainicial, datafinal=datafinal), Connect())
      df['data'] = pd.to_datetime(df['data'])
      df['dia_semana'] = df['data'].dt.strftime('%A')
      df['dia'] = pd.to_datetime(df['data']).dt.day
      df_sfds = df[~df['dia_semana'].isin(
        ['Saturday', 'Sunday'])]
      df_sfdsh = df_sfds[df_sfds['dia'] != hoje]
      
      custom_params = {"axes.spines.right": False,
                     "axes.spines.top": False,
                     "axes.spines.left": False,
                     "figure.facecolor": 'None',
                     "axes.facecolor": 'None',
                     'axes.labelcolor': 'white',
                     'xtick.color': 'white',
                     'ytick.color': 'white',
                     'text.color': 'white'}
      
      sns.set_style(style="white", rc=custom_params)

      fig, ax = plt.subplots()


     
      df_sfds.plot(kind='bar', x='dia', y='total',
                 legend=False, ax=ax, figsize=(9, 4))
      
      plt.xlabel('Dia')    
      plt.ylabel('Quantidade')

      for i in ax.containers:
        ax.bar_label(i, label_type='edge', fontsize=10, padding=4,
                     labels=[f'{h.get_height()}' if h.get_height() > 0 else '' for h in i])

      media = int(df_sfdsh['total'].mean())
      ax.axhline(y=media, color='r')
      ax.text(0.5,media,f'{media}', ha='center',color='red')

      img = io.BytesIO()
      plt.savefig(img, format='png')
      img.seek(0)
      plt.close()
      plot_montagem = base64.b64encode(img.getvalue()).decode()
      

      return plot_montagem
    except:
        df = cache.get('grafico_montagem')
        return df

@cache.memoize()
def os_atrasada():
    try:
       df= pd.read_sql(os_atrasadas,Connect())
       df['DT última movimentação'] = pd.to_datetime(df['DT última movimentação'])
       df['DT última movimentação'] = df['DT última movimentação'].dt.strftime('%d/%m/%y')
       df['DT Prevista'] = pd.to_datetime(df['DT Prevista'])
       df['DT Prevista'] = df['DT Prevista'].dt.strftime('%d/%m/%y')
       values = ['Recebimento Estoque','Entrada Laboratório','Aguardando Compra das Lentes','Compra realizada','Translado estoque -> Laboratorio Externo']
       df = df[df['STATUS'].isin(values)]
       return df
    except:
        df = cache.get('os_atrasada')
        return df

@app.route('/')
def index():


    receitas_atrasadas= os_atrasada()
    consult_dia = consulta_dia()
    ultima_mov = consulta_ultima_movimentacao()

    mes_ano = dt.datetime.today().strftime('%m%Y')

    Montagem_finalizada = consult_dia.loc[(consult_dia["MOVIMENTAÇÃO"] == "Montagem finalizada") &(consult_dia["tipo"] == 'M'), "total"].sum()
    if Montagem_finalizada:
        mf = Montagem_finalizada
    else:
        mf = 0

    Recebimento_estoque = consult_dia.loc[(consult_dia["MOVIMENTAÇÃO"] == "Recebimento Estoque") , "total"].sum()
    if Recebimento_estoque:
        re = Recebimento_estoque
    else:
        re = 0

    Agu_compra = consult_dia.loc[(consult_dia["MOVIMENTAÇÃO"] == "Aguardando Compra das Lentes"), "total"].sum()
    if Agu_compra:
        ag = Agu_compra
    else:
        ag = 0

    Compra_realizada = consult_dia.loc[(consult_dia["MOVIMENTAÇÃO"] == "Compra realizada"), "total"].sum()
    if Compra_realizada:
        cr = Compra_realizada
    else:
        cr = 0

    Translado_estoque = consult_dia.loc[(consult_dia["MOVIMENTAÇÃO"] == "Translado estoque -> Laboratorio Externo"), "total"].sum()
    if Translado_estoque:
        ele = Translado_estoque
    else:
        ele = 0

    AMontagem_finalizada = ultima_mov.loc[(ultima_mov["STATUS"]
                                  == "Montagem finalizada") & (ultima_mov["TIPO"] == 'M') & (ultima_mov["mes_ano"] == mes_ano), 'total'].sum()
    if AMontagem_finalizada:
        amf = AMontagem_finalizada
    else:
        amf = 0

    aRecebimento_estoque = ultima_mov.loc[ultima_mov["STATUS"] ==
                                  "Recebimento Estoque", 'total'].sum()
    if aRecebimento_estoque:
        are = aRecebimento_estoque
    else:
        are = 0

    aAgu_compra = ultima_mov.loc[ultima_mov["STATUS"] ==
                         "Aguardando Compra das Lentes", 'total'].sum()
    if aAgu_compra:
        aag = aAgu_compra
    else:
        aag = 0

    aCompra_realizada = ultima_mov.loc[ultima_mov["STATUS"] ==
                               "Compra realizada", 'total'].sum()
    if aCompra_realizada:
        acr = aCompra_realizada
    else:
        acr = 0

    aTranslado_estoque = ultima_mov.loc[ultima_mov["STATUS"] ==
                                "Translado estoque -> Laboratorio Externo", 'total'].sum()
    if aTranslado_estoque:
        aele = aTranslado_estoque
    else:
        aele = 0

        
    aAgu_arm = ultima_mov.loc[ultima_mov["STATUS"] ==
                                "Aguardando armação", 'total'].sum()
    if aAgu_arm:
        aaga = aAgu_arm
    else:
        aaga = 0 
    
    #tabela
    receitas_atrasadas = receitas_atrasadas.sort_values(by='Dias atrasados', ascending=False).head(17)
    receita = receitas_atrasadas.to_html(table_id='table', index=False)

    plot_montagem = grafico_montagem()

    som = are + aag + acr + aele
    #media = g.media
    

    return render_template('index.html',
                           mf=mf,
                           re=re,
                           ag=ag,
                           cr=cr,
                           ele=ele,
                           amf=amf,
                           are=are,
                           aag=aag,
                           acr=acr,
                           aele=aele,
                           plot_montagem=plot_montagem,
                           receita=receita,
                           som=som,
                           aaga=aaga

                           )
