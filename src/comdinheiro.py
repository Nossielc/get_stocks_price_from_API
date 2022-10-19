#!/usr/bin/python
import requests
import json
import pyodbc 
import pandas as pd
from datetime import datetime

def get_from_API(init_date, end_data, sql_conn,logging):
    queryAcoes = "SELECT [CODIGO],[SIGLA] FROM [dbo].[SLN_ATIVO] WHERE ATIVO = 'S'"

    dfAcoes = pd.read_sql(queryAcoes, sql_conn)

    dfAcoes['DATA'] = ""
    dfAcoes['PRECO_ABERTURA'] = ""
    dfAcoes['PRECO_MAXIMO'] = ""
    dfAcoes['PRECO_MINIMO'] = ""
    dfAcoes['PRECO_MEDIO'] = ""
    dfAcoes['PRECO_FECHAMENTO'] = ""
    dfAcoes['NUMERO_NEGOCIO'] = ""
    dfAcoes['QUANTIDADE_NEGOCIADA'] = ""
    dfAcoes['VOLUME_TOTAL'] = ""
    dfAcoes['FATOR'] = ""
    dfAcoes['OSCILACAO'] = ""

    url = "https://www.comdinheiro.com.br/Clientes/API/EndPoint001.php"
    
    querystring = {"code":"import_data"}
    payload = "username=YourUser&password=YourPassword&URL=HistoricoIndicadoresFundamentalistas001.php%3F%26data_ini="+init_date+"%26data_fim="+end_data+"%26trailing%3D12%26conv%3DMIXED%26moeda%3DMOEDA_ORIGINAL%26c_c%3Dconsolidado%26m_m%3D1%26n_c%3D2%26f_v%3D0%26papel%3Dexplode(RotinaAPI)%26indic%3DTICKER%2BPRECO_ABERTURA%2BPRECO_MAXIMO%2BPRECO_MINIMO%2BPRECO_MEDIO%2BPRECO_FECHAMENTO%2BNEGOCIOS_DIA%2BQUANT_NEGOCIADA(0%2C%2C%2Csoma)%2BVOLUME_DIA%2B%2BFATOR_COTACAO%2BRETORNO(01d%2C%2C%2Ctodos)%26periodicidade%3Ddu%26graf_tab%3Dtabela_v%26desloc_data_analise%3D1%26flag_transpor%3D0%26c_d%3Dd%26enviar_email%3D0%26enviar_email_log%3D0%26cabecalho_excel%3Dmodo1%26relat_alias_automatico%3Dcmd_alias_01&format=json2"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.post(url, data=payload, headers=headers, params=querystring)
    responseJson = json.loads(response.text)
    responseVector = responseJson['resposta']['tab-p0']['linha']

    indexEmptyAcoes = []
    for resp in responseVector:
        targetIndexAcoes = dfAcoes.loc[dfAcoes['SIGLA'] == resp['ticker']].index
        if targetIndexAcoes.empty:
            logging.info(resp['ticker'] + " doesnt exit in Database")
        elif not resp['preco_abertura'] == []:
            if not targetIndexAcoes.empty:
                if not any(dfAcoes.loc[targetIndexAcoes, 'PRECO_ABERTURA']):
                    dfAcoes.loc[targetIndexAcoes, 'DATA'] = resp['data']
                    dfAcoes.loc[targetIndexAcoes, 'PRECO_ABERTURA'] = 0 if resp['preco_abertura'] == [] else resp['preco_abertura'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'PRECO_MAXIMO'] = 0 if resp['preco_maximo'] == [] else resp['preco_maximo'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'PRECO_MINIMO'] = 0 if resp['preco_minimo'] == [] else resp['preco_minimo'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'PRECO_MEDIO'] = 0 if resp['preco_medio'] == [] else resp['preco_medio'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'PRECO_FECHAMENTO'] = 0 if resp['preco_fechamento'] == [] else resp['preco_fechamento'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'NUMERO_NEGOCIO'] = 0 if resp['negocios_dia'] == [] else resp['negocios_dia'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'QUANTIDADE_NEGOCIADA'] = 0 if resp['quant_negociada(0,,,soma)'] == [] else resp['quant_negociada(0,,,soma)'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'VOLUME_TOTAL'] = 0 if (resp['volume_dia'] == [] or "E" in resp['volume_dia']) else resp['volume_dia'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'FATOR'] = resp['fator_cotacao'].replace(',','.')
                    dfAcoes.loc[targetIndexAcoes, 'OSCILACAO'] = 0 if resp['retorno01d'] == [] else (float(resp['retorno01d'].replace(',','.'))/100)

    dfAcoes = dfAcoes[dfAcoes.PRECO_ABERTURA != '']

    cursor = sql_conn.cursor()
    for index,row in dfAcoes.iterrows():
        cursor.execute( "INSERT INTO dbo.SLN_COTACAO_API([SLN_ATIV_CODIGO],\
                                                        [DATA_CRIACAO],\
                                                        [DATA],\
                                                        [PRECO_ABERTURA],\
                                                        [PRECO_MAXIMO],\
                                                        [PRECO_MINIMO],\
                                                        [PRECO_MEDIO],\
                                                        [PRECO_FECHAMENTO],\
                                                        [NUMERO_NEGOCIO],\
                                                        [QUANTIDADE_NEGOCIADA],\
                                                        [VOLUME_TOTAL],\
                                                        [FATOR],\
                                                        [OSCILACAO])\
                            values (?,getutcdate() AT TIME ZONE 'E. South America Standard Time',?,?,?,?,?,?,?,?,?,?,?)",
                        row['CODIGO'],
                        datetime.strptime(row['DATA'], '%d/%m/%Y'),
                        row['PRECO_ABERTURA'],
                        row['PRECO_MAXIMO'],
                        row['PRECO_MINIMO'],
                        row['PRECO_MEDIO'],
                        row['PRECO_FECHAMENTO'],
                        row['NUMERO_NEGOCIO'],
                        row['QUANTIDADE_NEGOCIADA'],
                        row['VOLUME_TOTAL'],
                        row['FATOR'],
                        row['OSCILACAO'])
        sql_conn.commit()

    cursor.close()
    logging.info("Dados que vieram da API:")
    logging.info(dfAcoes)