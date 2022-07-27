import boto3
import os
import json
from unidecode import unidecode
from boto3.dynamodb.conditions import Key, Attr

## O que ela faz ?
## Lista todos os atendimentos sem casos abertos no SN e gera historico

## < Funções importadas comuns
import sys 
sys.path.insert(0, str(os.getcwd() + '\\config-commons'))
from configCommons import configVariaveis, outFileTime, outFileSame, dataHoje, outPathSameNew, outFileSameNew
configVariaveis()
sourceFile = outFileSame()
## Funções importadas comuns >

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
tableAtendimento = dynamodb.Table('Atendimento')
AtendimentoStatusFullLog = dynamodb.Table('AtendimentoStatusFullLog')
tableParametrizacaoParceiros = dynamodb.Table('ParametrizacaoParceiros')
## Tabelas do dynamo >

dataInicio = '2022-01-01T11:00:00.000Z'
dataFim = dataHoje()
print('Periodo:', dataInicio, ' até ',dataFim)
print('Periodo:', dataInicio, ' ate ',dataFim , file=sourceFile)

print('idAtendimento', ';', 'caso', ';', 'parceiroComercialId', ';', 'idTransacao', ';',
              'serviceNowJson', file=sourceFile)


response = tableAtendimento.scan(FilterExpression=Attr('caso').between(dataInicio, dataFim))
registros = response['Items']
count = 1
pathHistorico = outPathSameNew("historico")

##### Salvar Arquivo para cada atendimento
def salvarRegistroFinalHistoricoAtendimento(historicoAtendimento, item):
    sourceFileAtendimento = outFileSameNew(pathHistorico,item['id'])  
    historicoAtendimento.sort(key=lambda x: x["dataHora"])
    
    print('idLog', ';', 'idAtendimento',';','tipoInteracao',';','mensagem', ';'
        , 'objeto', file=sourceFileAtendimento)
    
    for item in historicoAtendimento:
        try:
            print(item['idLog'],';', item['dataHora'],';', item['tipoInteracao'],';',
                unidecode(item['mensagem']),';', item['original'], file=sourceFileAtendimento)
        except Exception as e:
            print('Erro ao salvar historico atendimento: ' + str(e))
            pass
    sourceFileAtendimento.close()
    
##### Verifica os erros
def dadosObjeto(item):
    objeto = item['objeto']
    objeto.replace("'", "\\u027")
    texto = "sem mensagem"
    i = texto
    try:
        if 'AtualizarCasoDTO' in objeto:
            i = 'AtualizarCasoDTO'
            texto = item['mensagem']
        elif 'mensagem' in objeto:
            i = 'mensagem'
            objetoJson = json.loads(objeto)
            texto = objetoJson['mensagem']
        elif 'statusMessage' in objeto:
            i = 'statusMessage'
            objetoJson = json.loads(objeto)
            result = objetoJson["result"]
            texto = result['statusMessage']
    except Exception as e:
        print('[' + i + '] Objeto Erro: ' + str(item))
        print('\nException: ' + str(e))
    return "Erro Ocorrido: " + texto

##Cria uma lista com novos dados
def salvarRegistroHistorico(item):
    itemNovo = {}
    itemNovo['idLog']= item['id']
    itemNovo['dataHora']= item['dataHora']
    itemNovo['tipoInteracao']= item['tipoInteracao']
    itemNovo['mensagem']= dadosObjeto(item)
    itemNovo['original']= item
    return itemNovo
    
## Busca o historico do atendimento na tabela de Log e cria um arquivo
def historico(item):
    historicoAtendimento = []
    countHist = 1
    response2 = AtendimentoStatusFullLog.query(
        IndexName="_IdAtendimento_index",
        KeyConditionExpression=Key('idAtendimento').eq(item['id']))
        
    for itemHist in response2['Items']:
        countHist = countHist + 1
        itemNovo = salvarRegistroHistorico(itemHist)
        historicoAtendimento.append(itemNovo)

    while 'LastEvaluatedKey' in response2:
        response2 = AtendimentoStatusFullLog.query(
            IndexName="_IdAtendimento_index",
            KeyConditionExpression=Key('idAtendimento').eq(item['id']),
            ExclusiveStartKey=response['LastEvaluatedKey'])
        for itemHist in response['Items']:
            countHist = countHist + 1
            itemNovo = salvarRegistroHistorico(itemHist)
            historicoAtendimento.append(itemNovo)
    
    salvarRegistroFinalHistoricoAtendimento(historicoAtendimento, item)

## Salva os casos não abertos 
def salvarRegistro(item):
    global count
    count = count + 1
    print('- Processo:', count, end='\r')
    try:
        print(item['id'], ';', item['caso'], ';', item['parceiroComercialId'], ';', item['idTransacao'], ';',
              item['serviceNowJson'], file=sourceFile)
        historico(item)
    except Exception as e:
        print('Erro:' + str(e))
        pass

## Busca os casos não abertos pela data
for item in response['Items']:
    salvarRegistro(item)

while 'LastEvaluatedKey' in response:
    response = tableAtendimento.scan(
        FilterExpression=Attr('caso').between(dataInicio, dataFim),
        ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        salvarRegistro(item)

sourceFile.close()
print('\nPROCESSO FINALIZADO')
