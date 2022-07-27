import boto3
import os
import datetime
from boto3.dynamodb.conditions import Key, Attr

## O que ela faz ?
## Lista todos os atendimentos sem casos abertos no SN

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
tableParametrizacaoParceiros = dynamodb.Table('ParametrizacaoParceiros')
## Tabelas do dynamo >

print('idAtendimento', ';', 'caso', ';', 'parceiroComercialId', ';', 'idTransacao', ';',
              'serviceNowJson', ';', 'sqsEnvioServiceNow', file=sourceFile)

dataInicio = '2022-01-01T11:00:00.000Z'
dataFim = '2022-05-26T00:00:00.000Z'

response = tableAtendimento.scan(
    FilterExpression=Attr('caso').between(dataInicio, dataFim))

registros = response['Items']

count = 1


def getCnpj(parceiroComercialId):
    res = tableParametrizacaoParceiros.query(
        IndexName="_parceiroComercialIdIndex",
        KeyConditionExpression=Key('parceiroComercialId').eq(parceiroComercialId),
    )
    for item in res['Items']:
        # print(item)
        return item['cnpj']


def montaSqs(item):
    return "{" + "'atendimentoId':'" + item['id'] + "','parceiroComercialId': '" + item[
        'parceiroComercialId'] + "', 'serviceNowJSON': '" + item['serviceNowJson'] + "', 'cnpj': '" + getCnpj(
        item['parceiroComercialId']) + "'}"


def salvarRegistro(item):
    # print(item['id'], ';', item['caso'],';', item['parceiroComercialId'], ';' , item['idTransacao'], ';' , item['serviceNowJson'], ';', montaSqs(item)  ,file=sourceFile)
    print('- Processo:', count, end='\r')
    try:
        print(item['id'], ';', item['caso'], ';', item['parceiroComercialId'], ';', item['idTransacao'], ';',
              item['serviceNowJson'], ';', montaSqs(item), file=sourceFile)
    except:
        print('Erro ao salvar item')
        pass


for item in response['Items']:
    count = count + 1
    salvarRegistro(item)

while 'LastEvaluatedKey' in response:
    response = tableAtendimento.scan(
        FilterExpression=Attr('caso').between(dataInicio, dataFim),
        ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        count = count + 1
        salvarRegistro(item)

print('PROCESSO FINALIZADO')
