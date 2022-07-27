from ast import Expression
import boto3
import os
from unidecode import unidecode
from boto3.dynamodb.conditions import Key, Attr

## O que ela faz ?
## Listar identificadores sem historico

## < Funções importadas comuns
import sys 
sys.path.insert(0, str(os.getcwd() + '\\config-commons'))
from configCommons import configVariaveis, outFileTime, outFileSame, dataHoje, outPathSameNew, outFileSameNew
configVariaveis()
sourceFile = outFileSame()
## Funções importadas comuns >

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
tableIdentificador = dynamodb.Table('Identificador')
tableIdentificadorMaasLog = dynamodb.Table('IdentificadorMaaSLog')
## Tabelas do dynamo >

dataInicio = '2022-03-01T00:00:00.000Z'
dataFim = '2022-05-01T00:00:00.000Z'
#dataFim = dataHoje()
count = 0
countLog = 0
IdClienteC6 = 3270840
codIdentificadorProblema = "1161303990"
pathHistorico = outPathSameNew("historico")

print('Periodo:', dataInicio, ' até ',dataFim)
print('Periodo:',";", dataInicio,";", ' ate ',";",dataFim ,";", file=sourceFile)

print("codIdentificador" ,";", "dataInclusao",";", "placa",";", "cliente", ";", "json", file=sourceFile)

##### Salvar Arquivo para cada identificador
def salvarRegistroFinalHistoricoIdentificador(historicoIdentificador, item):
    sourceFileAtendimento = outFileSameNew(pathHistorico,item['id'])  
    historicoIdentificador.sort(key=lambda x: x["dataInclusao"])
    
    for item in historicoIdentificador:
        try:
            print(item['codIdentificador'],";",item['dataInclusao'],";", item['operacao'],";",item, file=sourceFileAtendimento)
        except Exception as e:
            print('Erro ao salvar historico identificador: ' + str(e))
            pass
    sourceFileAtendimento.close()

## Busca o historico do identificador na tabela de Log e cria um arquivo
def historico(item):
    historicoIdentificador = []
    countHist = 1
    response2 = tableIdentificadorMaasLog.query(
        IndexName="_GestaoMaaSCodIdentificadorOperacaoIndex",
        KeyConditionExpression=Key('codIdentificador').eq(item['codIdentificador']))
        
    for itemHist in response2['Items']:
        countHist = countHist + 1
        historicoIdentificador.append(itemHist)

    while 'LastEvaluatedKey' in response2:
        response2 = tableIdentificadorMaasLog.query(
            IndexName="_GestaoMaaSCodIdentificadorOperacaoIndex",
            KeyConditionExpression=Key('codIdentificador').eq(item['codIdentificador']),
            ExclusiveStartKey=response['LastEvaluatedKey'])
        for itemHist in response['Items']:
            countHist = countHist + 1
            historicoIdentificador.append(itemHist)
            
    #print("Save Historico: " + str(countHist))
    salvarRegistroFinalHistoricoIdentificador(historicoIdentificador, item)

def existeLog(item):
    response2 = tableIdentificadorMaasLog.query(
        IndexName="_GestaoMaaSCodIdentificadorOperacaoIndex",
        KeyConditionExpression='codIdentificador = :cod AND operacao = :operacao',
        ExpressionAttributeValues={
            ':cod': item['codIdentificador'],
            ':operacao': "INATIVAR" })

    while 'LastEvaluatedKey' in response2:
        response2 = tableIdentificadorMaasLog.query(
            IndexName="_GestaoMaaSCodIdentificadorOperacaoIndex",
            KeyConditionExpression='codIdentificador = :cod AND operacao = :operacao',
            ExpressionAttributeValues={
                ':cod': item['codIdentificador'],
                ':operacao': "INATIVAR" },
            ExclusiveStartKey=response['LastEvaluatedKey'])
    
    return response2['Count']

def salvarRegistro(item):
    global count, countLog
    count += 1
    print('- Processo:', count, end='\r')
    try:
        if existeLog(item) <= 0 :
            countLog += 1
            print(item["codIdentificador"] ,";"
                  , item["dataInclusao"],";"
                  , item["placa"],";"
                  , item["idCliente"],";"
                  , item, file=sourceFile)
        
        #historico(item)  
         
    except Exception as e:
        print('Erro:' + str(e))
        pass

response = tableIdentificador.scan(
    FilterExpression=Attr('dataInclusao').between(dataInicio, dataFim)
        & Attr('situacao').eq("INATIVO")
        & Attr('idCliente').eq(IdClienteC6))

for item in response['Items']:
    #print("*")
    salvarRegistro(item)

while 'LastEvaluatedKey' in response:
    response = tableIdentificador.scan(
        FilterExpression=Attr('dataInclusao').between(dataInicio, dataFim)
        & Attr('situacao').eq("INATIVO")
        & Attr('idCliente').eq(IdClienteC6),
        ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        #print("*")
        salvarRegistro(item)

sourceFile.close()
print('\nResultado: ' + str(countLog))
print('\nPROCESSO FINALIZADO')
