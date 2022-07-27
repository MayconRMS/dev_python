import re
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import uuid

## O que ela faz ?
## Adicionar historico (log) na tabela IdentificadorMaasLog dos identificadores com situação INATIVO.
## Lista todos os identificadores com situação INATIVO por periodo , verifica se existe o log de INATIVAR , 
##      não existindo grava um novo registro na identificadorMaasLog 
## Motivo?
## Alguns identificadores com a situação inativa não esta com log dessa ação no identificadorMaasLog, 
#       deixando seu historico imcompleto na visualição do portal.

dataHora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
os.environ['AWS_PROFILE'] = "dev"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1" 
print("\nEXECUTANDO EM <" + os.environ['AWS_PROFILE'] + ">")
print('POR FAVOR AGUARDE, ISSO PODE LEVAR ALGUM TEMPO...' , dataHora)

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
#tableIdentificador = dynamodb.Table('Identificador')
#tableIdentificadorMaasLog = dynamodb.Table('IdentificadorMaaSLog')

tableIdentificador = dynamodb.Table('identificadorInsert')
tableIdentificadorMaasLog = dynamodb.Table('identificadorInsertLog')
## Tabelas do dynamo >

count = 0
dataInicio = '2022-06-14T00:00:00.000Z'
dataFim = '2022-06-15T00:00:00.000Z'
IdClienteC6 = 3270840

def removeRegistroHistorico(item):
    itemRemove = {}
    itemRemove['id']= item['id']
    print("\n=> DEL : " + str(item['codIdentificador']))
    tableIdentificadorMaasLog.delete_item(Key=itemRemove)

def salvarRegistroHistorico(item):
    itemNovo = {}
    itemNovo['id']= str(uuid.uuid4())
    itemNovo['codConta']= item['clienteConta']['conta']['id']
    itemNovo['codIdentificador']= item["codIdentificador"]
    itemNovo['dataInclusao']= item["dataAtualizacao"]
    itemNovo['dataUltimaAtualizacao']= item["dataAtualizacao"]
    itemNovo['mensagemProcessamento']= ""
    itemNovo['motivoBloqueio']= "BLOQUEIO_TEMPORARIO"
    itemNovo['operacao']= "INATIVAR"
    itemNovo['passoProcessamento']= "insert manual"
    itemNovo['payload']= ""
    itemNovo['placa']= item["placa"]
    itemNovo['statusProcessamento']= 'FINALIZADO'
    
    print("\n=> ADD : " + str(itemNovo['codIdentificador']))
    tableIdentificadorMaasLog.put_item(Item=itemNovo)
    return itemNovo

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
    
    print("=> Consulta Log Inativar: " + str(response2['Count']))
    
    if response2['Count'] > 0 :
        for item in response2['Items']:
            print("\n================= IDENTIFICADORMAASLOG ==========================")
            print(item)
            print("=================================================================")
        
    return response2['Count']
    
def salvarRegistro(item):
    global count
    count += 1
    #print('- Processo:', count, end='\r')
    try:
        print("\n ===================== IDENTIFICADOR =============================")
        print(item)
        existeLog(item)
                
        print("=================================================================")
                
        itemNovo = salvarRegistroHistorico(item)
        existeLog(item)
                
        removeRegistroHistorico(itemNovo)
        existeLog(item)
    except Exception as e:
        print('Erro:' + str(e))
        pass

response = tableIdentificador.scan(
    FilterExpression=Attr('dataInclusao').between(dataInicio, dataFim)
        & Attr('situacao').eq("INATIVO")
        & Attr('idCliente').eq(IdClienteC6))

for item in response['Items']:
    salvarRegistro(item)

while 'LastEvaluatedKey' in response:
    response = tableIdentificador.scan(
        FilterExpression=Attr('dataInclusao').between(dataInicio, dataFim)
        & Attr('situacao').eq("INATIVO")
        & Attr('idCliente').eq(IdClienteC6),
        ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        salvarRegistro(item)



