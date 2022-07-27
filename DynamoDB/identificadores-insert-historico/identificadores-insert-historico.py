import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import uuid
import sys
from pathlib import Path

file_path = sys.path[0]
file_path_name = Path(file_path).stem
file_name = sys.argv[0].split('\\')[-1].replace('.py','')

## O que ela faz ?
## Adicionar historico (log) na tabela IdentificadorMaasLog dos identificadores com situação INATIVO.
## Lista todos os identificadores com situação INATIVO por periodo , verifica se existe o log de INATIVAR , 
##      não existindo grava um novo registro na identificadorMaasLog ou arquivo
## Motivo?
## Alguns identificadores com a situação inativa não esta com log dessa ação no identificadorMaasLog, 
#       deixando seu historico imcompleto na visualição do portal.

dataHora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
os.environ['AWS_PROFILE'] = "prod"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1" 
print("\nEXECUTANDO EM <" + os.environ['AWS_PROFILE'] + ">")
print('POR FAVOR AGUARDE, ISSO PODE LEVAR ALGUM TEMPO...' , dataHora)

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
tableIdentificador = dynamodb.Table('Identificador')
tableIdentificadorMaasLog = dynamodb.Table('IdentificadorMaaSLog')
## Tabelas do dynamo >

count = 0
dataInicio = '2022-06-16T00:00:00.000Z'
dataFim = '2022-06-17T00:00:00.000Z'
IdClienteC6 = 3270840
# Quando habilitado o metodo de salvar no arquivo não exibe erro de nao salvo!
saveBd = True;

# Nome do arquivo de saida sempre out
fileNameOut = file_path + "/out_" +  os.environ['AWS_PROFILE'] + "_"  + file_name + '.csv'
sourceFile = open(fileNameOut, 'w', encoding="utf-8")

print('Nº' ,";",
    'id',";",
    'codConta',";",
    'codIdentificador' ,";",
    'dataInclusao' ,";",
    'dataUltimaAtualizacao' ,";",
    'mensagemProcessamento' ,";",
    'motivoBloqueio' ,";",
    'operacao' ,";",
    'passoProcessamento' ,";",
    'payload' ,";",
    'placa' ,";",
    'statusProcessamento' ,";",
    'identificadorJSON' ,
     file=sourceFile)

def salvarRegistroFile(identificador, itemNovo):
    global saveBd
    saveBd = False
    try:
        print(
            count,";",
            itemNovo['id'] ,";",
            itemNovo['codConta'] ,";",
            itemNovo['codIdentificador'] ,";",
            itemNovo['dataInclusao'] ,";",
            itemNovo['dataUltimaAtualizacao'] ,";",
            itemNovo['mensagemProcessamento'] ,";",
            itemNovo['motivoBloqueio'] ,";",
            itemNovo['operacao'] ,";",
            itemNovo['passoProcessamento'] ,";",
            itemNovo['payload'] ,";",
            itemNovo['placa'] ,";",
            itemNovo['statusProcessamento'] ,";",
            identificador,
            file=sourceFile)
    except Exception as e:
        print('Erro:' + str(e))
        pass

def salvarRegistroHistorico(identificador):
    itemNovo = {}
    itemNovo['id']= str(uuid.uuid4())
    itemNovo['codConta']= identificador['clienteConta']['conta']['id']
    itemNovo['codIdentificador']= identificador["codIdentificador"]
    itemNovo['dataInclusao']= identificador["dataAtualizacao"]
    itemNovo['dataUltimaAtualizacao']= identificador["dataAtualizacao"]
    itemNovo['mensagemProcessamento']= ""
    itemNovo['motivoBloqueio']= "BLOQUEIO_TEMPORARIO"
    itemNovo['operacao']= "INATIVAR"
    itemNovo['passoProcessamento']= "insert manual"
    itemNovo['payload']= ""
    itemNovo['placa']= identificador["placa"]
    itemNovo['statusProcessamento']= 'FINALIZADO'
    
    #comente para salvar no bd
    salvarRegistroFile(identificador,itemNovo);
    
    if saveBd :
        tableIdentificadorMaasLog.put_item(Item=itemNovo)

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
    
def salvarRegistro(identificador):
    global count
    count += 1
    print('- Processo:', count, end='\r')
    try:
        #Se não existe log ele grava um novo conforme o identificador
        if existeLog(identificador) <= 0 :
            salvarRegistroHistorico(identificador)
        
        #Se so gravou no arquivo não faz essa verificação
        if saveBd :
            # Verifica se salvou no bd 
            if existeLog(identificador) <= 0:
                print('=> Erro salvar logIdentificador codIdentificador: ' + str(identificador["codIdentificador"]))
                
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

sourceFile.close()
print('\nPROCESSO FINALIZADO')

