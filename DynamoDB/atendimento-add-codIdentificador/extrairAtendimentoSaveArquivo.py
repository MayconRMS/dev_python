import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import json
import sys
from pathlib import Path

## @Auhtor Maycon R - DEV-MM
## O que ela faz ?
## Lista todos os atendimentos por periodo inicio e fim que não tem o campo "codIdentificador" 
## copia o valor do identificador/tag pegando do campo "requestBodyJson"
## Gera um arquivo com as informações de ID - CODIDENTIFICADOR - ATENDIMENTO(OPCIONAL) 
## vai ser usado para atualizar esses atendimento com outro script.
## Utilizar:
## Para utilizar apenas selecione o periodo e o ambiente da aws.

os.system('cls||clear')
file_path = sys.path[0]
file_path_name = Path(file_path).stem
file_name = sys.argv[0].split('\\')[-1].replace('.py','')

dataHora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
os.environ['AWS_PROFILE'] = "hml"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1" 
print("\nEXECUTANDO EM <" + os.environ['AWS_PROFILE'] + ">")
print('POR FAVOR AGUARDE, ISSO PODE LEVAR ALGUM TEMPO...' , dataHora)

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
tableAtendimento = dynamodb.Table('Atendimento')
## Tabelas do dynamo >

count = 0
countTotalSaveFile = 0
dataInicio = '2022-01-01T00:00:00.000Z'
dataFim = '2022-07-20T00:00:00.000Z'
print('DATAS SELECIONADAS: ' , dataInicio, " - ", dataFim)

# Nome do arquivo de saida sempre out
fileNameOut = file_path + "/out_add_codIdentificador_" +  os.environ['AWS_PROFILE'] + '.txt'
sourceFile = open(fileNameOut, 'w', encoding="utf-8")

def salvarRegistroFile(atendimentoNovo):
    global countTotalSaveFile
    countTotalSaveFile += 1
    try:
        print(atendimentoNovo,file=sourceFile)
    except Exception as e:
        print('Erro:' + str(e))
        pass

#Verifica se existe o codIdentificador no body com os nomes 
def verPorNomeIdentificadorTag(atendimento):
    codIdentificador = 'null'
    body = json.loads(atendimento['requestBodyJson'])
    try:
        if 'tag' in body:
            codIdentificador = body['tag'] 
        elif 'identificador' in body:
            codIdentificador = body['identificador'] 
            
        if codIdentificador == None:
            codIdentificador = 'null'    
            
    except Exception as e:
        print('[' + count + '] Objeto Erro: ' , str(item))
        pass
    return codIdentificador

#So salva se nao existir codIdentificador e verifica se existe no body para copiar
def salvarRegistro(atendimento):
    global count
    count += 1
    print('- Processo:', count, end='\r')
    
    if 'codIdentificador' not in atendimento:
        codIdentificador = verPorNomeIdentificadorTag(atendimento)
        
        itemNovo = {}
        itemNovo['id']= atendimento['id']
        itemNovo['codIdentificador'] = codIdentificador
        itemNovo['atendimentoOriginal']= json.dumps(atendimento, sort_keys=True)
        salvarRegistroFile(itemNovo);
            
response = tableAtendimento.scan(
FilterExpression=Attr('creationDate').between(dataInicio, dataFim))
    
for item in response['Items']:
        salvarRegistro(item)

while 'LastEvaluatedKey' in response:
    response = tableAtendimento.scan(
    FilterExpression=Attr('creationDate').between(dataInicio, dataFim),
    ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        salvarRegistro(item)
    
sourceFile.close()
print('\nPROCESSO FINALIZADO - Quantidade Salva:' , countTotalSaveFile)

