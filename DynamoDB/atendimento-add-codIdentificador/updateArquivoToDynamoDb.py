import boto3
import ast
import os
import sys
from datetime import datetime

## @Auhtor Maycon R - DEV-MM
## O que ela faz ?
## Abre o arquivo gerado pelo script de extrairAtendimentoSaveArquivo.py
## Atualiza os atendimentos com os campos CODIDENTIFICADOR E UPDATEDATE conforme o ID
## Utilizar:
## Para utilizar apenas selecione o ambiente da aws ele pegara o ultimo arquivo gerado de acordo.

os.system('cls||clear')
file_path = sys.path[0]
os.environ['AWS_PROFILE'] = "hml"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
tableAtendimento = dynamodb.Table('Atendimento')
## Tabelas do dynamo >

print('INICIANDO ENVIO DE DADOS ',str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]))
fileNameOut =  file_path + "/out_add_codIdentificador_" +  os.environ['AWS_PROFILE'] + '.txt'
print('OPEN FILE >  ',fileNameOut)

def updateDynamo(tableAtendimento, registro):
    response = tableAtendimento.update_item(
                    Key={'id': registro['id']},
                    UpdateExpression="set codIdentificador = :codIdentificador , updateDate = :updateDate",
                    ExpressionAttributeValues={
                        ':codIdentificador': registro['codIdentificador'],
                        ':updateDate': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                    },
                    ReturnValues="UPDATED_NEW"
            )
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        print('ERRO PROCESSAMENTO REGISTRO: ID', registro['id'])

with open(fileNameOut, "r") as arquivo:
    dados = arquivo.readlines()
    count = 0;
    
    for dado in dados:
        try:
            count += 1
            print('- Processo:', count, end='\r')
            registro = ast.literal_eval(dado)
            updateDynamo(tableAtendimento, registro)
        except Exception as e:
            print('ERRO PROCESSAMENTO REGISTRO: ID', registro['id'] , str(e))
            pass    

print('ENVIO DE DADOS FINALIZADO ' , count)