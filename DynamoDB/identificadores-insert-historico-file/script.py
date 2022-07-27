import boto3
import ast
from boto3.dynamodb.conditions import Key


# Create SQS client
sqs = boto3.client('sqs')

## < Tabelas do dynamo
dynamodb = boto3.resource('dynamodb')
tableIdentificadorMaasLog = dynamodb.Table('IdentificadorMaaSLog') 

print('INICIANDO ENVIO DE DADOS')

with open("arquivo.txt", "r") as arquivo:
    dados = arquivo.readlines()

    count = 1;
    for dado in dados:
        try:
            print('PROCESSANDO REGISTRO: ', count)
            registro = ast.literal_eval(dado)

            itemNovo = {}
            itemNovo["Id"]= registro["id"]
            itemNovo["codConta"]= int(registro["codConta"])
            itemNovo["codIdentificador"]= registro["codIdentificador"]
            itemNovo["dataInclusao"]= registro["dataInclusao"]
            itemNovo["dataUltimaAtualizacao"]= registro["dataUltimaAtualizacao"]
            itemNovo["mensagemProcessamento"]= ""
            itemNovo["motivoBloqueio"]= ""
            itemNovo["operacao"]= registro["operacao"]
            itemNovo["passoProcessamento"]= registro["passoProcessamento"]
            itemNovo["payload"]= ""
            itemNovo["placa"]= registro["placa"]
            itemNovo["statusProcessamento"]= registro["statusProcessamento"]

            #tableIdentificadorMaasLog.put_item(Item=itemNovo)
            # print(registro['id'])
            
            
        except:
            print('ERRO PROCESSAMENTO REGISTRO: ', count)
            pass

        count = count + 1

print('ENVIO DE DADOS FINALIZADO')