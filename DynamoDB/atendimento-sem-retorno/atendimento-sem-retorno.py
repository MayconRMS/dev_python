import boto3
import os
import datetime
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd

print('INICIANDO BUSCA POR ATENDIMENTOS SEM RETORNO AO PARCEIRO')
print('')
print('POR FAVOR AGUARDE, ISSO PODE LEVAR ALGUM TEMPO... ')
print('')

hoje = datetime.date.today()
os.environ['AWS_PROFILE'] = "prod"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('AtendimentoStatusFullLog')

response = table.scan(
FilterExpression=Attr('Status').eq('SUCESSO') &
Attr('tipoInteracao').eq('ABRIR_CASO_VALIDACAO'),)

registros = response['Items']

primeiro = True;
count = 1

while 'LastEvaluatedKey' in response:
    print('')
    print(count)
    count = count + 1
    response = table.scan(
    FilterExpression=Attr('Status').eq('SUCESSO') &
    Attr('tipoInteracao').eq('ABRIR_CASO_VALIDACAO'),
    ExclusiveStartKey=response['LastEvaluatedKey'])

    registros.extend(response['Items'])

statusEncerrados = ['encerrado com sucesso']

obj1 = {
        'id':[],
        'dataHora':[],
        'idAtendimento':[],
        'tipoInteracao':[],
        'idLog':[],
        'tipoCaso':[],
        'Status':[],
        'objeto':[]}

dfFinal = pd.DataFrame(obj1)

for registro in registros:
    print('PROCESSANDO ATENDIMENTO', registro['idAtendimento'])
    print('')
    response = table.scan(
    FilterExpression=Attr('idLog').eq(registro['idLog']) & 
    Attr('tipoInteracao').eq('ATUALIZAR_CASO_REQUISICAO_SN'))

    atualizacoes = response['Items']

    teveRetorno = 0

for at in atualizacoes:
    for se in statusEncerrados:
        if at['objeto'].find(se) != -1:
            teveRetorno = 1


if teveRetorno == 0:
    obj2 = {
        'id':[registro['id']],
        'dataHora':[registro['dataHora']],
        'idAtendimento':[registro['idAtendimento']],
        'tipoInteracao':[registro['tipoInteracao']],
        'idLog':[registro['idLog']],
        'tipoCaso':[registro['tipoCaso']],
        'Status':[registro['Status']],
        'objeto':[registro['objeto']]}

    df = pd.DataFrame(obj2)
    dfFinal = dfFinal.append(df)

# print('Dataframe', dfFinal)
dfFinal.to_excel("atendimento-sem-retorno" + str(hoje) + ".xlsx")
