import boto3
import sys
import os
import json
from tqdm import tqdm
from unidecode import unidecode
from boto3.dynamodb.conditions import Key, Attr

## O que ela faz ?
## Lista todos os atendimento da tabela statusFullLog por periodo e com status de ERRO
## Verifica na tabela atendimento se existe atendimento
##      Existe? Verifica se tem caso aberto (CS)
##      Não? Erro antes , deve ser de validação
## Verifica o motivo do erro (Erro ocorrido)
## Grava em um arquivo ordenando pelo erro ocorrido

## < Funções importada comuns
import sys
sys.path.insert(0, str(os.getcwd() + '\\config-commons'))
from configCommons import configVariaveis, outFileTime, outFileSame, dataHoje
configVariaveis()
sourceFile = outFileSame()
## Funções importadas comuns >

dynamodb = boto3.resource('dynamodb')
AtendimentoStatusFullLog = dynamodb.Table('AtendimentoStatusFullLog')
tableAtendimento = dynamodb.Table('Atendimento')

dataInicio = '2022-05-01T00:00:00.000Z'
print('Periodo:', dataInicio, ' até ',dataHoje())
print('Periodo:', dataInicio, ' ate ',dataHoje(), file=sourceFile)
print('id', ';', 'idAtendimento',';','tipoInteracao',';','ABERTO', ';','statusSN', ';',
      'mensagem', ';', 'objeto', file=sourceFile)

response = AtendimentoStatusFullLog.scan(
    FilterExpression=Attr('dataHora').between(dataInicio, dataHoje()) 
    & Attr('Status').eq('ERRO') )

registros = response['Items']
registros.sort(key=lambda x: x["dataHora"])
historico = []

def salvarRegistro(item):
    itemNovo = {}
    itemNovo['id']= item['id']
    itemNovo['idAtendimento']= item['idAtendimento']
    itemNovo['tipoInteracao']= item['tipoInteracao']
    itemNovo['mensagem']= dadosObjeto(item)
    itemNovo['original']= item
    historico.append(itemNovo)
    print('- Processo:', str(len(historico)), end='\r')

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

for item in response['Items']:
    salvarRegistro(item)

while 'LastEvaluatedKey' in response:
    response = AtendimentoStatusFullLog.scan(
        FilterExpression=Attr('dataHora').between(dataInicio, dataHoje()) 
        & Attr('Status').eq('ERRO'),
        ExclusiveStartKey=response['LastEvaluatedKey'])
    for item in response['Items']:
        salvarRegistro(item)

#### Verificar se tem o caso
print("Verificar se tem o caso no SN")

with tqdm(total=len(historico)) as barra_progresso1:   
    for item in historico:
        responseAtendimento = tableAtendimento.query(
            IndexName="id-index",
            KeyConditionExpression=Key('id').eq(item['idAtendimento']))
        
        item["aberto"] = "NAO"
        item["statusSN"] = "Sem Atendimento - Validação"
        if len(responseAtendimento['Items']) > 0: 
            item["statusSN"] = "Com Atendimento aberto"
            if 'CS' in responseAtendimento['Items'][0]['caso'] :
                item["aberto"] = "SIM"
                item["statusSN"] = "Caso: " + responseAtendimento['Items'][0]['caso'];
            
        barra_progresso1.update(1)

#####GravarArquivo
print("Salvar Arquivo")

def salvarRegistroFinal(item):
    try:
         print(item['id'],';', item['idAtendimento'],';', item['tipoInteracao'],';',item['aberto'],';',item['statusSN'],';',
               unidecode(item['mensagem']),';', item['original'], file=sourceFile)
    except Exception as e:
        print('Erro ao salvar itemNovo ' + str(e))
        pass

historico.sort(key=lambda x: x["mensagem"])

with tqdm(total=len(historico)) as barra_progresso:       
    for item in historico:
        barra_progresso.update(1)
        salvarRegistroFinal(item)

sourceFile.close()
print('\nPROCESSO FINALIZADO<=')
