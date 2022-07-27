import boto3
import os

os.environ['AWS_PROFILE'] = "dev"
os.environ['AWS_DEFAULT_REGION'] = "us-east-1" 
print("\nEXECUTANDO EM <" + os.environ['AWS_PROFILE'] + ">")
print('POR FAVOR AGUARDE, ISSO PODE LEVAR ALGUM TEMPO...')

client = boto3.client('dynamodb')

stmt = "SELECT * FROM Identificador WHERE codIdentificador = '1019023179'"
response = client.execute_statement(Statement= stmt)

print("\nIdentificador")
def salvarRegistro(item):
    print(item["codIdentificador"])

for item in response['Items']:
    salvarRegistro(item)

stmtLog = "SELECT * FROM IdentificadorMaaSLog WHERE codIdentificador = '1019023179'"
response2 = client.execute_statement(Statement= stmtLog)

print("\nIdentificadorMaaSLog")
def salvarRegistro(item):
    print(item["codIdentificador"])

for item in response2['Items']:
    salvarRegistro(item)

stmtAll = ""
print("\n")
