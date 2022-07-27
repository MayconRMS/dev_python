import os
import sys
from datetime import datetime
from pathlib import Path
import shutil

#print("Atenção: Arquivo de configurações não existe processos!\n")
os.system('cls||clear')
hoje = datetime.now()
dataHoraStr = hoje.strftime('%d_%m_%Y-%H_%M_%S')
dataHora = hoje.strftime('%d/%m/%Y %H:%M:%S')
file_path = sys.path[1]
file_path_name = Path(file_path).stem
file_name = sys.argv[0].split('\\')[-1].replace('.py','')

def configVariaveis():
    os.environ['AWS_PROFILE'] = "prod"
    os.environ['AWS_DEFAULT_REGION'] = "us-east-1" 
    print("\nEXECUTANDO EM <" + os.environ['AWS_PROFILE'] + ">")
    print('POR FAVOR AGUARDE, ISSO PODE LEVAR ALGUM TEMPO...' , dataHora)
    
# Nome do arquivo de saida com a data
def outFileTime():
    sourceFile= open(file_path + '/out_' + os.environ['AWS_PROFILE'] + "_"
                     + file_name + "_" + dataHoraStr +'.csv', 'w', encoding="utf-8")
    return sourceFile

# Nome do arquivo de saida sempre out
def outFileSame():
    sourceFile= open(file_path + '/out_' +  os.environ['AWS_PROFILE'] + "_"
                     + file_name + '.csv', 'w', encoding="utf-8")
    return sourceFile

def dataHoje():
    dataHoje = hoje.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return dataHoje

# Criar diretorio 
def outPathSameNew(pathNew):
    try:
        diretorio = file_path + "/" + pathNew + "_" + os.environ['AWS_PROFILE']
        if os.path.isdir(diretorio):
            shutil.rmtree(diretorio)
        os.makedirs(diretorio)
        return diretorio
    except Exception as e:
        print('Erro ao criar diretorio:' + str(e))
        sys.exit()
        
def outFileSameNew(pathNew, pathName):
    try:
        return open(pathNew + '/out_' + pathName + '.csv', 'w', encoding="utf-8")
    except Exception as e:
        print('Erro ao criar arquivo:' + str(e))
        sys.exit()        