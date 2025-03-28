import pygsheets
import pandas as pd
import time
from datetime import datetime
from math import floor
from time import sleep
import sys
import json
import paramiko
import atexit
import os

############# CONFIGURAÇÕES ###################################################

# Configuração da API
api_token_NOX = os.getenv('API_TOKEN_NOX')
url_financial = "https://api.iugu.com/v1/accounts/financial"

# Configuração SSH
SSH_HOST = os.getenv('SSH_HOST')
SSH_PORT = int(os.getenv('SSH_PORT', "22"))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')

# Variáveis globais
ssh_client = None

############# CONFIGURAÇÃO SSH #############

def connect_ssh():
    """
    Estabelece uma conexão SSH com o servidor.
    """
    global ssh_client
    
    try:
        print(f"Conectando ao servidor SSH {SSH_HOST}...")
        
        # Cria o cliente SSH
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=SSH_HOST, 
            port=SSH_PORT, 
            username=SSH_USERNAME, 
            password=SSH_PASSWORD,
            allow_agent=False,
            look_for_keys=False,
            timeout=120
        )
        
        print("Conexão SSH estabelecida com sucesso.")
        return ssh_client
            
    except Exception as e:
        print(f"Erro ao conectar ao servidor SSH: {e}")
        close_ssh()
        return None

def close_ssh():
    """Encerra a conexão SSH."""
    global ssh_client
    
    if ssh_client:
        try:
            ssh_client.close()
            print("Conexão SSH encerrada.")
        except:
            pass
        ssh_client = None

atexit.register(close_ssh)

def execute_curl(url, token):
    """
    Executa um comando curl no servidor SSH para fazer uma requisição à API.
    Isso garante que a requisição saia do IP do servidor.
    """
    if not ssh_client:
        print("Erro: Sem conexão SSH estabelecida")
        return None
    
    try:
        curl_cmd = f'curl -s -X GET "{url}?api_token={token}" -H "accept: application/json"'
        
        print(f"Executando comando no servidor...")
        
        stdin, stdout, stderr = ssh_client.exec_command(curl_cmd, timeout=60)
        
        response = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        
        if error:
            print(f"Erro na execução do comando: {error}")
            return None
            
        try:
            return json.loads(response)
        except Exception as e:
            print(f"Erro ao processar resposta JSON: {e}")
            print(f"Resposta: {response}")
            return None
    
    except Exception as e:
        print(f"Erro ao executar comando no servidor: {e}")
        return None

############# CONEXÃO COM GOOGLE SHEETS ###########

try:
    gc = pygsheets.authorize(service_file="controles.json")

    sh_gateway = gc.open("Gateway")
    wks_subcontas = sh_gateway.worksheet_by_title("Subcontas")
    sh_balance = gc.open("Daily Balance - Nox Pay")

    wks_IUGU_subacc = sh_balance.worksheet_by_title("IUGU Subcontas")
except Exception as e:
    print(f"Erro ao conectar ao Google Sheets: {e}")
    sys.exit(1)

############# TRIGGER #############

def check_trigger(wks_IUGU_subacc):
    """Verifica se a célula B1 contém TRUE para executar o script."""
    try:
        status = wks_IUGU_subacc.get_value("B1")
        return status.strip().upper() == "TRUE"
    except Exception as e:
        print(f"Erro ao verificar trigger: {e}")
        return False

def reset_trigger(wks_IUGU_subacc):
    """Após a execução, redefine a célula B1 para FALSE."""
    try:
        wks_IUGU_subacc.update_value("B1", "FALSE")
    except Exception as e:
        print(f"Erro ao resetar trigger: {e}")

def update_status(wks_IUGU_subacc, status):
    """Atualiza o status de execução na célula A1."""
    try:
        wks_IUGU_subacc.update_value("A1", status)
    except Exception as e:
        print(f"Erro ao atualizar status: {e}")

############# FUNÇÃO PARA OBTER SALDO COM `start` #############################

def fetch_balance_with_start(token):
    """
    Obtém o saldo mais recente da conta, utilizando `start` para pular registros
    e garantir que a última requisição tenha menos de 1000 transações.
    """
    try:
        url_last = f"{url_financial}?limit=1&sort=-created_at"
        response_data = execute_curl(url_last, token)
        
        if response_data and response_data.get('transactions'):
            last_transaction = response_data['transactions'][0]
            return (
                last_transaction["balance"],
                float(last_transaction["balance_cents"]) / 100,
                response_data.get("transactions_total", 0)
            )
        
        print("Tentando método alternativo...")
        response_data = execute_curl(url_financial, token)
        
        if not response_data:
            print("Não foi possível obter dados da API")
            return None, 0, 0
            
        transactions_total = response_data.get("transactions_total", 0)
        
        transactions = response_data.get("transactions", [])
        if transactions:
            last_transaction = transactions[-1]
            return (
                last_transaction["balance"],
                float(last_transaction["balance_cents"]) / 100,
                transactions_total
            )

    except Exception as e:
        print(f"Erro ao buscar transações: {e}")

    return None, 0, 0

############# CONSULTAR SALDOS ################################################

def consultar_saldos():
    """
    Função principal que obtém os saldos das subcontas e exporta para o Google Sheets.
    """
    df_subcontas = pd.DataFrame(wks_subcontas.get_all_records())

    df_subcontas_ativas = df_subcontas[df_subcontas["NOX"] == "SIM"]

    df_subcontas_ativas = df_subcontas_ativas[["live_token_full", "account"]]

    saldo_subcontas = []

    for _, row in df_subcontas_ativas.iterrows():
        try:
            token = row["live_token_full"]
            account = row["account"]

            saldo_final, saldo_cents, transactions_total = fetch_balance_with_start(token)

            saldo_subcontas.append({
                "account": account,
                "transactions_total": transactions_total,
                "saldo_cents": saldo_cents
            })

            print(f"Processado: {account} | Saldo: {saldo_final} | Transações: {transactions_total}")

            sleep(2)

        except Exception as e:
            print(f"Erro ao processar a subconta {row['account']}: {e}")
            saldo_subcontas.append({
                "account": row['account'],
                "transactions_total": 0,
                "saldo_cents": 0
            })

    df_saldo_subcontas = pd.DataFrame(saldo_subcontas)

    ############# EXPORTAR PARA O GOOGLE SHEETS ##############################

    rodado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    wks_IUGU_subacc.update_value("A1", f"Última atualização: {rodado}")

    wks_IUGU_subacc.set_dataframe(df_saldo_subcontas, (2, 1), encoding="utf-8", copy_head=True)

    print(f"Execução concluída: {rodado}")

############# AGENDADOR ######################################

def main():
    print(f"Verificando trigger em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Verifica o trigger primeiro
    if not check_trigger(wks_IUGU_subacc):
        print("Trigger não está ativo (B1 = FALSE). Aguardando próxima verificação.")
        sys.exit(0)
    
    print("Trigger ativo! Iniciando processo de atualização...")
    
    if not connect_ssh():
        print("AVISO: Não foi possível estabelecer conexão SSH.")
        sys.exit(1)

    try:
        update_status(wks_IUGU_subacc, "Atualizando...")
        consultar_saldos()
        
        last_update = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        update_status(wks_IUGU_subacc, f"Última atualização: {last_update}")
        
        reset_trigger(wks_IUGU_subacc)
        print("Atualização concluída com sucesso!")
        
    except Exception as e:
        print(f"Erro inesperado: {e}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)
    finally:
        close_ssh()

if __name__ == "__main__":
    main()