import psycopg2
import time
import pygsheets
import pandas as pd
from datetime import datetime
import os
import json
from pathlib import Path

############# CONFIGURAÇÃO DO GOOGLE SHEETS #############

try:
    print("Conectando ao Google Sheets...")
    gc = pygsheets.authorize(service_file=os.getenv('GOOGLE_SHEETS_CREDS', 'controles.json'))
    sh = gc.open('Daily Balance - Nox Pay')

    # Conectando às abas
    wks_JACI = sh.worksheet_by_title("DATABASE JACI")
    print("✓ Conectado à aba DATABASE JACI")
    
    wks_backtxs = sh.worksheet_by_title("Backoffice Ajustes")
    print("✓ Conectado à aba Backoffice Ajustes")
    
    wks_balances = sh.worksheet_by_title("jaci")
    print("✓ Conectado à aba jaci")
    
    print("Conexão com Google Sheets estabelecida com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao Google Sheets: {e}")
    raise

############# FUNÇÃO PARA OBTER SALDO TOTAL POR MERCHANT #############

# Arquivo para armazenar os saldos da meia-noite
SALDOS_FILE = "saldos_meia_noite.json"

def load_saldos_meia_noite():
    """Carrega os saldos da meia-noite do arquivo"""
    if Path(SALDOS_FILE).exists():
        with open(SALDOS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_saldos_meia_noite(saldos):
    """Salva os saldos da meia-noite no arquivo"""
    with open(SALDOS_FILE, 'w') as f:
        json.dump(saldos, f)

def get_balances(cursor):
    """Obtém os saldos das contas: atual e da meia-noite (horário de Brasília)"""
    try:
        query = """
        SELECT 
            cm.id AS merchant_id,
            cm.balance_decimal AS saldo_atual,
            cm.name_text,
            COALESCE(
                (SELECT SUM(CASE 
                    WHEN status_text = 'PAID' AND method_text = 'PIX' THEN amount_decimal
                    WHEN status_text = 'PAID' AND method_text = 'PIXOUT' THEN -amount_decimal
                    WHEN status_text = 'REFUNDED' THEN -amount_decimal
                    ELSE 0
                END)
                FROM public.core_payment cp
                WHERE cp.merchant_id = cm.id
                AND cp.created_at_date >= (DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'GMT')
                AND cp.created_at_date < NOW()
                AND cp.status_text IN ('PAID', 'REFUNDED')
                AND cp.method_text IN ('PIX', 'PIXOUT')), 0
            ) as total_transacoes
        FROM public.core_merchant cm
        ORDER BY cm.id ASC;
        """
        
        print("Executando query de saldos...")
        cursor.execute(query)
        results = cursor.fetchall()
        
        df = pd.DataFrame(results, columns=["merchant_id", "saldo_atual", "name_text", "total_transacoes"])
        df["saldo_0h"] = df["saldo_atual"] - df["total_transacoes"]
        df = df[["merchant_id", "saldo_atual", "saldo_0h", "name_text"]]
        
        print(f"✓ Query de saldos retornou {len(df)} registros")
        return df

    except Exception as e:
        print(f"Erro ao obter saldos das contas: {e}")
        return pd.DataFrame(columns=["merchant_id", "saldo_atual", "saldo_0h", "name_text"])

############# FUNÇÃO PARA OBTER ÚLTIMA LINHA PREENCHIDA #############

def get_last_row(worksheet):
    """Obtém a última linha preenchida de uma aba do Google Sheets."""
    try:
        last_row = len(worksheet.get_col(9, include_tailing_empty=False)) + 1
        print(f"Última linha encontrada em {worksheet.title}: {last_row}")
        return last_row
    except Exception as e:
        print(f"Erro ao obter última linha: {e}")
        return 1

############# FUNÇÃO PARA OBTER PAGAMENTOS EM TEMPO REAL #################

def get_payments(cursor):
    """Obtém os pagamentos processados do dia atual."""
    try:
        query = """
        SELECT DISTINCT
            DATE_TRUNC('day', cp.created_at_date AT TIME ZONE 'America/Sao_Paulo') AS data, 
            cm.name_text AS merchant, 
            cp.provider_text AS provider, 
            cp.method_text AS meth, 
            COUNT(*) AS quantidade, 
            SUM(cp.amount_decimal) AS volume
        FROM core_payment cp 
        JOIN core_merchant cm ON cm.id = cp.merchant_id
        WHERE cp.status_text = 'PAID' 
        AND cp.created_at_date >= (DATE_TRUNC('day', NOW() AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'GMT')
        -- Apenas dados do dia atual considerando horário de Brasília
        GROUP BY data, merchant, cm.name_text, cp.provider_text, cp.method_text
        ORDER BY data DESC;
        """
        print("Executando query de pagamentos do dia...")
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["data", "merchant", "provider", "meth", "quantidade", "volume"])
        
        if not df.empty:
            df = df.drop_duplicates()
            print(f"✓ Query de pagamentos retornou {len(df)} registros do dia")
        return df
    except Exception as e:
        print(f"Erro ao obter pagamentos: {e}")
        return pd.DataFrame()

############# FUNÇÃO PARA OBTER TRANSAÇÕES DO BACKOFFICE EM TEMPO REAL #################

def get_backtransactions(cursor):
    """Obtém transações do Backoffice do dia atual."""
    try:
        query = """
        SELECT DISTINCT
            (SELECT cm2.name_text FROM core_merchant cm2 WHERE id = merchant_id) AS merchant,
            description_text AS descricao,
            SUM(amount_decimal) AS valor_total,
            DATE_TRUNC('minute', created_at_date) AS data_criacao,
            MAX(created_at_date) as ultima_atualizacao
        FROM public.core_backofficetrasactions
        WHERE created_at_date >= DATE_TRUNC('day', NOW())  -- Apenas dados do dia atual
        GROUP BY DATE_TRUNC('minute', created_at_date), merchant_id, descricao
        ORDER BY ultima_atualizacao ASC
        LIMIT 100;
        """
        print("Executando query de backoffice do dia...")
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=["merchant", "descricao", "valor_total", "data_criacao", "ultima_atualizacao"])
        
        if not df.empty:
            df = df.drop(columns=["ultima_atualizacao"])
            df = df.drop_duplicates()
            print(f"✓ Query de backoffice retornou {len(df)} registros do dia")
        return df
    except Exception as e:
        print(f"Erro ao obter transações do backoffice: {e}")
        return pd.DataFrame()

############# LOOP PRINCIPAL - TEMPO REAL #############

print("\nIniciando loop principal...")
while True:
    try:
        current_time = datetime.now()
        print(f"\n{'='*50}")
        print(f"Nova atualização iniciada em: {current_time}")
        print(f"{'='*50}")

        # Executa a cópia dos saldos à meia-noite
        if current_time.hour == 0 and current_time.minute == 0:
            print("Meia-noite detectada, aguardando 1 minuto...")
            time.sleep(60)
        
        # Conexão com o banco de dados
        print("\nConectando ao banco de dados...")
        with psycopg2.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'),
            port=int(os.getenv('DB_PORT', "5432"))
        ) as conn:
            print("✓ Conexão estabelecida com sucesso")
            
            with conn.cursor() as cursor:
                # Atualiza saldos
                print("\nAtualizando saldos...")
                df_balances = get_balances(cursor)
                if not df_balances.empty:
                    wks_balances.set_dataframe(df_balances, (1, 1), encoding="utf-8", copy_head=True)
                    print("✓ Saldos atualizados com sucesso na aba 'jaci'")

                # Atualiza pagamentos   
                print("\nAtualizando pagamentos...")
                df_payments = get_payments(cursor)
                if not df_payments.empty:
                    last_row_JACI = get_last_row(wks_JACI)
                    wks_JACI.set_dataframe(df_payments, (last_row_JACI, 1), encoding="utf-8", copy_head=False)
                    print("✓ Pagamentos atualizados com sucesso na aba 'DATABASE JACI'")

                # Atualiza backoffice
                print("\nAtualizando transações do backoffice...")
                df_backtxs = get_backtransactions(cursor)
                if not df_backtxs.empty:
                    last_row_backtxs = get_last_row(wks_backtxs)
                    wks_backtxs.set_dataframe(df_backtxs, (last_row_backtxs, 1), encoding="utf-8", copy_head=False)
                    print("✓ Transações do backoffice atualizadas com sucesso na aba 'Backoffice Ajustes'")
           
    except Exception as e:
        print(f"\nERRO CRÍTICO: {e}")
        print("Fechando conexão antiga...")
        try:
            cursor.close()
            conn.close()
        except:
            pass
        print("Tentando reiniciar o loop em 60 segundos...")
        time.sleep(60)
        continue

    print(f"\nAtualização concluída em: {datetime.now()}")
    print("Aguardando 60 segundos para próxima atualização...")
    time.sleep(60)

# Configurações do Banco de Dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT', "5432"))
}

# Configurações do Google Sheets
SHEETS_CONFIG = {
    'service_file': 'controles.json',
    'spreadsheet_name': 'Daily Balance - Nox Pay',
    'worksheets': {
        'jaci_data': "DATABASE JACI",
        'backoffice': "Backoffice Ajustes",
        'balances': "jaci"
    }
}