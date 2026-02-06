import requests
import pandas as pd



# 1. Função para extrair vendas da Fake Store API
def extract_sales():
    endpoint = "https://fakestoreapi.com/carts"

    try:
        response = requests.get(endpoint, timeout=10)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro ao extrair vendas: {response.status_code}")
            return None

    except requests.exceptions.ConnectionError as e:
        print(f"Erro de conexão: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro inesperado: {e}")
        return None



# 2. Execução principal
if __name__ == "__main__":

    print("📡 Extraindo dados de vendas da Fake Store API...")

    sales_data = extract_sales()

    if not sales_data:
        print("❌ Nenhum dado extraído")
        exit(1)

    # Converte JSON em DataFrame (20 vendas)
    sales_df = pd.DataFrame(sales_data)

    print(f"🔢 Vendas originais: {len(sales_df)}")


    # 3. SIMULAÇÃO DE MAIS VENDAS (OPÇÃO 2)
    # Duplica os dados 10 vezes → 200 vendas
    sales_df = pd.concat([sales_df] * 10, ignore_index=True)

    print(f"🔢 Vendas após simulação: {len(sales_df)}")

   
    # 4. Salvar dados brutos 
    output_path = "data/raw/sales_raw.csv"
    sales_df.to_csv(output_path, index=False)

    print(f"✅ Dados de vendas salvos em {output_path}")
