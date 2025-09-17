from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
import json
import os

def test_workspace_connection():
    """Testa conexão com workspace Azure ML"""
    try:
        credential = DefaultAzureCredential()
        ml_client = MLClient.from_config(credential=credential)
        print(f"    ✅ Conectado ao workspace: {ml_client.workspace_name}")

        computes = ml_client.compute.list()
        print(f"    ✅ Computes disponíveis: {[c.name for c in computes]}")

        return True
    
    except Exception as e:
        print(f"    ❌ Erro na conexão: {e}")

        return False

def test_environment_variables():
    """Verifica variáveis de ambiente"""
    required_vars = ['AZURE_CLIENTE_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID']

    for var in required_vars:
        if var in os.environ:
            print(f"    ✅ {var} configurado")
        else:
            print(f"    ⚠️  {var} não encontrado")

if __name__ == "__main__":
    print("\n🔍 Validando configuração do ambiente...")
    test_workspace_connection()
    test_environment_variables()
    print("    ✅ Validação concluída!\n")