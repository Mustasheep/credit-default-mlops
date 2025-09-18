"""
Script para testar o pipeline de dados localmente
"""
import os
import sys
import pandas as pd
from pathlib import Path

# Diretórios base
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "credit_default_dataset.csv"

# Adicionar src ao path
sys.path.append(str(PROJECT_ROOT / "src"))


def test_data_ingestion():
    """Testa o componente de data ingestion localmente"""
    print("\n🧪 Testando Data Ingestion...")

    # Usar caminho absoluto para o dataset
    input_data = str(DATA_PATH)
    output_data = BASE_DIR / "test_outputs" / "ingestion"

    # Criar pasta de saída
    output_data.mkdir(parents=True, exist_ok=True)

    # Executar componente
    from components.data_ingestion.data_ingestion import ingest_data

    try:
        ingest_data(input_data, str(output_data))
        print("✅ Data Ingestion - PASSOU")
        return True
    except Exception as e:
        print(f"❌ Data Ingestion - FALHOU: {e}")
        return False


def test_data_preprocessing():
    """Testa o componente de data preprocessing localmente"""
    print("\n🧪 Testando Data Preprocessing...")

    input_data = BASE_DIR / "test_outputs" / "ingestion"
    output_data = BASE_DIR / "test_outputs" / "preprocessing"

    # Criar pasta de saída
    output_data.mkdir(parents=True, exist_ok=True)

    # Executar componente
    from components.data_preprocessing.data_preprocessing import preprocess_data

    try:
        preprocess_data(str(input_data), str(output_data))
        print("✅ Data Preprocessing - PASSOU")
        return True
    except Exception as e:
        print(f"❌ Data Preprocessing - FALHOU: {e}")
        return False


def validate_outputs():
    """Valida se os outputs foram gerados corretamente"""
    print("\n🔁 Validando outputs...")

    ingestion_files = [
        BASE_DIR / "test_outputs" / "ingestion" / "raw_data.csv",
        BASE_DIR / "test_outputs" / "ingestion" / "metadata.json"
    ]
    preprocessing_files = [
        BASE_DIR / "test_outputs" / "preprocessing" / "processed_data.csv",
        BASE_DIR / "test_outputs" / "preprocessing" / "features.csv",
        BASE_DIR / "test_outputs" / "preprocessing" / "target.csv",
        BASE_DIR / "test_outputs" / "preprocessing" / "preprocessing_metadata.json"
    ]

    all_files = ingestion_files + preprocessing_files
    missing_files = []

    for file_path in all_files:
        if not file_path.exists():
            missing_files.append(file_path)
        else:
            print(f"✅ {file_path}")

    if missing_files:
        print(f"❌ Arquivos faltantes: {missing_files}")
        return False

    # Validar conteúdo dos arquivos principais
    try:
        processed_data = pd.read_csv(BASE_DIR / "test_outputs/preprocessing/processed_data.csv")
        features_data = pd.read_csv(BASE_DIR / "test_outputs/preprocessing/features.csv")
        target_data = pd.read_csv(BASE_DIR / "test_outputs/preprocessing/target.csv")

        print(f"📊 Dados processados: {processed_data.shape}")
        print(f"📊 Features: {features_data.shape}")
        print(f"📊 Target: {target_data.shape}")

        if len(features_data) != len(target_data):
            print("❌ Inconsistência entre features e target")
            return False

        print("✅ Validação dos outputs - PASSOU")
        return True

    except Exception as e:
        print(f"❌ Erro na validação: {e}")
        return False


def main():
    """Executa todos os testes"""
    print("=" * 45)
    print("|   INICIANDO TESTES DO PIPELINE DE DADOS   |")
    print("=" * 45)

    # Verificar se dataset existe
    if not DATA_PATH.exists():
        print(f"❌ Dataset não encontrado: {DATA_PATH}")
        print("💡 Execute primeiro a análise exploratória para gerar o dataset\n")
        return False

    tests_passed = 0
    total_tests = 3

    if test_data_ingestion():
        tests_passed += 1

    if test_data_preprocessing():
        tests_passed += 1

    if validate_outputs():
        tests_passed += 1

    print("=" * 50)
    print(f"\n📊 RESULTADO: {tests_passed}/{total_tests} testes passaram")

    if tests_passed == total_tests:
        print("TODOS OS TESTES PASSARAM!")
        print("✅ Pipeline de dados está funcionando corretamente\n")
        return True
    else:
        print("⚠️ ALGUNS TESTES FALHARAM")
        print("Revise os componentes que falharam\n")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
