"""
Componente de Data Ingestion para Azure ML Pipeline
Responsável por carregar dados e validações básicas
"""
import argparse
import pandas as pd
import os
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse argumentos da linha de comando"""
    parser = argparse.ArgumentParser(description="Data Ingestion Component")
    parser.add_argument("--input_data", type=str, help="Caminho para dados de entrada")
    parser.add_argument("--output_data", type=str, help="Caminho para dados de saída")
    return parser.parse_args()

def ingest_data(input_path: str, output_path: str):
    """
    Função principal de ingestão de dados

    Args:
        input_path (str): Caminho para os dados de entrada
        output_path (str): Caminho para salvar os dados processados
    """
    logger.info("Iniciando ingestão de dados...")

    # Criar diretório de saída se não existir
    os.makedirs(output_path, exist_ok=True)

    try:
        # Carregar dados
        logger.info(f"Carregando dados de: {input_path}")

        # Verificar se é arquivo ou pasta
        if os.path.isfile(input_path):
            df = pd.read_csv(input_path)
        else:
            # Buscar arquivos CSV na pasta
            csv_files = list(Path(input_path).glob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {input_path}")
            df = pd.read_csv(csv_files[0])

        # Validações básicas
        logger.info("Executando validações básicas...")

        # Verificar se dataset não está vazio
        if df.empty:
            raise ValueError("Dataset está vazio!")

        # Verificar se tem coluna target
        if 'default' not in df.columns:
            raise ValueError("Coluna 'default' não encontrada!")

        # Log estatísticas básicas
        logger.info(f"Dataset carregado: {df.shape[0]} linhas, {df.shape[1]} colunas")
        logger.info(f"Taxa de inadimplência: {df['default'].mean():.3f}")
        logger.info(f"Valores faltantes: {df.isnull().sum().sum()}")

        # Salvar dados validados
        output_file = os.path.join(output_path, "raw_data.csv")
        df.to_csv(output_file, index=False)
        logger.info(f"💾 Dados salvos em: {output_file}")

        # Salvar metadados
        metadata = {
            "total_records": int(len(df)),
            "total_features": int(len(df.columns)),
            "default_rate": float(df['default'].mean()),
            "missing_values": int(df.isnull().sum().sum()),
            "columns": df.columns.tolist()
        }

        import json
        metadata_file = os.path.join(output_path, "metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info("✅ Ingestão de dados concluída com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro na ingestão de dados: {str(e)}")
        raise

def main():
    """Função principal"""
    args = parse_args()
    ingest_data(args.input_data, args.output_data)

if __name__ == "__main__":
    main()
