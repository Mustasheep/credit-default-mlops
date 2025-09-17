"""
Componente de Data Preprocessing para Azure ML Pipeline
Responsável por limpeza, transformação e feature engineering
"""
import argparse
import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path
from sklearn.preprocessing import StandardScaler, LabelEncoder
import json

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse argumentos da linha de comando"""
    parser = argparse.ArgumentParser(description="Data Preprocessing Component")
    parser.add_argument("--input_data", type=str, help="Caminho para dados de entrada")
    parser.add_argument("--output_data", type=str, help="Caminho para dados processados")
    return parser.parse_args()

def preprocess_data(input_path: str, output_path: str):
    """
    Função principal de pré-processamento dos dados

    Args:
        input_path (str): Caminho para os dados de entrada
        output_path (str): Caminho para salvar os dados processados
    """
    logger.info("Iniciando pré-processamento de dados...")

    # Criar diretório de saída
    os.makedirs(output_path, exist_ok=True)

    try:
        # Carregar dados
        input_file = os.path.join(input_path, "raw_data.csv")
        logger.info(f"Carregando dados de: {input_file}")
        df = pd.read_csv(input_file)

        # Backup dos dados originais
        original_shape = df.shape
        logger.info(f"Dados originais: {original_shape}")

        # 1. Limpeza de dados
        logger.info("Executando limpeza de dados...")

        # Remover duplicatas baseado no ID
        duplicates_before = len(df)
        df = df.drop_duplicates(subset=['ID'], keep='first')
        duplicates_removed = duplicates_before - len(df)
        if duplicates_removed > 0:
            logger.info(f"Removidas {duplicates_removed} linhas duplicadas")

        # Tratar valores faltantes (se houver)
        missing_values = df.isnull().sum().sum()
        if missing_values > 0:
            logger.info(f"Tratando {missing_values} valores faltantes...")
            # Para este dataset, preencher com mediana para numéricos
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

        # 2. Feature Engineering
        logger.info("Executando feature engineering...")

        # Criar features derivadas do histórico de pagamentos
        pay_cols = ['PAY_0', 'PAY_1', 'PAY_2', 'PAY_3', 'PAY_4', 'PAY_5']
        df['avg_payment_delay'] = df[pay_cols].apply(
            lambda x: x[x > 0].mean() if any(x > 0) else 0, axis=1
        )
        df['total_delayed_months'] = df[pay_cols].apply(lambda x: sum(x > 0), axis=1)
        df['max_delay'] = df[pay_cols].max(axis=1)

        # Criar features financeiras
        bill_cols = ['BILL_AMT1', 'BILL_AMT2', 'BILL_AMT3', 'BILL_AMT4', 'BILL_AMT5', 'BILL_AMT6']
        pay_amt_cols = ['PAY_AMT1', 'PAY_AMT2', 'PAY_AMT3', 'PAY_AMT4', 'PAY_AMT5', 'PAY_AMT6']

        df['avg_bill_amount'] = df[bill_cols].mean(axis=1)
        df['avg_payment_amount'] = df[pay_amt_cols].mean(axis=1)
        df['bill_payment_ratio'] = df['avg_payment_amount'] / (df['avg_bill_amount'].abs() + 1)
        df['credit_utilization'] = (df['avg_bill_amount'] / df['LIMIT_BAL']).clip(0, 2)

        # Criar categorias de idade
        df['age_group'] = pd.cut(df['AGE'], 
                               bins=[0, 25, 35, 45, 55, 100], 
                               labels=[1, 2, 3, 4, 5])  # Numérico para manter compatibilidade

        # Criar categorias de limite de crédito
        df['limit_category'] = pd.cut(df['LIMIT_BAL'], 
                                    bins=[0, 50000, 150000, 300000, 500000, float('inf')], 
                                    labels=[1, 2, 3, 4, 5])  # Numérico

        # 3. Validação de dados processados
        logger.info("Validando dados processados...")

        # Verificar se não há valores infinitos
        inf_values = np.isinf(df.select_dtypes(include=[np.number])).sum().sum()
        if inf_values > 0:
            logger.warning(f"⚠️  Encontrados {inf_values} valores infinitos - corrigindo...")
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

        # Verificar distribuição da variável target
        target_dist = df['default'].value_counts(normalize=True)
        logger.info(f"Distribuição target: {target_dist.to_dict()}")

        # 4. Salvar dados processados
        logger.info("💾 Salvando dados processados...")

        # Salvar dataset completo
        processed_file = os.path.join(output_path, "processed_data.csv")
        df.to_csv(processed_file, index=False)

        # Salvar apenas features para treinamento (sem ID)
        feature_cols = [col for col in df.columns if col not in ['ID', 'default']]
        target_col = 'default'

        X = df[feature_cols]
        y = df[target_col]

        # Salvar X e y separadamente
        X.to_csv(os.path.join(output_path, "features.csv"), index=False)
        y.to_csv(os.path.join(output_path, "target.csv"), index=False)

        # Salvar metadados do processamento
        preprocessing_metadata = {
            "original_shape": [int(original_shape[0]), int(original_shape[1])],
            "processed_shape": [int(df.shape[0]), int(df.shape[1])],
            "duplicates_removed": int(duplicates_removed),
            "missing_values_treated": int(missing_values),
            "new_features_created": [
                "avg_payment_delay", "total_delayed_months", "max_delay",
                "avg_bill_amount", "avg_payment_amount", "bill_payment_ratio", 
                "credit_utilization", "age_group", "limit_category"
            ],
            "feature_columns": list(map(str, feature_cols)),
            "target_distribution": {str(k): float(v) for k, v in target_dist.to_dict().items()},
            "total_features": int(len(feature_cols))
        }

        metadata_file = os.path.join(output_path, "preprocessing_metadata.json")
        with open(metadata_file, "w") as f:
            json.dump(preprocessing_metadata, f, indent=2)


        logger.info(f"✅ Pré-processamento concluído!")
        logger.info(f"Shape final: {df.shape}")
        logger.info(f"Features criadas: {len(feature_cols)}")

    except Exception as e:
        logger.error(f"❌ Erro no pré-processamento: {str(e)}")
        raise

def main():
    """Função principal"""
    args = parse_args()
    preprocess_data(args.input_data, args.output_data)

if __name__ == "__main__":
    main()
