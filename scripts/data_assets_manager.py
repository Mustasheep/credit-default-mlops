"""
Sistema de Data Assets para Azure ML
Gerencia datasets, versionamento e lineage na nuvem
"""
from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.ai.ml.constants import AssetTypes
from azure.identity import DefaultAzureCredential
from datetime import datetime
import pandas as pd
import json
import os

class DataAssetsManager:
    """Gerenciador de Data Assets no Azure ML"""

    def __init__(self, ml_client: MLClient):
        self.ml_client = ml_client
        self.asset_registry = {}

    def register_dataset_as_asset(self, dataset_path: str, asset_name: str, 
                                 asset_version: str = None, description: str = "",
                                 tags: dict = None):
        """
        Registra dataset como Data Asset versionado

        Args:
            dataset_path: Caminho local ou URI para dataset
            asset_name: Nome do asset
            asset_version: Versão (auto se None)
            description: Descrição do asset
            tags: Tags para metadata

        Returns:
            Data Asset registrado
        """

        print(f"📝 REGISTRANDO DATA ASSET: {asset_name}")
        print("=" * 60)
        print(f"Dataset: {dataset_path}")

        try:
            # Gerar versão automaticamente se não fornecida
            if not asset_version:
                asset_version = datetime.now().strftime("%Y.%m.%d.%H%M")

            # Tags padrão
            default_tags = {
                "project": "credit-default-mlops",
                "data_type": "structured",
                "registered_by": "mlops_pipeline",
                "registration_date": datetime.now().isoformat(),
                "source_path": dataset_path
            }

            if tags:
                default_tags.update(tags)

            # Determinar tipo de asset baseado na extensão
            if dataset_path.endswith('.csv'):
                asset_type = AssetTypes.URI_FILE
                data_format = "csv"
            elif dataset_path.endswith('.parquet'):
                asset_type = AssetTypes.URI_FILE
                data_format = "parquet"
            elif os.path.isdir(dataset_path):
                asset_type = AssetTypes.URI_FOLDER
                data_format = "folder"
            else:
                asset_type = AssetTypes.URI_FILE
                data_format = "file"

            default_tags["data_format"] = data_format

            # Criar Data Asset
            data_asset = Data(
                name=asset_name,
                version=asset_version,
                path=dataset_path,
                type=asset_type,
                description=description or f"Data asset for {asset_name}",
                tags=default_tags
            )

            # Registrar no workspace
            registered_asset = self.ml_client.data.create_or_update(data_asset)

            # Armazenar no registry local
            asset_key = f"{asset_name}:{asset_version}"
            self.asset_registry[asset_key] = {
                "name": asset_name,
                "version": asset_version,
                "path": dataset_path,
                "type": asset_type,
                "format": data_format,
                "description": description,
                "tags": default_tags,
                "registered_at": datetime.now().isoformat(),
                "asset_id": registered_asset.id
            }

            print("\n✅ DATA ASSET REGISTRADO!")
            print("=" * 40)
            print(f"🆔 Asset ID: {registered_asset.id}")
            print(f"📄 Nome: {registered_asset.name}")
            print(f"🔢 Versão: {registered_asset.version}")
            print(f"📁 Tipo: {registered_asset.type}")
            print(f"🏷️  Tags: {len(default_tags)} aplicadas")

            return registered_asset

        except Exception as e:
            print(f"❌ Erro ao registrar asset: {e}")
            raise

    def create_processed_data_asset(self, processed_data_path: str, 
                                   source_asset_name: str = None,
                                   processing_info: dict = None):
        """
        Cria asset para dados processados com lineage

        Args:
            processed_data_path: Caminho para dados processados
            source_asset_name: Nome do asset fonte
            processing_info: Informações do processamento

        Returns:
            Asset de dados processados
        """

        print("🔄 CRIANDO ASSET DE DADOS PROCESSADOS")
        print("=" * 60)

        # Tags com lineage information
        processing_tags = {
            "data_stage": "processed",
            "processing_pipeline": "data_preprocessing",
            "source_asset": source_asset_name or "unknown",
            "features_engineered": "true",
            "quality_checked": "true"
        }

        if processing_info:
            processing_tags.update(processing_info)

        # Nome do asset processado
        asset_name = f"credit_default_processed"
        description = "Dataset processado com feature engineering aplicado"

        return self.register_dataset_as_asset(
            dataset_path=processed_data_path,
            asset_name=asset_name,
            description=description,
            tags=processing_tags
        )

    def list_data_assets(self, name_filter: str = None):
        """
        Lista data assets do workspace

        Args:
            name_filter: Filtro opcional por nome

        Returns:
            Lista de assets
        """

        print("📋 DATA ASSETS DO WORKSPACE")
        print("=" * 50)

        try:
            # Listar todos os assets
            assets = list(self.ml_client.data.list())

            # Filtrar por nome se especificado
            if name_filter:
                assets = [asset for asset in assets if name_filter.lower() in asset.name.lower()]

            if not assets:
                print("⚠️  Nenhum data asset encontrado")
                return []

            # Agrupar por nome
            assets_by_name = {}
            for asset in assets:
                if asset.name not in assets_by_name:
                    assets_by_name[asset.name] = []
                assets_by_name[asset.name].append(asset)

            # Mostrar assets agrupados
            print(f"📊 Total: {len(assets)} assets em {len(assets_by_name)} datasets")
            print()

            for asset_name, versions in assets_by_name.items():
                # Ordenar versões (mais recente primeiro)
                versions.sort(key=lambda x: x.creation_context.created_at, reverse=True)
                latest = versions[0]

                print(f"📄 {asset_name}")
                print(f"   🔢 Versões disponíveis: {len(versions)}")
                print(f"   📅 Mais recente: v{latest.version} ({latest.creation_context.created_at.strftime('%Y-%m-%d %H:%M')})")
                print(f"   📁 Tipo: {latest.type}")

                # Mostrar tags importantes
                if latest.tags:
                    important_tags = {k: v for k, v in latest.tags.items() 
                                    if k in ['data_stage', 'data_format', 'source_asset', 'project']}
                    if important_tags:
                        print(f"   🏷️  Tags: {important_tags}")

                print(f"   📝 Descrição: {latest.description}")
                print()

            return assets

        except Exception as e:
            print(f"❌ Erro ao listar assets: {e}")
            return []

    def get_asset_lineage(self, asset_name: str, version: str = "latest"):
        """
        Obtém lineage de um data asset

        Args:
            asset_name: Nome do asset
            version: Versão (latest para mais recente)

        Returns:
            dict: Informações de lineage
        """

        print(f"🔍 LINEAGE DO ASSET: {asset_name} v{version}")
        print("=" * 60)

        try:
            # Obter asset
            if version == "latest":
                asset = self.ml_client.data.get(name=asset_name, label="latest")
            else:
                asset = self.ml_client.data.get(name=asset_name, version=version)

            # Extrair informações de lineage das tags
            lineage_info = {
                "asset_name": asset.name,
                "version": asset.version,
                "creation_time": asset.creation_context.created_at.isoformat(),
                "created_by": getattr(asset.creation_context.created_by, "user_name", str(asset.creation_context.created_by)),
                "source_path": asset.tags.get("source_path") if asset.tags else None,
                "source_asset": asset.tags.get("source_asset") if asset.tags else None,
                "processing_pipeline": asset.tags.get("processing_pipeline") if asset.tags else None,
                "data_stage": asset.tags.get("data_stage") if asset.tags else "raw",
                "tags": asset.tags or {}
            }

            # Mostrar lineage
            print(f"📄 Asset: {lineage_info['asset_name']} v{lineage_info['version']}")
            print(f"📅 Criado: {lineage_info['creation_time'][:19]}")
            print(f"👤 Por: {lineage_info['created_by']}")
            print(f"🎯 Estágio: {lineage_info['data_stage']}")

            if lineage_info['source_asset']:
                print(f"📥 Asset fonte: {lineage_info['source_asset']}")

            if lineage_info['processing_pipeline']:
                print(f"🔄 Pipeline: {lineage_info['processing_pipeline']}")

            if lineage_info['source_path']:
                print(f"📁 Path original: {lineage_info['source_path']}")

            # Mostrar evolução se houver múltiplas versões
            all_versions = list(self.ml_client.data.list(name=asset_name))
            if len(all_versions) > 1:
                print(f"\n📈 Evolução ({len(all_versions)} versões):")

                all_versions.sort(key=lambda x: x.creation_context.created_at)

                for i, version_asset in enumerate(all_versions):
                    created_at = version_asset.creation_context.created_at.strftime("%m-%d %H:%M")
                    stage = version_asset.tags.get("data_stage", "unknown") if version_asset.tags else "unknown"

                    print(f"   {i+1}. v{version_asset.version} | {created_at} | {stage}")

            return lineage_info

        except Exception as e:
            print(f"❌ Erro ao obter lineage: {e}")
            return {}

    def create_asset_from_dataframe(self, df: pd.DataFrame, asset_name: str,
                                   output_path: str = None, asset_tags: dict = None):
        """
        Cria asset a partir de DataFrame

        Args:
            df: DataFrame a ser salvo
            asset_name: Nome do asset
            output_path: Caminho para salvar (gerado se None)
            asset_tags: Tags adicionais

        Returns:
            Asset criado
        """

        print(f"💾 CRIANDO ASSET A PARTIR DE DATAFRAME")
        print("=" * 60)
        print(f"Asset: {asset_name}")
        print(f"Shape: {df.shape}")

        try:
            # Gerar path se não fornecido
            if not output_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = f"./outputs/{asset_name}_{timestamp}.csv"

                # Criar diretório se não existe
                os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Salvar DataFrame
            df.to_csv(output_path, index=False)
            print(f"💾 DataFrame salvo: {output_path}")

            # Análise básica do DataFrame
            data_analysis = {
                "rows": len(df),
                "columns": len(df.columns),
                "dtypes": df.dtypes.value_counts().to_dict(),
                "missing_values": df.isnull().sum().sum(),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2
            }

            # Tags com análise dos dados
            analysis_tags = {
                "data_analysis": json.dumps(data_analysis),
                "column_names": str(list(df.columns)),
                "has_missing_values": str(data_analysis["missing_values"] > 0).lower(),
                "created_from": "dataframe"
            }

            if asset_tags:
                analysis_tags.update(asset_tags)

            # Registrar como asset
            asset = self.register_dataset_as_asset(
                dataset_path=output_path,
                asset_name=asset_name,
                description=f"Dataset created from DataFrame - {df.shape[0]} rows, {df.shape[1]} columns",
                tags=analysis_tags
            )

            print(f"\n📊 ANÁLISE DOS DADOS:")
            print(f"   📏 Shape: {df.shape}")
            print(f"   💾 Tamanho: {data_analysis['memory_usage_mb']:.2f} MB")
            print(f"   ❓ Missing values: {data_analysis['missing_values']}")
            print(f"   📋 Tipos: {data_analysis['dtypes']}")

            return asset

        except Exception as e:
            print(f"❌ Erro ao criar asset: {e}")
            raise

    def download_asset(self, asset_name: str, version: str = "latest", 
                      download_path: str = "./downloads"):
        """
        Faz download de um asset

        Args:
            asset_name: Nome do asset
            version: Versão do asset
            download_path: Caminho para download

        Returns:
            Caminho do arquivo baixado
        """

        print(f"⬇️  FAZENDO DOWNLOAD DO ASSET: {asset_name}")
        print("=" * 60)

        try:
            # Obter asset
            if version == "latest":
                asset = self.ml_client.data.get(name=asset_name, label="latest")
            else:
                asset = self.ml_client.data.get(name=asset_name, version=version)

            print(f"📄 Asset: {asset.name} v{asset.version}")
            print(f"📁 Tipo: {asset.type}")

            # Criar diretório de download
            os.makedirs(download_path, exist_ok=True)

            # Download usando ML Client
            downloaded_path = self.ml_client.data.download(
                name=asset_name,
                version=asset.version,
                download_path=download_path
            )

            print(f"✅ Download concluído: {downloaded_path}")
            return downloaded_path

        except Exception as e:
            print(f"❌ Erro no download: {e}")
            return None

    def create_data_lineage_report(self, output_path: str = "data_lineage_report.json"):
        """
        Cria relatório completo de lineage

        Args:
            output_path: Caminho para salvar relatório
        """

        print("📊 GERANDO RELATÓRIO DE DATA LINEAGE")
        print("=" * 60)

        try:
            # Obter todos os assets
            all_assets = list(self.ml_client.data.list())

            lineage_report = {
                "report_timestamp": datetime.now().isoformat(),
                "total_assets": len(all_assets),
                "assets_by_stage": {},
                "assets_by_project": {},
                "lineage_chains": [],
                "asset_details": []
            }

            # Analisar cada asset
            for asset in all_assets:
                asset_info = {
                    "name": asset.name,
                    "version": asset.version,
                    "type": asset.type,
                    "created_at": asset.creation_context.created_at.isoformat(),
                    "created_by": getattr(asset.creation_context.created_by, "user_name", str(asset.creation_context.created_by)),
                    "tags": asset.tags or {}
                }

                lineage_report["asset_details"].append(asset_info)

                # Contabilizar por stage
                stage = asset.tags.get("data_stage", "unknown") if asset.tags else "unknown"
                if stage not in lineage_report["assets_by_stage"]:
                    lineage_report["assets_by_stage"][stage] = 0
                lineage_report["assets_by_stage"][stage] += 1

                # Contabilizar por projeto
                project = asset.tags.get("project", "unknown") if asset.tags else "unknown"
                if project not in lineage_report["assets_by_project"]:
                    lineage_report["assets_by_project"][project] = 0
                lineage_report["assets_by_project"][project] += 1

                # Identificar lineage chains
                source_asset = asset.tags.get("source_asset") if asset.tags else None
                if source_asset:
                    lineage_report["lineage_chains"].append({
                        "source": source_asset,
                        "target": asset.name,
                        "version": asset.version,
                        "pipeline": asset.tags.get("processing_pipeline", "unknown")
                    })

            # Salvar relatório
            with open(output_path, 'w') as f:
                json.dump(lineage_report, f, indent=2)

            # Mostrar resumo
            print(f"✅ Relatório gerado: {output_path}")
            print()
            print("📊 RESUMO DO LINEAGE:")
            print("-" * 30)
            print(f"Total assets: {lineage_report['total_assets']}")
            print()
            print("Por estágio:")
            for stage, count in lineage_report["assets_by_stage"].items():
                print(f"  • {stage}: {count}")
            print()
            print("Por projeto:")
            for project, count in lineage_report["assets_by_project"].items():
                print(f"  • {project}: {count}")
            print()
            print(f"Chains de lineage: {len(lineage_report['lineage_chains'])}")

            return lineage_report

        except Exception as e:
            print(f"❌ Erro ao gerar relatório: {e}")
            return {}

def demo_data_assets_management():
    """Demonstração do gerenciamento de data assets"""

    print("🎯 DEMONSTRAÇÃO: GERENCIAMENTO DE DATA ASSETS")
    print("=" * 80)

    try:
        # Conectar ao workspace
        credential = DefaultAzureCredential()
        ml_client = MLClient.from_config(credential=credential)

        print(f"✅ Conectado ao workspace: {ml_client.workspace_name}")

        # Criar gerenciador
        assets_manager = DataAssetsManager(ml_client)

        print("\n📋 1. Listando data assets existentes...")
        existing_assets = assets_manager.list_data_assets()

        print("\n📝 2. Demonstrando registro de asset...")

        # Verificar se temos dataset para registrar
        if os.path.exists("credit_default_dataset.csv"):
            print("✅ Dataset encontrado para demonstração")
            print("💡 Em demo - asset não será registrado efetivamente")

            # Simular informações do asset que seria criado
            demo_asset_info = {
                "name": "credit_default_raw",
                "version": "2024.09.18.0020",
                "type": "uri_file",
                "format": "csv",
                "tags": {
                    "project": "credit-default-mlops",
                    "data_stage": "raw",
                    "data_format": "csv",
                    "registered_by": "mlops_pipeline"
                }
            }

            print(f"📄 Asset que seria criado:")
            print(f"   Nome: {demo_asset_info['name']}")
            print(f"   Versão: {demo_asset_info['version']}")
            print(f"   Tipo: {demo_asset_info['type']}")
            print(f"   Tags: {len(demo_asset_info['tags'])} aplicadas")

        else:
            print("⚠️  Dataset não encontrado para demonstração")

        print("\n🔍 3. Demonstrando análise de lineage...")

        # Se temos assets, mostrar lineage de um deles
        if existing_assets:
            first_asset = existing_assets[0]
            print(f"📊 Analisando lineage de: {first_asset.name}")
            lineage = assets_manager.get_asset_lineage(first_asset.name)

        print("\n📊 4. Demonstrando relatório de lineage...")
        lineage_report = assets_manager.create_data_lineage_report()

        print("\n💡 FUNCIONALIDADES DISPONÍVEIS:")
        print("-" * 40)
        print("• register_dataset_as_asset() - Registrar datasets")
        print("• create_processed_data_asset() - Assets com lineage") 
        print("• list_data_assets() - Listar e filtrar assets")
        print("• get_asset_lineage() - Rastrear origem dos dados")
        print("• create_asset_from_dataframe() - De DataFrame para asset")
        print("• download_asset() - Download de assets")
        print("• create_data_lineage_report() - Relatório completo")

        return assets_manager

    except Exception as e:
        print(f"❌ Erro na demonstração: {e}")
        return None

if __name__ == "__main__":
    demo_data_assets_management()
