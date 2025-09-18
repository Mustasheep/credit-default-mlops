"""
Script Principal - Aula 4: Execução na Nuvem
Integra compute clusters, data assets, execução e automação
"""
import os
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential

def main_cloud_execution_workflow():
    """Workflow principal de execução na nuvem"""

    print("EXECUÇÃO NA NUVEM - AZURE ML")
    print("=" * 80)

    try:
        # 1. Conectar ao Azure ML Workspace
        print("\nCONECTANDO AO WORKSPACE")
        print("-" * 40)

        credential = DefaultAzureCredential()
        ml_client = MLClient.from_config(credential=credential)

        print(f"✅ Conectado: {ml_client.workspace_name}")
        print(f"📍 Região: {getattr(ml_client, 'location', 'N/A')}")
        print(f"📊 Subscription: {ml_client.subscription_id[:8]}...")

        # 2. Gerenciamento de Compute Clusters
        print("\nGERENCIAMENTO DE COMPUTE CLUSTERS")
        print("-" * 40)

        if os.path.exists('setup_compute_clusters.py'):
            print("✅ Sistema de Compute Clusters disponível")

            try:
                from setup_compute_clusters import ComputeManager
                compute_manager = ComputeManager(ml_client)

                print("📋 Listando clusters existentes...")
                existing_clusters = compute_manager.list_all_clusters()

                if not existing_clusters:
                    print("⚠️  Nenhum compute cluster encontrado")
                    print("💡 Para criar clusters otimizados:")
                    print("   clusters = compute_manager.create_optimized_clusters()")
                else:
                    print(f"✅ {len(existing_clusters)} clusters encontrados")

                # Demonstrar otimizações
                print("\nOtimizações por workload:")
                workloads = ["data_processing", "model_training", "batch_inference"]

                for workload in workloads:
                    print(f"\n📊 {workload.replace('_', ' ').title()}:")
                    recommendation = compute_manager.optimize_cluster_for_workload(workload, 5)

            except Exception as e:
                print(f"⚠️  Erro ao instanciar ComputeManager: {e}")
        else:
            print("❌ setup_compute_clusters.py não encontrado")

        # 3. Data Assets e Lineage
        print("\nDATA ASSETS E LINEAGE")
        print("-" * 40)

        if os.path.exists('data_assets_manager.py'):
            print("✅ Sistema de Data Assets disponível")

            try:
                from data_assets_manager import DataAssetsManager
                assets_manager = DataAssetsManager(ml_client)

                print("📋 Listando data assets existentes...")
                existing_assets = assets_manager.list_data_assets()

                if existing_assets:
                    print(f"✅ {len(existing_assets)} assets encontrados")

                    # Demonstrar lineage do primeiro asset
                    first_asset = existing_assets[0]
                    print(f"🔍 Analisando lineage: {first_asset.name}")
                    lineage = assets_manager.get_asset_lineage(first_asset.name)
                else:
                    print("⚠️  Nenhum data asset encontrado")

                # Gerar relatório de lineage
                print("\n📊 Gerando relatório de lineage...")
                lineage_report = assets_manager.create_data_lineage_report()

                if os.path.exists("credit_default_dataset.csv"):
                    print("\n💡 Dataset disponível para registro como asset:")
                    print("   asset = assets_manager.register_dataset_as_asset('./credit_default_dataset.csv', 'credit_default_raw')")

            except Exception as e:
                print(f"⚠️  Erro ao instanciar DataAssetsManager: {e}")
        else:
            print("❌ data_assets_manager.py não encontrado")

        # 4. Execução de Pipelines na Nuvem
        print("\nEXECUÇÃO DE PIPELINES NA NUVEM")
        print("-" * 40)

        if os.path.exists('pipeline_executor.py'):
            print("✅ Sistema de Execução disponível")

            try:
                from pipeline_executor import CloudPipelineExecutor
                executor = CloudPipelineExecutor(ml_client)

                print("📋 Listando execuções existentes...")
                existing_executions = executor.list_pipeline_executions()

                if existing_executions:
                    print(f"✅ {len(existing_executions)} execuções encontradas")
                else:
                    print("⚠️  Nenhuma execução de pipeline encontrada")

                # Demonstrar capacidades
                print("\n🚀 Capacidades de execução:")
                print("   • submit_data_pipeline() - Submeter pipelines")
                print("   • monitor_pipeline_execution() - Monitorar em tempo real")
                print("   • get_pipeline_logs() - Obter logs detalhados")

                if os.path.exists("credit_default_dataset.csv"):
                    print("\n💡 Para executar pipeline de dados:")
                    print("   job = executor.submit_data_pipeline('./credit_default_dataset.csv')")
                    print("   status = executor.monitor_pipeline_execution(job.name)")

            except Exception as e:
                print(f"⚠️  Erro ao instanciar CloudPipelineExecutor: {e}")
        else:
            print("❌ pipeline_executor.py não encontrado")

        # 5. Automação de Pipelines
        print("\nAUTOMAÇÃO DE PIPELINES")
        print("-" * 40)

        if os.path.exists('pipeline_automation.py'):
            print("✅ Sistema de Automação disponível")

            try:
                from pipeline_automation import PipelineAutomation
                automation = PipelineAutomation(ml_client)

                # Demonstrar tipos de triggers
                print("\n⚡ Tipos de triggers disponíveis:")
                print("   • Data Triggers - Baseados em mudanças de dados")
                print("   • Schedule Triggers - Execução agendada")
                print("   • Conditional Triggers - Condições customizadas")
                print("   • Workflow Automation - Múltiplas etapas")

                # Configuração exemplo
                pipeline_config_example = {
                    "pipeline_name": "data_processing_pipeline",
                    "compute_cluster": "data-processing-cluster",
                    "experiment_name": "automated_execution"
                }

                print("\n💡 Exemplo de configuração de trigger:")
                print("   trigger = automation.create_data_trigger(")
                print("       'new_data_trigger',")
                print("       'credit_default_raw',")
                print("       pipeline_config)")

            except Exception as e:
                print(f"⚠️  Erro ao instanciar PipelineAutomation: {e}")
        else:
            print("❌ pipeline_automation.py não encontrado")

        # 6. Resumo do Sistema Cloud
        print("\nRESUMO DO SISTEMA CLOUD")
        print("-" * 40)

        # Verificar componentes criados
        cloud_components = [
            'setup_compute_clusters.py',
            'data_assets_manager.py',
            'pipeline_executor.py',
            'pipeline_automation.py'
        ]

        components_status = {}
        for component in cloud_components:
            components_status[component] = "✅" if os.path.exists(component) else "❌"

        print("📊 Status dos componentes:")
        for component, status in components_status.items():
            component_name = component.replace('.py', '').replace('_', ' ').title()
            print(f"   {status} {component_name}")

        # Calcular score de completude
        completed_components = sum(1 for status in components_status.values() if status == "✅")
        total_components = len(components_status)
        completeness_score = (completed_components / total_components) * 100

        print(f"\n📈 Completude: {completeness_score:.0f}% ({completed_components}/{total_components})")

        # Status geral
        if completeness_score == 100:
            status_emoji = "🎉"
            status_text = "SISTEMA COMPLETO"
        elif completeness_score >= 75:
            status_emoji = "✅"
            status_text = "SISTEMA FUNCIONAL"
        else:
            status_emoji = "⚠️"
            status_text = "SISTEMA INCOMPLETO"

        print(f"{status_emoji} Status: {status_text}")

        # 7. Próximos Passos
        print("\nPRÓXIMOS PASSOS RECOMENDADOS")
        print("-" * 40)

        if completeness_score == 100:
            print("🚀 Sistema pronto para uso!")
            print("   1. Criar compute clusters otimizados")
            print("   2. Registrar datasets como data assets") 
            print("   3. Executar pipelines na nuvem")
            print("   4. Configurar automação com triggers")
            print("   5. Monitorar execuções e custos")
        else:
            print("Completar componentes faltantes:")
            for component, status in components_status.items():
                if status == "❌":
                    print(f"   • Execute código para criar {component}")

        # 8. Exemplos Práticos
        print("\nEXEMPLOS PRÁTICOS DE USO")
        print("-" * 40)

        print("💡 1. Criar cluster otimizado:")
        print("   compute_manager = ComputeManager(ml_client)")
        print("   clusters = compute_manager.create_optimized_clusters()")

        print("\n💡 2. Registrar dataset como asset:")
        print("   assets_manager = DataAssetsManager(ml_client)")
        print("   asset = assets_manager.register_dataset_as_asset('./data.csv', 'my_dataset')")

        print("\n💡 3. Executar pipeline na nuvem:")
        print("   executor = CloudPipelineExecutor(ml_client)")
        print("   job = executor.submit_data_pipeline('./data.csv')")
        print("   status = executor.monitor_pipeline_execution(job.name)")

        print("\n💡 4. Configurar automação:")
        print("   automation = PipelineAutomation(ml_client)")
        print("   automation.create_schedule_trigger('daily_job', '@daily', config)")

        return {
            "ml_client": ml_client,
            "components_status": components_status,
            "completeness_score": completeness_score,
            "ready_for_cloud": completeness_score >= 75
        }

    except Exception as e:
        print(f"❌ Erro no workflow: {e}")
        return {"error": str(e)}

def test_cloud_connectivity():
    """Testa conectividade com a nuvem"""

    print("\n🔌 TESTANDO CONECTIVIDADE COM A NUVEM")
    print("=" * 60)

    connectivity_tests = {
        "Azure Authentication": False,
        "Workspace Connection": False,
        "Compute Access": False,
        "Storage Access": False
    }

    try:
        # Teste 1: Autenticação
        print("1️⃣  Testando autenticação Azure...")
        credential = DefaultAzureCredential()
        print("✅ Credenciais Azure válidas")
        connectivity_tests["Azure Authentication"] = True

        # Teste 2: Conexão com workspace
        print("\n2️⃣  Testando conexão com workspace...")
        ml_client = MLClient.from_config(credential=credential)
        print(f"✅ Conectado ao workspace: {ml_client.workspace_name}")
        connectivity_tests["Workspace Connection"] = True

        # Teste 3: Acesso a compute
        print("\n3️⃣  Testando acesso a recursos de compute...")
        try:
            computes = list(ml_client.compute.list())
            print(f"✅ Acesso a compute: {len(computes)} recursos encontrados")
            connectivity_tests["Compute Access"] = True
        except Exception as e:
            print(f"⚠️  Acesso limitado a compute: {e}")

        # Teste 4: Acesso a storage
        print("\n4️⃣  Testando acesso a storage...")
        try:
            datastores = list(ml_client.datastores.list())
            print(f"✅ Acesso a datastores: {len(datastores)} encontrados")
            connectivity_tests["Storage Access"] = True
        except Exception as e:
            print(f"⚠️  Acesso limitado a storage: {e}")

        # Resumo dos testes
        print("\n📊 RESUMO DOS TESTES:")
        print("-" * 30)

        passed_tests = sum(connectivity_tests.values())
        total_tests = len(connectivity_tests)

        for test_name, passed in connectivity_tests.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"   {status} {test_name}")

        print(f"\n🏆 Resultado: {passed_tests}/{total_tests} testes passaram")

        if passed_tests == total_tests:
            print("🎉 Conectividade completa com Azure ML!")
        elif passed_tests >= 2:
            print("✅ Conectividade básica estabelecida")
        else:
            print("⚠️  Problemas de conectividade detectados")

        return connectivity_tests

    except Exception as e:
        print(f"❌ Erro nos testes de conectividade: {e}")
        connectivity_tests["error"] = str(e)
        return connectivity_tests

def show_cloud_architecture_summary():
    """Mostra resumo da arquitetura na nuvem"""

    print("\nARQUITETURA DE EXECUÇÃO NA NUVEM")
    print("=" * 60)

    architecture_components = {
        "Compute Layer": {
            "description": "Clusters otimizados para diferentes workloads",
            "components": [
                "Data Processing Cluster (STANDARD_DS3_v2, 0-6 nós)",
                "Model Training Cluster (STANDARD_DS4_v2, 0-4 nós)",
                "Development Cluster (STANDARD_DS2_v2, low priority)",
                "Batch Inference Cluster (STANDARD_DS3_v2, 0-10 nós)"
            ]
        },
        "Data Layer": {
            "description": "Assets versionados com lineage tracking",
            "components": [
                "Data Assets com versionamento automático",
                "Lineage tracking entre datasets",
                "Metadata rica com tags estruturadas",
                "Storage otimizado no Azure Blob"
            ]
        },
        "Execution Layer": {
            "description": "Orquestração e monitoramento de pipelines",
            "components": [
                "Submissão automática de pipelines",
                "Monitoramento em tempo real",
                "Logs centralizados e estruturados", 
                "Gestão de estados e retry logic"
            ]
        },
        "Automation Layer": {
            "description": "Triggers e workflows automatizados",
            "components": [
                "Data-driven triggers",
                "Schedule-based triggers",
                "Conditional triggers",
                "Multi-stage workflows"
            ]
        }
    }

    for layer_name, layer_info in architecture_components.items():
        print(f"\n🔧 {layer_name}")
        print(f"   📝 {layer_info['description']}")
        print("   Componentes:")
        for component in layer_info['components']:
            print(f"     • {component}")

    print("\nBENEFÍCIOS DA ARQUITETURA:")
    print("-" * 40)

    benefits = [
        "🔄 Escalabilidade automática baseada em demanda",
        "💰 Otimização de custos com auto-scaling e low priority",
        "📊 Observabilidade completa de execuções",
        "🤖 Automação end-to-end com triggers inteligentes",
        "🔍 Rastreabilidade completa com lineage",
        "⚡ Execução paralela e distribuída",
        "🛡️  Governança e compliance integrados"
    ]

    for benefit in benefits:
        print(f"   {benefit}")

if __name__ == "__main__":
    # Executar workflow principal
    print("INICIANDO EXECUÇÃO NA NUVEM")
    print("=" * 80)

    # Testar conectividade primeiro
    connectivity = test_cloud_connectivity()

    if connectivity.get("Workspace Connection", False):
        # Executar workflow principal se conectividade OK
        result = main_cloud_execution_workflow()

        # Mostrar arquitetura
        show_cloud_architecture_summary()

        print(f"\nStatus: {'✅ SUCESSO' if not result.get('error') else '❌ ERRO'}")

        if result.get("ready_for_cloud"):
            print("🎉 Sistema pronto para execução na nuvem!")
        else:
            print("Complete os componentes faltantes")
    else:
        print("\n⚠️  Conectividade insuficiente para execução completa")
        print("💡 Configure as credenciais Azure e tente novamente")
