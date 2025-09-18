"""
Sistema de Configuração de Compute Clusters - Azure ML
Gerencia criação, configuração e otimização de recursos computacionais
"""
from azure.ai.ml import MLClient
from azure.ai.ml.entities import AmlCompute
from azure.identity import DefaultAzureCredential
from datetime import datetime
import json
import os

class ComputeManager:
    """Gerenciador de Compute Clusters para Azure ML"""

    def __init__(self, ml_client: MLClient):
        self.ml_client = ml_client
        self.cluster_configs = {}

    def create_compute_cluster(self, cluster_name: str, vm_size: str = "STANDARD_DS3_v2",
                              min_instances: int = 0, max_instances: int = 4,
                              location: str = None, idle_time: int = 120,
                              tier: str = "dedicated", description: str = ""):
        """
        Cria compute cluster otimizado

        Args:
            cluster_name: Nome do cluster
            vm_size: Tamanho da VM
            min_instances: Número mínimo de nós
            max_instances: Número máximo de nós
            location: Região do Azure
            idle_time: Tempo antes de scale down
            tier: dedicated ou low_priority
            description: Descrição do cluster

        Returns:
            Cluster criado
        """

        print(f"CRIANDO COMPUTE CLUSTER: {cluster_name}")
        print("=" * 60)
        print(f"VM Size: {vm_size}")
        print(f"Scaling: {min_instances}-{max_instances} nós")
        print(f"Tier: {tier}")
        print(f"Idle Time: {idle_time}s")

        try:
            # Verificar se cluster já existe
            existing_cluster = None
            try:
                existing_cluster = self.ml_client.compute.get(cluster_name)
                if existing_cluster:
                    print(f"⚠️  Cluster {cluster_name} já existe")
                    print(f"   Estado atual: {existing_cluster.provisioning_state}")
                    return existing_cluster
            except:
                pass

            # Configurar cluster
            cluster_config = AmlCompute(
                name=cluster_name,
                type="amlcompute",
                size=vm_size,
                location=location,
                min_instances=min_instances,
                max_instances=max_instances,
                idle_time_before_scale_down=idle_time,
                tier=tier,
                description=description or f"Compute cluster for {cluster_name} workloads"
            )

            # Criar cluster
            print("Iniciando criação do cluster...")
            cluster = self.ml_client.begin_create_or_update(cluster_config).result()

            # Armazenar configuração
            self.cluster_configs[cluster_name] = {
                "name": cluster_name,
                "vm_size": vm_size,
                "min_instances": min_instances,
                "max_instances": max_instances,
                "tier": tier,
                "location": location or "workspace_region",
                "idle_time": idle_time,
                "created_at": datetime.now().isoformat(),
                "description": description
            }

            print("\n✅ CLUSTER CRIADO COM SUCESSO!")
            print("=" * 40)
            print(f"🆔 Nome: {cluster.name}")
            print(f"💻 VM Size: {cluster.size}")
            print(f"📍 Location: {cluster.location}")
            print(f"⚡ Estado: {cluster.provisioning_state}")

            return cluster

        except Exception as e:
            print(f"❌ Erro ao criar cluster: {e}")
            raise

    def create_optimized_clusters(self):
        """
        Cria conjunto otimizado de clusters para diferentes workloads

        Returns:
            dict: Clusters criados
        """

        print("CRIANDO CONJUNTO OTIMIZADO DE CLUSTERS")
        print("=" * 70)

        # Configurações otimizadas por tipo de workload
        cluster_configs = {
            "data-processing-cluster": {
                "vm_size": "STANDARD_DS3_v2",  # 4 cores, 14GB RAM
                "min_instances": 0,
                "max_instances": 6,
                "tier": "dedicated",
                "idle_time": 300,  # 5 minutos
                "description": "Cluster otimizado para processamento de dados"
            },
            "model-training-cluster": {
                "vm_size": "STANDARD_DS4_v2",  # 8 cores, 28GB RAM
                "min_instances": 0,
                "max_instances": 4,
                "tier": "dedicated", 
                "idle_time": 600,  # 10 minutos
                "description": "Cluster otimizado para treinamento de modelos"
            },
            "development-cluster": {
                "vm_size": "STANDARD_DS2_v2",  # 2 cores, 7GB RAM
                "min_instances": 0,
                "max_instances": 2,
                "tier": "low_priority",
                "idle_time": 180,  # 3 minutos
                "description": "Cluster econômico para desenvolvimento e testes"
            },
            "batch-inference-cluster": {
                "vm_size": "STANDARD_DS3_v2",  # 4 cores, 14GB RAM
                "min_instances": 0,
                "max_instances": 10,
                "tier": "low_priority",
                "idle_time": 120,  # 2 minutos
                "description": "Cluster para inferência em lote"
            }
        }

        created_clusters = {}

        for cluster_name, config in cluster_configs.items():
            print(f"\n⏳ Criando {cluster_name}...")

            try:
                cluster = self.create_compute_cluster(
                    cluster_name=cluster_name,
                    **config
                )
                created_clusters[cluster_name] = cluster

            except Exception as e:
                print(f"⚠️  Erro ao criar {cluster_name}: {e}")
                continue

        # Resumo
        print("\n📊 RESUMO DOS CLUSTERS CRIADOS")
        print("=" * 50)

        for cluster_name, cluster in created_clusters.items():
            config = self.cluster_configs.get(cluster_name, {})
            print(f"\n✅ {cluster_name}")
            print(f"   💻 VM: {config.get('vm_size', 'N/A')}")
            print(f"   📏 Scale: {config.get('min_instances', 0)}-{config.get('max_instances', 0)} nós")
            print(f"   💰 Tier: {config.get('tier', 'N/A')}")
            print(f"   🎯 Uso: {config.get('description', 'N/A')}")

        return created_clusters

    def get_cluster_info(self, cluster_name: str):
        """
        Obtém informações detalhadas do cluster

        Args:
            cluster_name: Nome do cluster

        Returns:
            dict: Informações do cluster
        """

        print(f"\nINFORMAÇÕES DO CLUSTER: {cluster_name}")
        print("=" * 50)

        try:
            cluster = self.ml_client.compute.get(cluster_name)

            cluster_info = {
                "name": cluster.name,
                "type": cluster.type,
                "size": cluster.size,
                "location": cluster.location,
                "provisioning_state": cluster.provisioning_state,
                "min_instances": cluster.min_instances,
                "max_instances": cluster.max_instances,
                "current_instances": getattr(cluster, 'current_instances', 0),
                "idle_time_before_scale_down": cluster.idle_time_before_scale_down,
                "tier": getattr(cluster, 'tier', 'dedicated'),
                "created_time": cluster.creation_context.created_at.isoformat() if cluster.creation_context else None,
                "created_by": cluster.creation_context.created_by.user_name if cluster.creation_context else None
            }

            # Mostrar informações
            print(f"✅ Estado: {cluster_info['provisioning_state']}")
            print(f"💻 VM Size: {cluster_info['size']}")
            print(f"📍 Location: {cluster_info['location']}")
            print(f"📏 Scaling: {cluster_info['min_instances']}-{cluster_info['max_instances']} nós")
            print(f"🔄 Nós atuais: {cluster_info['current_instances']}")
            print(f"⏰ Idle time: {cluster_info['idle_time_before_scale_down']}s")
            print(f"💰 Tier: {cluster_info['tier']}")

            if cluster_info['created_time']:
                print(f"📅 Criado em: {cluster_info['created_time'][:19]}")
            if cluster_info['created_by']:
                print(f"👤 Criado por: {cluster_info['created_by']}")

            return cluster_info

        except Exception as e:
            print(f"❌ Erro ao obter informações: {e}")
            return {}

    def list_all_clusters(self):
        """
        Lista todos os clusters do workspace

        Returns:
            list: Lista de clusters
        """

        print("\nTODOS OS CLUSTERS DO WORKSPACE")
        print("=" * 50)

        try:
            clusters = list(self.ml_client.compute.list())

            # Filtrar apenas compute clusters
            compute_clusters = [c for c in clusters if c.type == "amlcompute"]

            if not compute_clusters:
                print("⚠️  Nenhum compute cluster encontrado")
                return []

            print(f"Total de clusters: {len(compute_clusters)}")
            print()

            for i, cluster in enumerate(compute_clusters, 1):
                state_emoji = {
                    "Succeeded": "✅",
                    "Creating": "🔄", 
                    "Failed": "❌",
                    "Deleting": "🗑️"
                }.get(cluster.provisioning_state, "❓")

                tier_info = getattr(cluster, 'tier', 'dedicated')
                cost_indicator = "💰" if tier_info == "dedicated" else "🏷️"

                print(f"{i:2d}. {state_emoji} {cluster.name}")
                print(f"    💻 {cluster.size} | 📏 {cluster.min_instances}-{cluster.max_instances} nós")
                print(f"    📍 {cluster.location} | {cost_indicator} {tier_info}")
                print(f"    🔄 Nós atuais: {getattr(cluster, 'current_instances', 0)}")
                print()

            return compute_clusters

        except Exception as e:
            print(f"❌ Erro ao listar clusters: {e}")
            return []

    def delete_cluster(self, cluster_name: str):
        """
        Deleta compute cluster

        Args:
            cluster_name: Nome do cluster

        Returns:
            bool: Success status
        """

        print(f"🗑️  DELETANDO CLUSTER: {cluster_name}")
        print("=" * 50)

        try:
            # Confirmar existência
            cluster = self.ml_client.compute.get(cluster_name)

            print(f"📋 Cluster encontrado: {cluster.name}")
            print(f"💻 VM Size: {cluster.size}")
            print(f"⚡ Estado: {cluster.provisioning_state}")

            # Deletar
            print("🔄 Iniciando deleção...")
            self.ml_client.compute.begin_delete(cluster_name).wait()

            # Remover da configuração local
            if cluster_name in self.cluster_configs:
                del self.cluster_configs[cluster_name]

            print("✅ Cluster deletado com sucesso!")
            return True

        except Exception as e:
            print(f"❌ Erro ao deletar cluster: {e}")
            return False

    def estimate_costs(self, cluster_name: str, hours_per_month: int = 100):
        """
        Estima custos mensais do cluster

        Args:
            cluster_name: Nome do cluster
            hours_per_month: Horas de uso estimadas por mês

        Returns:
            dict: Estimativa de custos
        """

        print(f"💰 ESTIMATIVA DE CUSTOS: {cluster_name}")
        print("=" * 50)

        # Tabela simplificada de custos por hora
        vm_costs_hourly = {
            "STANDARD_DS2_v2": 0.096,   # 2 cores, 7GB
            "STANDARD_DS3_v2": 0.192,   # 4 cores, 14GB
            "STANDARD_DS4_v2": 0.384,   # 8 cores, 28GB
            "STANDARD_DS5_v2": 0.768,   # 16 cores, 56GB
        }

        try:
            cluster_config = self.cluster_configs.get(cluster_name)
            if not cluster_config:
                # Tentar obter do Azure
                cluster = self.ml_client.compute.get(cluster_name)
                vm_size = cluster.size
                max_instances = cluster.max_instances
                tier = getattr(cluster, 'tier', 'dedicated')
            else:
                vm_size = cluster_config['vm_size']
                max_instances = cluster_config['max_instances']
                tier = cluster_config['tier']

            # Custo base por hora
            base_cost_per_hour = vm_costs_hourly.get(vm_size, 0.20)

            # Desconto para low priority
            if tier == "low_priority":
                base_cost_per_hour *= 0.3  # ~70% de desconto

            # Cálculos
            cost_single_node_hourly = base_cost_per_hour
            cost_max_cluster_hourly = base_cost_per_hour * max_instances

            cost_single_node_monthly = cost_single_node_hourly * hours_per_month
            cost_max_cluster_monthly = cost_max_cluster_hourly * hours_per_month

            # Estimativa realística (assumindo 30% de utilização média)
            realistic_monthly = cost_max_cluster_monthly * 0.3

            cost_estimate = {
                "vm_size": vm_size,
                "tier": tier,
                "max_instances": max_instances,
                "hours_per_month": hours_per_month,
                "cost_per_node_hourly": cost_single_node_hourly,
                "cost_max_cluster_hourly": cost_max_cluster_hourly,
                "cost_single_node_monthly": cost_single_node_monthly,
                "cost_max_cluster_monthly": cost_max_cluster_monthly,
                "realistic_monthly_estimate": realistic_monthly,
                "currency": "USD"
            }

            # Mostrar estimativa
            print(f"💻 VM Size: {vm_size}")
            print(f"🏷️  Tier: {tier}")
            print(f"📏 Max Instances: {max_instances}")
            print(f"⏰ Horas/mês: {hours_per_month}")
            print()
            print(f"💲 Custo por nó/hora: ${cost_single_node_hourly:.3f}")
            print(f"💹 Custo cluster max/hora: ${cost_max_cluster_hourly:.2f}")
            print()
            print(f"💲 Custo 1 nó/mês: ${cost_single_node_monthly:.2f}")
            print(f"💹 Custo cluster max/mês: ${cost_max_cluster_monthly:.2f}")
            print(f"📊 Estimativa realística/mês: ${realistic_monthly:.2f}")

            if tier == "low_priority":
                dedicated_cost = cost_max_cluster_monthly / 0.3
                savings = dedicated_cost - cost_max_cluster_monthly
                print(f"💚 Economia vs dedicated: ${savings:.2f}/mês ({savings/dedicated_cost*100:.0f}%)")

            return cost_estimate

        except Exception as e:
            print(f"❌ Erro ao estimar custos: {e}")
            return {}

    def optimize_cluster_for_workload(self, workload_type: str, expected_jobs_per_day: int = 10):
        """
        Recomenda configuração otimizada para tipo de workload

        Args:
            workload_type: Tipo de workload (data_processing, model_training, batch_inference)
            expected_jobs_per_day: Jobs esperados por dia

        Returns:
            dict: Recomendações de configuração
        """

        print(f"OTIMIZAÇÃO PARA WORKLOAD: {workload_type}")
        print("=" * 60)
        print(f"Jobs esperados/dia: {expected_jobs_per_day}")

        # Recomendações baseadas em workload
        optimizations = {
            "data_processing": {
                "recommended_vm_size": "STANDARD_DS3_v2",  # 4 cores bom para I/O
                "min_instances": 0,
                "max_instances": min(6, max(2, expected_jobs_per_day // 5)),
                "idle_time": 300,  # 5 min
                "tier": "dedicated",
                "reasoning": "Processamento de dados precisa de I/O confiável e paralelização"
            },
            "model_training": {
                "recommended_vm_size": "STANDARD_DS4_v2",  # 8 cores para ML
                "min_instances": 0,
                "max_instances": min(4, max(1, expected_jobs_per_day // 8)),
                "idle_time": 600,  # 10 min
                "tier": "dedicated",
                "reasoning": "Treinamento precisa de CPU/RAM e não pode ser interrompido"
            },
            "batch_inference": {
                "recommended_vm_size": "STANDARD_DS3_v2",  # 4 cores suficiente
                "min_instances": 0,
                "max_instances": min(10, max(2, expected_jobs_per_day // 3)),
                "idle_time": 120,  # 2 min - inference rápida
                "tier": "low_priority",
                "reasoning": "Inferência batch pode usar low priority para economia"
            },
            "development": {
                "recommended_vm_size": "STANDARD_DS2_v2",  # 2 cores suficiente
                "min_instances": 0,
                "max_instances": 2,
                "idle_time": 180,  # 3 min - desenvolvimento ágil
                "tier": "low_priority",  # Máxima economia
                "reasoning": "Desenvolvimento não precisa de alta performance"
            }
        }

        if workload_type not in optimizations:
            print(f"⚠️  Workload type '{workload_type}' não reconhecido")
            print(f"Tipos disponíveis: {list(optimizations.keys())}")
            return {}

        recommendation = optimizations[workload_type]

        # Mostrar recomendações
        print("\n📋 RECOMENDAÇÕES:")
        print("-" * 30)
        print(f"💻 VM Size: {recommendation['recommended_vm_size']}")
        print(f"📏 Scaling: {recommendation['min_instances']}-{recommendation['max_instances']} nós")
        print(f"⏰ Idle time: {recommendation['idle_time']}s")
        print(f"🏷️  Tier: {recommendation['tier']}")
        print()
        print(f"💡 Raciocínio: {recommendation['reasoning']}")

        # Estimativa de custo
        print("\n💰 Estimativa de Custo:")
        print("-" * 30)

        vm_costs = {
            "STANDARD_DS2_v2": 0.096,
            "STANDARD_DS3_v2": 0.192, 
            "STANDARD_DS4_v2": 0.384
        }

        hourly_cost = vm_costs.get(recommendation['recommended_vm_size'], 0.20)
        if recommendation['tier'] == 'low_priority':
            hourly_cost *= 0.3

        max_hourly = hourly_cost * recommendation['max_instances']
        monthly_estimate = max_hourly * 100 * 0.3  # 100h/mês, 30% utilização

        print(f"💲 Máximo por hora: ${max_hourly:.2f}")
        print(f"💹 Estimativa mensal: ${monthly_estimate:.2f}")

        return recommendation

    def save_cluster_configurations(self, output_path: str = "cluster_configurations.json"):
        """Salva configurações dos clusters"""

        with open(output_path, 'w') as f:
            json.dump(self.cluster_configs, f, indent=2)

        print(f"💾 Configurações salvas: {output_path}")

def demo_compute_management():
    """Demonstração do gerenciamento de compute"""

    print("DEMONSTRAÇÃO: GERENCIAMENTO DE COMPUTE CLUSTERS")
    print("=" * 80)

    try:
        # Conectar ao workspace
        credential = DefaultAzureCredential()
        ml_client = MLClient.from_config(credential=credential)

        print(f"✅ Conectado ao workspace: {ml_client.workspace_name}")

        # Criar gerenciador
        compute_manager = ComputeManager(ml_client)

        print("\n1. Listando clusters existentes...")
        existing_clusters = compute_manager.list_all_clusters()

        print("\n2. Demonstrando criação de clusters otimizados...")
        print("💡 Em demonstração - clusters não serão criados efetivamente")

        # Simular criação sem efetivamente criar
        print("\n3. Clusters que seriam criados:")
        recommended_clusters = [
            {
                "name": "data-processing-cluster",
                "vm_size": "STANDARD_DS3_v2",
                "scaling": "0-6 nós",
                "tier": "dedicated",
                "uso": "Processamento de dados"
            },
            {
                "name": "model-training-cluster", 
                "vm_size": "STANDARD_DS4_v2",
                "scaling": "0-4 nós",
                "tier": "dedicated",
                "uso": "Treinamento de modelos"
            },
            {
                "name": "development-cluster",
                "vm_size": "STANDARD_DS2_v2", 
                "scaling": "0-2 nós",
                "tier": "low_priority",
                "uso": "Desenvolvimento (economia 70%)"
            }
        ]

        for cluster in recommended_clusters:
            print(f"\n✅ {cluster['name']}")
            print(f"   💻 {cluster['vm_size']} | 📏 {cluster['scaling']}")
            print(f"   🏷️  {cluster['tier']} | 🎯 {cluster['uso']}")

        print("\n4. Demonstrando estimativas de custo...")
        for cluster in recommended_clusters:
            vm_costs = {"STANDARD_DS2_v2": 0.096, "STANDARD_DS3_v2": 0.192, "STANDARD_DS4_v2": 0.384}
            base_cost = vm_costs.get(cluster["vm_size"], 0.20)
            max_nodes = int(cluster["scaling"].split("-")[1].split()[0])

            if cluster["tier"] == "low_priority":
                base_cost *= 0.3

            monthly_est = base_cost * max_nodes * 100 * 0.3  # 100h/mês, 30% utilização

            print(f"   💵 {cluster['name']}: ~${monthly_est:.0f}/mês")

        print("\n5. Demonstrando otimizações por workload...")
        workloads = ["data_processing", "model_training", "batch_inference"]

        for workload in workloads:
            print(f"\n📊 Otimização para {workload}:")
            recommendation = compute_manager.optimize_cluster_for_workload(workload, 10)

        return compute_manager

    except Exception as e:
        print(f"❌ Erro na demonstração: {e}")
        return None

# if __name__ == "__main__":
#     demo_compute_management()

if __name__ == "__main__":
    try:
        # Conectar ao workspace
        credential = DefaultAzureCredential()
        ml_client = MLClient.from_config(credential=credential)

        print(f"✅ Conectado ao workspace: {ml_client.workspace_name}")

        # Criar gerenciador
        compute_manager = ComputeManager(ml_client)

        # Listar clusters existentes
        compute_manager.list_all_clusters()

        # Criar clusters otimizados de verdade
        created_clusters = compute_manager.create_optimized_clusters()

        # Estimar custos
        for cluster_name in created_clusters.keys():
            compute_manager.estimate_costs(cluster_name)

        # Salvar configuração local
        compute_manager.save_cluster_configurations()

    except Exception as e:
        print(f"❌ Erro ao gerenciar clusters: {e}")