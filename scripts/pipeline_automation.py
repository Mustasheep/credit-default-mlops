"""
Sistema de Automação de Pipelines - Azure ML
Triggers automáticos, scheduling e workflows condicionais
"""
from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta
import json
import os
import time
from typing import List, Dict

class PipelineAutomation:
    """Sistema de automação de pipelines com triggers inteligentes"""

    def __init__(self, ml_client: MLClient):
        self.ml_client = ml_client
        self.automation_rules = {}
        self.scheduled_jobs = {}
        self.trigger_history = []

    def create_data_trigger(self, trigger_name: str, data_asset_name: str,
                           pipeline_config: dict, conditions: dict = None):
        """
        Cria trigger baseado em mudanças de dados

        Args:
            trigger_name: Nome do trigger
            data_asset_name: Asset a monitorar
            pipeline_config: Configuração do pipeline a executar
            conditions: Condições para disparo

        Returns:
            Trigger configurado
        """

        print(f"⚡ CRIANDO DATA TRIGGER: {trigger_name}")
        print("=" * 60)
        print(f"Monitorando asset: {data_asset_name}")

        # Condições padrão
        default_conditions = {
            "new_version": True,  # Disparar em nova versão
            "size_change_threshold": 0.1,  # 10% de mudança no tamanho
            "check_interval_hours": 6,  # Verificar a cada 6 horas
            "enabled": True
        }

        if conditions:
            default_conditions.update(conditions)

        trigger_config = {
            "trigger_name": trigger_name,
            "trigger_type": "data_change",
            "data_asset_name": data_asset_name,
            "pipeline_config": pipeline_config,
            "conditions": default_conditions,
            "created_at": datetime.now().isoformat(),
            "last_checked": None,
            "last_triggered": None,
            "trigger_count": 0
        }

        # Armazenar configuração
        self.automation_rules[trigger_name] = trigger_config

        print("✅ Data Trigger configurado:")
        print(f"   📊 Asset: {data_asset_name}")
        print(f"   ⏰ Check interval: {default_conditions['check_interval_hours']}h")
        print(f"   📏 Size threshold: {default_conditions['size_change_threshold']*100}%")
        print(f"   🔔 New version trigger: {default_conditions['new_version']}")

        return trigger_config

    def create_schedule_trigger(self, trigger_name: str, cron_expression: str,
                               pipeline_config: dict, enabled: bool = True):
        """
        Cria trigger baseado em agenda (cron-like)

        Args:
            trigger_name: Nome do trigger
            cron_expression: Expressão cron simplificada
            pipeline_config: Configuração do pipeline
            enabled: Se o trigger está ativo

        Returns:
            Trigger configurado
        """

        print(f"📅 CRIANDO SCHEDULE TRIGGER: {trigger_name}")
        print("=" * 60)
        print(f"Schedule: {cron_expression}")

        # Interpretar expressão cron simplificada
        schedule_info = self._parse_cron_expression(cron_expression)

        trigger_config = {
            "trigger_name": trigger_name,
            "trigger_type": "scheduled",
            "cron_expression": cron_expression,
            "schedule_info": schedule_info,
            "pipeline_config": pipeline_config,
            "enabled": enabled,
            "created_at": datetime.now().isoformat(),
            "last_triggered": None,
            "next_run": self._calculate_next_run(schedule_info),
            "trigger_count": 0
        }

        self.automation_rules[trigger_name] = trigger_config

        print("✅ Schedule Trigger configurado:")
        print(f"   📅 Expressão: {cron_expression}")
        print(f"   ⏰ Próxima execução: {trigger_config['next_run']}")
        print(f"   🔔 Ativo: {enabled}")

        return trigger_config

    def create_conditional_trigger(self, trigger_name: str, condition_check: str,
                                 pipeline_config: dict, check_interval_minutes: int = 30):
        """
        Cria trigger baseado em condições customizadas

        Args:
            trigger_name: Nome do trigger
            condition_check: Condição a verificar
            pipeline_config: Configuração do pipeline
            check_interval_minutes: Intervalo de verificação

        Returns:
            Trigger configurado
        """

        print(f"🔍 CRIANDO CONDITIONAL TRIGGER: {trigger_name}")
        print("=" * 60)
        print(f"Condição: {condition_check}")

        trigger_config = {
            "trigger_name": trigger_name,
            "trigger_type": "conditional", 
            "condition_check": condition_check,
            "pipeline_config": pipeline_config,
            "check_interval_minutes": check_interval_minutes,
            "created_at": datetime.now().isoformat(),
            "last_checked": None,
            "last_triggered": None,
            "trigger_count": 0,
            "enabled": True
        }

        self.automation_rules[trigger_name] = trigger_config

        print("✅ Conditional Trigger configurado:")
        print(f"   🔍 Condição: {condition_check}")
        print(f"   ⏰ Check interval: {check_interval_minutes} min")

        return trigger_config

    def _parse_cron_expression(self, cron_expr: str):
        """Interpreta expressão cron simplificada"""

        # Mapeamento de expressões comuns
        cron_patterns = {
            "@daily": {"hours": [0], "minutes": [0], "description": "Diariamente às 00:00"},
            "@hourly": {"minutes": [0], "description": "A cada hora"},
            "@weekly": {"weekday": [0], "hours": [0], "minutes": [0], "description": "Semanalmente aos domingos 00:00"},
            "0 9 * * MON-FRI": {"hours": [9], "minutes": [0], "weekdays": [0,1,2,3,4], "description": "Dias úteis às 09:00"},
            "0 */6 * * *": {"hours": [0,6,12,18], "minutes": [0], "description": "A cada 6 horas"},
            "0 12 * * *": {"hours": [12], "minutes": [0], "description": "Diariamente às 12:00"}
        }

        if cron_expr in cron_patterns:
            return cron_patterns[cron_expr]

        # Expressão customizada básica
        return {
            "expression": cron_expr,
            "description": f"Schedule customizado: {cron_expr}"
        }

    def _calculate_next_run(self, schedule_info: dict):
        """Calcula próxima execução baseada no schedule"""

        now = datetime.now()

        # Para @daily
        if "hours" in schedule_info and "minutes" in schedule_info:
            next_run = now.replace(
                hour=schedule_info["hours"][0],
                minute=schedule_info["minutes"][0],
                second=0,
                microsecond=0
            )

            # Se já passou hoje, agendar para amanhã
            if next_run <= now:
                next_run += timedelta(days=1)

            return next_run.isoformat()

        # Default: próxima hora
        next_run = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        return next_run.isoformat()

    def check_triggers(self, execute_if_triggered: bool = False):
        """
        Verifica todos os triggers e executa se necessário

        Args:
            execute_if_triggered: Se deve executar pipelines quando triggered

        Returns:
            Lista de triggers que foram disparados
        """

        print("🔍 VERIFICANDO TRIGGERS DE AUTOMAÇÃO")
        print("=" * 60)

        triggered_rules = []
        current_time = datetime.now()

        for trigger_name, trigger_config in self.automation_rules.items():
            if not trigger_config.get("enabled", True):
                continue

            print(f"\n🔍 Verificando: {trigger_name}")
            print(f"   Tipo: {trigger_config['trigger_type']}")

            should_trigger = False
            trigger_reason = ""

            # Verificar trigger baseado no tipo
            if trigger_config["trigger_type"] == "data_change":
                should_trigger, trigger_reason = self._check_data_trigger(trigger_config)

            elif trigger_config["trigger_type"] == "scheduled":
                should_trigger, trigger_reason = self._check_schedule_trigger(trigger_config, current_time)

            elif trigger_config["trigger_type"] == "conditional":
                should_trigger, trigger_reason = self._check_conditional_trigger(trigger_config)

            # Atualizar último check
            trigger_config["last_checked"] = current_time.isoformat()

            if should_trigger:
                print(f"   ⚡ TRIGGERED: {trigger_reason}")

                # Registrar trigger
                trigger_record = {
                    "trigger_name": trigger_name,
                    "trigger_type": trigger_config["trigger_type"],
                    "triggered_at": current_time.isoformat(),
                    "reason": trigger_reason,
                    "pipeline_config": trigger_config["pipeline_config"]
                }

                triggered_rules.append(trigger_record)
                self.trigger_history.append(trigger_record)

                # Atualizar contadores
                trigger_config["last_triggered"] = current_time.isoformat()
                trigger_config["trigger_count"] = trigger_config.get("trigger_count", 0) + 1

                # Executar pipeline se solicitado
                if execute_if_triggered:
                    self._execute_triggered_pipeline(trigger_record)

            else:
                print(f"   ✅ OK: {trigger_reason}")

        print(f"\n📊 RESUMO: {len(triggered_rules)} triggers disparados")

        return triggered_rules

    def _check_data_trigger(self, trigger_config: dict):
        """Verifica trigger de mudança de dados"""

        data_asset_name = trigger_config["data_asset_name"]
        conditions = trigger_config["conditions"]

        try:
            # Obter versões do asset
            asset_versions = list(self.ml_client.data.list(name=data_asset_name))

            if not asset_versions:
                return False, "Asset não encontrado"

            # Ordenar por data de criação
            asset_versions.sort(key=lambda x: x.creation_context.created_at, reverse=True)
            latest_version = asset_versions[0]

            # Verificar se houve nova versão desde último check
            last_triggered = trigger_config.get("last_triggered")

            if conditions.get("new_version", True):
                if not last_triggered:
                    return True, f"Primeira verificação - versão {latest_version.version}"

                # Comparar com última execução
                last_triggered_time = datetime.fromisoformat(last_triggered.replace('Z', '+00:00').replace('+00:00', ''))
                asset_created_time = latest_version.creation_context.created_at.replace(tzinfo=None)

                if asset_created_time > last_triggered_time:
                    return True, f"Nova versão detectada: {latest_version.version}"

            return False, "Nenhuma mudança detectada"

        except Exception as e:
            return False, f"Erro ao verificar asset: {e}"

    def _check_schedule_trigger(self, trigger_config: dict, current_time: datetime):
        """Verifica trigger agendado"""

        next_run_str = trigger_config.get("next_run")
        if not next_run_str:
            return False, "Próxima execução não definida"

        try:
            next_run = datetime.fromisoformat(next_run_str)

            if current_time >= next_run:
                # Calcular próxima execução
                schedule_info = trigger_config["schedule_info"]
                new_next_run = self._calculate_next_run(schedule_info)
                trigger_config["next_run"] = new_next_run

                return True, f"Horário agendado atingido: {next_run.strftime('%H:%M')}"

            return False, f"Próxima execução: {next_run.strftime('%d/%m %H:%M')}"

        except Exception as e:
            return False, f"Erro no schedule: {e}"

    def _check_conditional_trigger(self, trigger_config: dict):
        """Verifica trigger condicional"""

        condition = trigger_config["condition_check"]

        # Implementação simplificada - em produção, avaliar condições reais
        # Por exemplo: verificar status de jobs, métricas, arquivos, etc.

        if "file_exists" in condition:
            # Exemplo: "file_exists:./new_data.csv"
            file_path = condition.split(":")[1]
            exists = os.path.exists(file_path)

            if exists:
                return True, f"Arquivo encontrado: {file_path}"
            else:
                return False, f"Arquivo não existe: {file_path}"

        elif "job_completed" in condition:
            # Exemplo: "job_completed:experiment_name"
            return False, "Verificação de job não implementada em demo"

        else:
            # Condição genérica - sempre falso para demo
            return False, f"Condição não atendida: {condition}"

    def _execute_triggered_pipeline(self, trigger_record: dict):
        """Executa pipeline que foi disparado por trigger"""

        print(f"\n🚀 EXECUTANDO PIPELINE TRIGGERED")
        print("-" * 50)
        print(f"Trigger: {trigger_record['trigger_name']}")
        print(f"Razão: {trigger_record['reason']}")

        pipeline_config = trigger_record["pipeline_config"]

        try:
            # Em implementação real, submeter pipeline usando configuração
            # Por enquanto, simular execução

            print("🎭 Simulando execução de pipeline...")
            print(f"   Pipeline: {pipeline_config.get('pipeline_name', 'unknown')}")
            print(f"   Compute: {pipeline_config.get('compute_cluster', 'default')}")
            print(f"   Experimento: {pipeline_config.get('experiment_name', 'automated')}")

            # Simular job ID
            job_id = f"triggered_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            print(f"   Job ID: {job_id}")

            return job_id

        except Exception as e:
            print(f"❌ Erro ao executar pipeline: {e}")
            return None

    def create_workflow_automation(self, workflow_name: str, stages: List[Dict]):
        """
        Cria workflow de automação com múltiplas etapas

        Args:
            workflow_name: Nome do workflow
            stages: Lista de etapas do workflow

        Returns:
            Workflow configurado
        """

        print(f"🔄 CRIANDO WORKFLOW DE AUTOMAÇÃO: {workflow_name}")
        print("=" * 60)

        workflow_config = {
            "workflow_name": workflow_name,
            "stages": stages,
            "created_at": datetime.now().isoformat(),
            "last_executed": None,
            "execution_count": 0,
            "enabled": True
        }

        # Validar estágios
        for i, stage in enumerate(stages):
            print(f"📋 Estágio {i+1}: {stage.get('name', 'Sem nome')}")
            print(f"   Tipo: {stage.get('type', 'unknown')}")

            if stage.get('depends_on'):
                print(f"   Depende de: {stage['depends_on']}")

        # Armazenar workflow
        workflow_key = f"workflow_{workflow_name}"
        self.automation_rules[workflow_key] = workflow_config

        print(f"\n✅ Workflow criado com {len(stages)} estágios")

        return workflow_config

    def list_automation_rules(self):
        """Lista todas as regras de automação"""

        print("📋 REGRAS DE AUTOMAÇÃO CONFIGURADAS")
        print("=" * 60)

        if not self.automation_rules:
            print("⚠️  Nenhuma regra configurada")
            return

        for rule_name, rule_config in self.automation_rules.items():
            rule_type = rule_config.get("trigger_type", rule_config.get("workflow_name", "unknown"))
            enabled = rule_config.get("enabled", True)
            status_emoji = "✅" if enabled else "⏹️"

            print(f"\n{status_emoji} {rule_name}")
            print(f"   Tipo: {rule_type}")
            print(f"   Criado: {rule_config.get('created_at', 'N/A')[:19]}")

            # Informações específicas por tipo
            if rule_type == "data_change":
                print(f"   Asset: {rule_config.get('data_asset_name', 'N/A')}")
                print(f"   Triggers: {rule_config.get('trigger_count', 0)}")

            elif rule_type == "scheduled":
                print(f"   Schedule: {rule_config.get('cron_expression', 'N/A')}")
                print(f"   Próxima: {rule_config.get('next_run', 'N/A')[:19]}")

            elif rule_type == "conditional":
                print(f"   Condição: {rule_config.get('condition_check', 'N/A')}")

            if rule_config.get("last_triggered"):
                print(f"   Último trigger: {rule_config['last_triggered'][:19]}")

    def get_trigger_statistics(self):
        """Gera estatísticas dos triggers"""

        print("📊 ESTATÍSTICAS DE TRIGGERS")
        print("=" * 50)

        if not self.trigger_history:
            print("⚠️  Nenhum histórico de triggers")
            return {}

        # Análise do histórico
        stats = {
            "total_triggers": len(self.trigger_history),
            "triggers_by_type": {},
            "triggers_by_name": {},
            "most_active_trigger": None,
            "recent_triggers": []
        }

        # Contadores
        for trigger in self.trigger_history:
            trigger_type = trigger["trigger_type"]
            trigger_name = trigger["trigger_name"]

            stats["triggers_by_type"][trigger_type] = stats["triggers_by_type"].get(trigger_type, 0) + 1
            stats["triggers_by_name"][trigger_name] = stats["triggers_by_name"].get(trigger_name, 0) + 1

        # Trigger mais ativo
        if stats["triggers_by_name"]:
            most_active = max(stats["triggers_by_name"].items(), key=lambda x: x[1])
            stats["most_active_trigger"] = most_active

        # Triggers recentes (últimos 5)
        stats["recent_triggers"] = self.trigger_history[-5:]

        # Mostrar estatísticas
        print(f"📊 Total de triggers: {stats['total_triggers']}")
        print()
        print("Por tipo:")
        for trigger_type, count in stats["triggers_by_type"].items():
            print(f"   • {trigger_type}: {count}")

        if stats["most_active_trigger"]:
            print(f"\n🏆 Mais ativo: {stats['most_active_trigger'][0]} ({stats['most_active_trigger'][1]} vezes)")

        print("\n🕒 Triggers recentes:")
        for trigger in stats["recent_triggers"]:
            triggered_at = trigger["triggered_at"][:16].replace("T", " ")
            print(f"   • {trigger['trigger_name']}: {triggered_at}")

        return stats

    def save_automation_config(self, output_path: str = "automation_config.json"):
        """Salva configuração de automação"""

        config_data = {
            "automation_rules": self.automation_rules,
            "trigger_history": self.trigger_history,
            "export_timestamp": datetime.now().isoformat()
        }

        with open(output_path, 'w') as f:
            json.dump(config_data, f, indent=2)

        print(f"💾 Configuração salva: {output_path}")

def demo_pipeline_automation():
    """Demonstração do sistema de automação"""

    print("🎯 DEMONSTRAÇÃO: AUTOMAÇÃO DE PIPELINES")
    print("=" * 80)

    try:
        # Conectar ao workspace
        credential = DefaultAzureCredential()
        ml_client = MLClient.from_config(credential=credential)

        print(f"✅ Conectado ao workspace: {ml_client.workspace_name}")

        # Criar sistema de automação
        automation = PipelineAutomation(ml_client)

        print("\n🔧 1. Configurando triggers de exemplo...")

        # Configuração de pipeline exemplo
        pipeline_config = {
            "pipeline_name": "data_processing_pipeline",
            "compute_cluster": "data-processing-cluster",
            "experiment_name": "automated_data_processing",
            "input_uri": "azureml://datastores/workspaceblobstore/paths/credit_default_dataset.csv"
        }

        # Data Trigger
        data_trigger = automation.create_data_trigger(
            trigger_name="new_data_trigger",
            data_asset_name="credit_default_raw",
            pipeline_config=pipeline_config,
            conditions={
                "new_version": True,
                "check_interval_hours": 2
            }
        )

        # Schedule Trigger
        schedule_trigger = automation.create_schedule_trigger(
            trigger_name="daily_processing",
            cron_expression="@daily",
            pipeline_config=pipeline_config
        )

        # Conditional Trigger
        conditional_trigger = automation.create_conditional_trigger(
            trigger_name="file_monitor",
            condition_check="file_exists:./new_data_batch.csv",
            pipeline_config=pipeline_config,
            check_interval_minutes=15
        )

        print("\n📋 2. Listando regras configuradas...")
        automation.list_automation_rules()

        print("\n🔍 3. Verificando triggers...")
        triggered = automation.check_triggers(execute_if_triggered=False)

        print("\n🔄 4. Criando workflow de exemplo...")

        workflow_stages = [
            {
                "name": "Data Validation",
                "type": "data_pipeline",
                "pipeline_name": "data_validation_pipeline"
            },
            {
                "name": "Feature Engineering", 
                "type": "data_pipeline",
                "pipeline_name": "feature_engineering_pipeline",
                "depends_on": ["Data Validation"]
            },
            {
                "name": "Model Training",
                "type": "ml_pipeline",
                "pipeline_name": "model_training_pipeline", 
                "depends_on": ["Feature Engineering"]
            }
        ]

        workflow = automation.create_workflow_automation(
            workflow_name="complete_ml_workflow",
            stages=workflow_stages
        )

        print("\n📊 5. Gerando estatísticas...")
        stats = automation.get_trigger_statistics()

        print("\n💾 6. Salvando configuração...")
        automation.save_automation_config()

        print("\n💡 FUNCIONALIDADES DE AUTOMAÇÃO:")
        print("-" * 40)
        print("• Data Triggers - Baseados em mudanças de assets")
        print("• Schedule Triggers - Execução baseada em agenda")
        print("• Conditional Triggers - Condições customizadas")
        print("• Workflow Automation - Múltiplas etapas sequenciais")
        print("• Monitoramento automático de triggers")
        print("• Estatísticas e histórico detalhado")

        return automation

    except Exception as e:
        print(f"❌ Erro na demonstração: {e}")
        return None

if __name__ == "__main__":
    demo_pipeline_automation()
