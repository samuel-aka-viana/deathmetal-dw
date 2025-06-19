import os
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from prefect import flow, task
import logging

try:
    from death_metal_pipeline import DeathMetalPipelineFactory
except ImportError:
    from death_metal_pipeline import DeathMetalPipelineFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(name="validate-environment", retries=1)
def validate_environment():
    logger.info("🔍 Validando ambiente...")

    required_vars = []
    optional_vars = {
        "DBT_USE_BIGQUERY": "false",
        "DBT_TARGET": "dev",
        "DBT_DUCKDB_SCHEMA": "metal_data"
    }

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Variáveis de ambiente obrigatórias não encontradas: {missing_vars}")

    for var, default in optional_vars.items():
        if not os.getenv(var):
            os.environ[var] = default
            logger.info(f" Configurado {var}={default}")

    use_bigquery = os.getenv("DBT_USE_BIGQUERY", "false").lower() == "true"
    target = os.getenv("DBT_TARGET", "dev")

    logger.info(f"✅ Ambiente validado:")
    logger.info(f"   • Usando BigQuery: {use_bigquery}")
    logger.info(f"   • Target: {target}")

    return {
        "use_bigquery": use_bigquery,
        "target": target,
        "timestamp": datetime.now().isoformat()
    }


@task(name="create-pipeline-instance", retries=2)
def create_pipeline_instance(env_config):
    logger.info(" Criando instância do pipeline...")

    try:
        pipeline_instance = DeathMetalPipelineFactory.create_pipeline()

        logger.info(f"✅ Pipeline criado: {type(pipeline_instance).__name__}")
        logger.info(f"   • Fonte: {pipeline_instance.get_source_name()}")
        logger.info(f"   • Bucket: {pipeline_instance.get_bucket_url()}")

        return pipeline_instance

    except Exception as e:
        logger.error(f" Erro ao criar pipeline: {str(e)}")
        raise


@task(name="execute-data-pipeline", retries=2, retry_delay_seconds=30)
def execute_data_pipeline(pipeline_instance):
    logger.info(" Executando pipeline de dados...")

    try:
        pipeline = pipeline_instance.run_pipeline()

        config = pipeline_instance.get_destination_config()

        result = {
            "pipeline_name": config["pipeline_name"],
            "destination": config["destination"],
            "dataset_name": config["dataset_name"],
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }

        if hasattr(pipeline_instance, 'project_id'):
            result["project_id"] = pipeline_instance.project_id
            result["bigquery_url"] = f"https://console.cloud.google.com/bigquery?project={pipeline_instance.project_id}"

        logger.info(" Pipeline executado com sucesso!")
        return result

    except Exception as e:
        logger.error(f" Erro na execução do pipeline: {str(e)}")
        raise



@flow(name="Death Metal Data Pipeline", log_prints=True)
def death_metal_data_pipeline():
    logger.info(" Iniciando Death Metal Data Pipeline...")
    logger.info(f" Horário: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    try:
        env_config = validate_environment()

        pipeline_instance = create_pipeline_instance(env_config)

        pipeline_result = execute_data_pipeline(pipeline_instance)
        logger.info(pipeline_result)

        return "SUCCESS"

    except Exception as e:
        logger.error(f" Erro no Death Metal Pipeline: {str(e)}")
        return "ERROR"


if __name__ == "__main__":
    death_metal_data_pipeline()
