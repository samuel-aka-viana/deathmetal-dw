import logging
import os
from datetime import datetime
from pathlib import Path

from prefect import flow, task
from prefect.cache_policies import NONE
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent
dbt_project_path = project_root / "dbt_deathmetal"


@task(name="validate-dbt-setup", retries=1)
def validate_dbt_setup():
    logger.info("Validando setup DBT")

    if not dbt_project_path.exists():
        raise FileNotFoundError(f"Projeto DBT não encontrado: {dbt_project_path}")

    dbt_project_yml = dbt_project_path / "dbt_project.yml"
    if not dbt_project_yml.exists():
        raise FileNotFoundError(f"dbt_project.yml não encontrado: {dbt_project_yml}")

    profiles_dir = Path.home() / ".dbt"
    profiles_yml = profiles_dir / "profiles.yml"

    if not profiles_yml.exists():
        raise FileNotFoundError(f"profiles.yml não encontrado: {profiles_yml}")

    target = os.getenv("DBT_TARGET", "dev")
    use_bigquery = os.getenv("DBT_USE_BIGQUERY", "false").lower() == "true"

    profile_name = "dbt_deathmetal_bq" if use_bigquery else "dbt_deathmetal"

    logger.info(" Setup DBT validado!")
    logger.info(f"   • Projeto: {dbt_project_path}")
    logger.info(f"   • Target: {target}")
    logger.info(f"   • Profile: {profile_name}")
    logger.info(f"   • BigQuery: {use_bigquery}")

    return {
        "dbt_project_path": str(dbt_project_path),
        "profiles_dir": str(profiles_dir),
        "target": target,
        "use_bigquery": use_bigquery,
        "profile_name": profile_name
    }


@task(name="setup-dbt-runner", cache_policy=NONE)
def setup_dbt_runner(config):
    logger.info("Configurando PrefectDbtRunner...")

    dbt_settings = PrefectDbtSettings(
        project_dir=config["dbt_project_path"],
        profiles_dir=config["profiles_dir"]
    )

    runner = PrefectDbtRunner(
        settings=dbt_settings
    )

    logger.info(" PrefectDbtRunner configurado!")
    logger.info(f"   • Project Dir: {dbt_settings.project_dir}")
    logger.info(f"   • Profiles Dir: {dbt_settings.profiles_dir}")

    return runner


@task(name="dbt-run", retries=2, retry_delay_seconds=60, cache_policy=NONE)
def dbt_run(config, runner):
    logger.info(" Executando: dbt run")
    logger.info(f"   • Target: {config['target']}")
    logger.info(f"   • Profile: {config['profile_name']}")

    try:
        result = runner.invoke(
            args=["run", "--target", config["target"]]
        )

        logger.info("✅ dbt run concluído com sucesso!")
        logger.info("   • Models transformados")
        logger.info("   • Dados prontos para análise")

        return {
            "status": "success",
            "command": "dbt run",
            "target": config["target"],
            "result": str(result),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ dbt run falhou: {str(e)}")
        raise


@task(name="dbt-test", retries=1, cache_policy=NONE)
def dbt_test(config, runner):
    logger.info(" Executando: dbt test")
    logger.info(f"   • Target: {config['target']}")

    try:
        result = runner.invoke(
            args=["test", "--target", config["target"]],
            raise_on_failure=False
        )

        logger.info(" dbt test concluído!")
        logger.info("   • Verificação de qualidade executada")

        return {
            "status": "success",
            "command": "dbt test",
            "target": config["target"],
            "result": str(result),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.warning(f" dbt test teve problemas: {str(e)}")
        logger.warning("   • Alguns testes podem ter falhado")

        return {
            "status": "warning",
            "command": "dbt test",
            "error": str(e),
            "target": config["target"],
            "timestamp": datetime.now().isoformat()
        }


@task(name="dbt-docs-generate", cache_policy=NONE)
def dbt_docs_generate(config, runner):
    logger.info(" Executando: dbt docs generate")
    logger.info(f"   • Target: {config['target']}")

    try:
        result = runner.invoke(
            args=["docs", "generate", "--target", config["target"]]
        )

        docs_dir = Path(config["dbt_project_path"]) / "target"
        catalog_file = docs_dir / "catalog.json"
        manifest_file = docs_dir / "manifest.json"
        index_file = docs_dir / "index.html"

        files_generated = {
            "catalog": catalog_file.exists(),
            "manifest": manifest_file.exists(),
            "index": index_file.exists()
        }

        docs_ready = files_generated["catalog"] and files_generated["manifest"]

        logger.info("✅ dbt docs generate concluído!")
        logger.info(f"   • Arquivos em: {docs_dir}")
        logger.info(f"   • Catalog: {'✓' if files_generated['catalog'] else '✗'}")
        logger.info(f"   • Manifest: {'✓' if files_generated['manifest'] else '✗'}")
        logger.info(f"   • Index: {'✓' if files_generated['index'] else '✗'}")

        if docs_ready:
            logger.info(" Para visualizar a documentação:")
            logger.info(f"   cd {config['dbt_project_path']}")
            logger.info("   dbt docs serve")
            logger.info("   Depois acesse: http://localhost:8080")

        return {
            "status": "success",
            "command": "dbt docs generate",
            "docs_path": str(docs_dir),
            "files_generated": files_generated,
            "docs_ready": docs_ready,
            "target": config["target"],
            "result": str(result),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"dbt docs generate falhou: {str(e)}")
        raise


@flow(name="Modern DBT Pipeline", log_prints=True)
def modern_dbt_pipeline():
    logger.info(" Iniciando Modern DBT Pipeline...")
    logger.info(f" {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    try:
        logger.info("\n VALIDAÇÃO")
        config = validate_dbt_setup()

        logger.info("\n️ SETUP")
        runner = setup_dbt_runner(config)

        logger.info("\n TRANSFORM")
        run_result = dbt_run(config, runner)
        logger.info(run_result)

        logger.info("\n QUALITY CHECK")
        test_result = dbt_test(config, runner)
        logger.info(test_result)

        logger.info("\n DOCS GENERATED")

        docs_result = dbt_docs_generate(config, runner)
        logger.info(docs_result)

        return "SUCCESS"

    except Exception as e:
        return "ERROR"


if __name__ == "__main__":
    resultado = modern_dbt_pipeline()
    print(f"Resultado final: {resultado}")
