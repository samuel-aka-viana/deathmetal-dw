import logging
import os
from typing import Iterator, Dict, Any
from abc import ABC, abstractmethod

import dlt
from dlt.sources.filesystem import filesystem, read_csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseDeathMetalPipeline(ABC):
    def __init__(self):
        self.bucket_name = "death-metal-raw-data"
        self.project_id = os.getenv("DBT_BIGQUERY_PROJECT", "dw-bigquery-462900")

    @abstractmethod
    def get_destination_config(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_bucket_url(self) -> str:
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        pass

    def _create_metal_bands_resource(self):
        columns = {
            "id": {"data_type": "bigint"},
            "name": {"data_type": "text"},
            "country": {"data_type": "text"},
            "status": {"data_type": "text"},
            "formed_in": {"data_type": "bigint"},
            "genre": {"data_type": "text"},
            "theme": {"data_type": "text"},
            "active": {"data_type": "text"}
        }

        @dlt.resource(
            name="metal_bands",
            write_disposition="replace",
            primary_key="id",
            columns=columns if self._use_schema() else None
        )
        def load_metal_bands() -> Iterator[Dict[str, Any]]:
            source = filesystem(
                bucket_url=self.get_bucket_url()+'/bands',
                file_glob="*.csv"
            ) | read_csv()

            for row in source:
                yield row

        return load_metal_bands

    def _create_metal_albums_resource(self):
        columns = {
            "id": {"data_type": "bigint"},
            "band": {"data_type": "bigint"},
            "title": {"data_type": "text"},
            "year": {"data_type": "bigint"}
        }

        @dlt.resource(
            name="metal_albums",
            write_disposition="replace",
            primary_key="id",
            columns=columns if self._use_schema() else None
        )
        def load_metal_albums() -> Iterator[Dict[str, Any]]:
            source = filesystem(
                bucket_url=self.get_bucket_url()+'/albums',
                file_glob="*.csv"
            ) | read_csv()

            for row in source:
                yield row

        return load_metal_albums

    def _create_metal_reviews_resource(self):
        columns = {
            "id": {"data_type": "bigint"},
            "album": {"data_type": "bigint"},
            "title": {"data_type": "text"},
            "score": {"data_type": "double"},
            "content": {"data_type": "text"}
        }

        @dlt.resource(
            name="metal_reviews",
            write_disposition="replace",
            primary_key="id",
            columns=columns if self._use_schema() else None
        )
        def load_metal_reviews() -> Iterator[Dict[str, Any]]:
            source = filesystem(
                bucket_url=self.get_bucket_url()+'/reviews',
                file_glob="*.csv"
            ) | read_csv()

            for row in source:
                yield row

        return load_metal_reviews

    @abstractmethod
    def _use_schema(self) -> bool:
        pass

    def create_source(self):
        @dlt.source(name=self.get_source_name())
        def death_metal_source():
            return (
                self._create_metal_bands_resource()(),
                self._create_metal_albums_resource()(),
                self._create_metal_reviews_resource()()
            )

        return death_metal_source

    def run_pipeline(self):
        config = self.get_destination_config()

        pipeline = dlt.pipeline(
            pipeline_name=config["pipeline_name"],
            destination=config["destination"],
            dataset_name=config["dataset_name"],
            progress="tqdm"
        )

        if config["destination"] == "duckdb":
            pipeline.drop()

        load_info = pipeline.run(
            self.create_source()(),
            write_disposition="replace"
        )

        logger.info(f"📊 Load Info: {load_info}")

        if config["destination"] == "duckdb":
            df = pipeline.dataset(dataset_type="default").metal_bands.df()
            print(df.head())

        return pipeline


class DuckDBDeathMetalPipeline(BaseDeathMetalPipeline):
    def get_destination_config(self) -> Dict[str, Any]:
        return {
            "pipeline_name": "death_metal_pipeline",
            "destination": "duckdb",
            "dataset_name": os.getenv("DBT_DUCKDB_SCHEMA", "metal_data")
        }

    def get_bucket_url(self) -> str:
        return "s3://death-metal-raw"

    def get_source_name(self) -> str:
        return "death_metal_data"

    def _use_schema(self) -> bool:
        return False


class BigQueryDeathMetalPipeline(BaseDeathMetalPipeline):
    def get_destination_config(self) -> Dict[str, Any]:
        env_target = os.getenv("DBT_TARGET", "dev")
        if env_target == "prod":
            dataset_name = os.getenv("DBT_BIGQUERY_PROD_DATASET", "death_metal_analytics_prod")
        else:
            dataset_name = os.getenv("DBT_BIGQUERY_DEV_DATASET", "death_metal_analytics")

        return {
            "pipeline_name": "death_metal_pipeline_bq",
            "destination": "bigquery",
            "dataset_name": dataset_name
        }

    def get_bucket_url(self) -> str:
        return f"gs://{self.bucket_name}"

    def get_source_name(self) -> str:
        return "death_metal_data_bq"

    def _use_schema(self) -> bool:
        return True


class DeathMetalPipelineFactory:
    @staticmethod
    def create_pipeline() -> BaseDeathMetalPipeline:
        use_bigquery = os.getenv("DBT_USE_BIGQUERY", "false").lower() == "true"

        if use_bigquery:
            logger.info("🚀 Executando pipeline BigQuery...")
            return BigQueryDeathMetalPipeline()
        else:
            logger.info("🚀 Executando pipeline DuckDB...")
            return DuckDBDeathMetalPipeline()


if __name__ == "__main__":
    pipeline_instance = DeathMetalPipelineFactory.create_pipeline()

    if isinstance(pipeline_instance, BigQueryDeathMetalPipeline):
        logger.info(f"📍 Usando bucket: gs://{pipeline_instance.bucket_name}")

    pipeline = pipeline_instance.run_pipeline()

    if isinstance(pipeline_instance, BigQueryDeathMetalPipeline):
        logger.info("\n🎉 Pipeline BigQuery executado com sucesso!")
        logger.info(f"🔗 Acesse: https://console.cloud.google.com/bigquery?project={pipeline_instance.project_id}")
        logger.info(
            f"📊 Dataset: {pipeline_instance.project_id}.{pipeline_instance.get_destination_config()['dataset_name']}")
    else:
        logger.info("\n🎉 Pipeline DuckDB executado com sucesso!")
        logger.info(f"📊 Banco: {os.getenv('DBT_DUCKDB_PATH', 'death_metal.duckdb')}")