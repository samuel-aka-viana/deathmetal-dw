import logging
from typing import Iterator, Dict, Any

import dlt
from dlt.sources.filesystem import filesystem, read_csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = "death-metal-raw-data"
PROJECT_ID = "dw-bigquery-462900"


@dlt.source(name="death_metal_data_bq")
def death_metal_source_bigquery():
    @dlt.resource(
        name="metal_bands",
        write_disposition="replace",
        primary_key="id",
        columns={
            "id": {"data_type": "bigint"},
            "name": {"data_type": "text"},
            "country": {"data_type": "text"},
            "status": {"data_type": "text"},
            "formed_in": {"data_type": "bigint"},
            "genre": {"data_type": "text"},
            "theme": {"data_type": "text"},
            "active": {"data_type": "text"}
        }
    )
    def load_metal_bands() -> Iterator[Dict[str, Any]]:
        source = filesystem(
            bucket_url=f"gs://{BUCKET_NAME}",
            file_glob="bands.csv"
        ) | read_csv()

        for row in source:
            yield row

    @dlt.resource(
        name="metal_albums",
        write_disposition="replace",
        primary_key="id",
        columns={
            "id": {"data_type": "bigint"},
            "band": {"data_type": "bigint"},
            "title": {"data_type": "text"},
            "year": {"data_type": "bigint"}
        }
    )
    def load_metal_albums() -> Iterator[Dict[str, Any]]:
        source = filesystem(
            bucket_url=f"gs://{BUCKET_NAME}",
            file_glob="albums.csv"
        ) | read_csv()

        for row in source:
            yield row

    @dlt.resource(
        name="metal_reviews",
        write_disposition="replace",
        primary_key="id",
        columns={
            "id": {"data_type": "bigint"},
            "album": {"data_type": "bigint"},
            "title": {"data_type": "text"},
            "score": {"data_type": "double"},
            "content": {"data_type": "text"}
        }
    )
    def load_metal_reviews() -> Iterator[Dict[str, Any]]:
        source = filesystem(
            bucket_url=f"gs://{BUCKET_NAME}",
            file_glob="reviews.csv"
        ) | read_csv()

        for row in source:
            yield row

    return load_metal_bands(), load_metal_albums(), load_metal_reviews()


def run_bigquery_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="death_metal_pipeline_bq",
        destination="bigquery",
        dataset_name="death_metal_analytics",
        progress="tqdm"
    )
    load_info = pipeline.run(
        death_metal_source_bigquery(),
        write_disposition="replace"
    )

    logger.info(f"📊 BigQuery Load Info: {load_info}")

    return pipeline


if __name__ == "__main__":
    logger.info("🚀 Executando pipeline BigQuery...")
    logger.info(f"📍 Usando bucket: gs://{BUCKET_NAME}")

    pipeline = run_bigquery_pipeline()
    logger.info("\n🎉 Pipeline BigQuery executado com sucesso!")
    logger.info(f"🔗 Acesse: https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
    logger.info(f"📊 Dataset: {PROJECT_ID}.death_metal_analytics")
