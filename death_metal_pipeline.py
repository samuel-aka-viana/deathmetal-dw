import logging
from typing import Iterator, Dict, Any

import dlt
from dlt.sources.filesystem import filesystem, read_csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dlt.source(name="death_metal_data")
def death_metal_source():
    @dlt.resource(
        name="metal_bands",
        write_disposition="replace",
        primary_key="id"
    )
    def load_metal_bands() -> Iterator[Dict[str, Any]]:
        source = filesystem(
            bucket_url="s3://death-metal-raw",
            file_glob="bands.csv"
        ) | read_csv()

        for row in source:
            yield row

    @dlt.resource(
        name="metal_albums",
        write_disposition="replace",
        primary_key="id"
    )
    def load_metal_albums() -> Iterator[Dict[str, Any]]:
        source = filesystem(
            bucket_url="s3://death-metal-raw",
            file_glob="albums.csv"
        ) | read_csv()

        for row in source:
            yield row

    @dlt.resource(
        name="metal_reviews",
        write_disposition="replace",
        primary_key="id"
    )
    def load_metal_reviews() -> Iterator[Dict[str, Any]]:
        source = filesystem(
            bucket_url="s3://death-metal-raw",
            file_glob="reviews.csv"
        ) | read_csv()

        for row in source:
            yield row

    return load_metal_bands(), load_metal_albums(), load_metal_reviews()


def main():
    pipeline = dlt.pipeline(
        pipeline_name="death_metal_pipeline",
        destination="duckdb",
        dataset_name="metal_data",
        progress="tqdm"
    )

    pipeline.drop()

    load_info = pipeline.run(
        death_metal_source(),
        write_disposition="replace"
    )

    print(f"📊 Load info: {load_info}")

    df = pipeline.dataset(dataset_type="default").metal_bands.df()
    print(f"📈 Total de registros: {len(df)}")
    print(f"📈 IDs únicos: {df['id'].nunique()}")

    duplicates = df[df.duplicated(subset=['id'], keep=False)]
    if not duplicates.empty:
        print(f"⚠️  ATENÇÃO: {len(duplicates)} registros duplicados encontrados!")
        print(duplicates[['id']].value_counts())
    else:
        print("✅ Nenhuma duplicata encontrada!")

    print(df.head())
    return pipeline





if __name__ == "__main__":
    # 🔧 SOLUÇÃO 1: Limpar e recarregar

    pipeline = main()


    print("\n🎉 Processo concluído!")