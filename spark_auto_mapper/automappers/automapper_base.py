from typing import List, Dict

from pyspark.sql import DataFrame, Column


class AutoMapperBase:
    def __init__(self) -> None:
        pass

    def transform_with_data_frame(
        self, df: DataFrame, source_df: DataFrame, keys: List[str]
    ) -> DataFrame:
        # implement in subclasses
        raise NotImplementedError

    def get_column_specs(self, source_df: DataFrame) -> Dict[str, Column]:
        raise NotImplementedError
