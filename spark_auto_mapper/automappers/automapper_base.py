from typing import List, Dict, Optional

from pyspark.sql import DataFrame, Column

from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult


class AutoMapperBase:
    def __init__(self) -> None:
        pass

    def transform_with_data_frame(
        self, df: DataFrame, source_df: Optional[DataFrame], keys: List[str]
    ) -> DataFrame:
        # implement in subclasses
        raise NotImplementedError

    def get_column_specs(self,
                         source_df: Optional[DataFrame]) -> Dict[str, Column]:
        raise NotImplementedError

    def check_schema(
        self, parent_column: Optional[str], source_df: Optional[DataFrame]
    ) -> Optional[CheckSchemaResult]:
        return None
