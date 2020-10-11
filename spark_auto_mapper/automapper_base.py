from typing import List, Optional

from pyspark.sql import DataFrame


class AutoMapperBase:
    def __init__(self,
                 parent: Optional['AutoMapperBase'] = None,
                 view: str = None,
                 source_view: str = None,
                 keys: List[str] = None
                 ) -> None:
        self.parent: Optional[AutoMapperBase] = parent
        self.view: Optional[str] = view
        self.source_view: Optional[str] = source_view
        self.keys: Optional[List[str]] = keys

    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        raise NotImplementedError
