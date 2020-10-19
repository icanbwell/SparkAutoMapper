from typing import List, Dict

from pyspark.sql import DataFrame, Column

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.with_column_base import AutoMapperWithColumnBase
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType


class AutoMapperContainer(AutoMapperBase):
    def __init__(self) -> None:
        super().__init__()

        # set up a bunch of withColumn for each parameter to AutoMapperFhirDataTypeComplexBase
        self.mappers: Dict[str, AutoMapperBase] = {}

    def generate_mappers(
        self, mappers_dict: Dict[str, AutoMapperAnyDataType]
    ) -> None:
        for column, value in mappers_dict.items():
            automapper = AutoMapperWithColumnBase(
                dst_column=column, value=value
            )
            assert isinstance(automapper,
                              AutoMapperWithColumnBase), type(automapper)
            self.mappers[column] = automapper

    def transform_with_data_frame(
        self, df: DataFrame, source_df: DataFrame, keys: List[str]
    ) -> DataFrame:
        return df  # we do nothing since self.mappers do all the work

    def get_column_specs(self, source_df: DataFrame) -> Dict[str, Column]:
        return {
            column_name:
            mapper.get_column_specs(source_df=source_df)[column_name]
            for column_name, mapper in self.mappers.items()
        }
