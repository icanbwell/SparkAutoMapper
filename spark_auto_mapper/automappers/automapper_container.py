from typing import List, Union, Dict, Optional

from pyspark.sql import DataFrame, Column

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.automapper_with_column import AutoMapperWithColumn
from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


class AutoMapperContainer(AutoMapperBase):
    def __init__(self,
                 parent: AutoMapperBase
                 ) -> None:
        super().__init__(parent=parent)

        # set up a bunch of withColumn for each parameter to AutoMapperFhirDataTypeComplexBase
        self.mappers: List[AutoMapperWithColumn] = []

    def generate_mappers(self, mappers_dict: Dict[str, AutoMapperAnyDataType]) -> None:
        automapper: Union[AutoMapperWithColumn, AutoMapperContainer] = self
        for column, value in mappers_dict.items():
            automapper = AutoMapperWithColumn(
                parent=automapper,
                dst_column=column,
                value=value
            )
            assert isinstance(automapper, AutoMapperWithColumn)
            self.mappers.append(automapper)

    def transform_with_data_frame(self, df: DataFrame, source_df: DataFrame, keys: List[str]) -> DataFrame:
        return df  # we do nothing since self.mappers do all the work

    def get_column_specs(self) -> Dict[str, Column]:
        return {mapper.dst_column: mapper.get_column_spec() for mapper in self.mappers}

    def transform(self, df: DataFrame) -> DataFrame:
        # find the top most AutoMapper which has the information we need
        auto_mapper: Optional[AutoMapperBase] = self
        while auto_mapper and auto_mapper.parent:
            auto_mapper = auto_mapper.parent

        assert auto_mapper
        assert auto_mapper.keys
        source_df: DataFrame = df.sql_ctx.table(auto_mapper.source_view)
        destination_df: DataFrame = source_df.select(auto_mapper.keys)
        # start with the last mapper
        return self.mappers[-1].transform_with_data_frame(df=destination_df, source_df=source_df, keys=auto_mapper.keys)
