from pyspark.sql import Column

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeComplexBase(AutoMapperDataTypeBase):
    def __init__(self):
        super().__init__()

    def get_column_spec(self) -> Column:
        raise NotImplementedError
