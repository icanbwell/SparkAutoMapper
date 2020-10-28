from datetime import date, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import lit

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.type_definitions.native_types import AutoMapperNativeSimpleType


class AutoMapperDataTypeLiteral(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperNativeSimpleType):
        super().__init__()
        self.value: AutoMapperNativeSimpleType = value

    def get_column_spec(self, source_df: DataFrame) -> Column:
        if isinstance(self.value, str) or isinstance(self.value, int) \
                or isinstance(self.value, float) or isinstance(self.value, date) \
                or isinstance(self.value, datetime):
            return lit(self.value)

        raise ValueError(f"value: {self.value} is not str")