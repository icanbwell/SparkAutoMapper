from datetime import date, datetime

from pyspark.sql import Column
from pyspark.sql.functions import lit

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.data_types.automapper_native_types import AutoMapperSimpleType


class AutoMapperDataTypeLiteral(AutoMapperDataTypeBase):
    def __init__(self, value: AutoMapperSimpleType):
        super().__init__()
        self.value: AutoMapperSimpleType = value

    def get_column_spec(self) -> Column:
        if isinstance(self.value, str) or isinstance(self.value, int) \
                or isinstance(self.value, float) or isinstance(self.value, date) \
                or isinstance(self.value, datetime):
            return lit(self.value)

        raise ValueError(f"value: {self.value} is not str")
