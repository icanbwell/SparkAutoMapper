from datetime import date, datetime
from typing import Union

from pyspark.sql import Column
from pyspark.sql.functions import lit

from spark_auto_mapper.data_types.automapper_data_type_base import AutoMapperDataTypeBase


class AutoMapperDataTypeLiteral(AutoMapperDataTypeBase):
    def __init__(self, value: Union[str, int, float, date, datetime]):
        super().__init__()
        self.value: Union[str, int, float, date, datetime] = value

    def get_column_spec(self) -> Column:
        if isinstance(self.value, str) or isinstance(self.value, int) \
                or isinstance(self.value, float) or isinstance(self.value, date) \
                or isinstance(self.value, datetime):
            return lit(self.value)

        raise ValueError(f"value: {self.value} is not str")
