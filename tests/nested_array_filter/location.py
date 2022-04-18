from typing import Optional, List

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.type_definitions.defined_types import AutoMapperString
from tests.nested_array_filter.schedule import AutoMapperElasticSearchSchedule


class AutoMapperElasticSearchLocation(AutoMapperDataTypeComplexBase):
    # noinspection PyPep8Naming
    def __init__(
        self,
        name: Optional[AutoMapperString] = None,
        scheduling: Optional[AutoMapperElasticSearchSchedule] = None,
    ) -> None:
        super().__init__(
            name=name,
            scheduling=scheduling,
        )
        super().include_null_properties(include_null_properties=True)

    @staticmethod
    def my_schema() -> StructType:
        return StructType(
            [
                StructField("name", StringType()),
                StructField("scheduling", AutoMapperElasticSearchSchedule.my_schema()),
            ]
        )

    def get_schema(
        self, include_extension: bool, extension_fields: Optional[List[str]] = None
    ) -> Optional[StructType]:
        return AutoMapperElasticSearchLocation.my_schema()
