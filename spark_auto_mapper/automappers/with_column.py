from spark_auto_mapper.automappers.with_column_base import AutoMapperWithColumnBase
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType


class AutoMapperWithColumn(AutoMapperWithColumnBase):
    # This class is just a helper that allows us to pass the parameters in the form dst_column=value
    def __init__(self, **kwargs: AutoMapperAnyDataType) -> None:
        assert len(kwargs) == 1, kwargs
        dst_column: str = list(kwargs.keys())[0]
        value = kwargs[dst_column]
        super().__init__(
            dst_column=dst_column,
            value=value,
            column_schema=None,
            include_null_properties=False,
            enable_schema_pruning=False,
        )
