from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType


class AutoMapperColumns(AutoMapperContainer):
    def __init__(self, **kwargs: AutoMapperAnyDataType) -> None:
        """
        Create mappings for columns


        :param kwargs: dictionary of column, AutoMapper
        """
        super().__init__()

        self.generate_mappers(
            mappers_dict=kwargs,
            column_schema={},
            include_null_properties=False,
            skip_schema_validation=[],
            skip_if_columns_null_or_empty=None,
            enable_schema_pruning=False,
        )
