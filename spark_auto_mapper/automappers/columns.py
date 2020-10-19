from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType


class AutoMapperColumns(AutoMapperContainer):
    def __init__(self, **kwargs: AutoMapperAnyDataType) -> None:
        super().__init__()

        self.generate_mappers(mappers_dict=kwargs)
