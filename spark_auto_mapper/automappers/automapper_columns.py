from spark_auto_mapper.automappers.automapper_container import AutoMapperContainer
from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


class AutoMapperColumns(AutoMapperContainer):
    def __init__(self,
                 **kwargs: AutoMapperAnyDataType
                 ) -> None:
        super().__init__()

        self.generate_mappers(mappers_dict=kwargs)
