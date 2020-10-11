from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.automapper_container import AutoMapperContainer
from spark_auto_mapper.data_types.automapper_defined_types import AutoMapperAnyDataType


class AutoMapperColumns(AutoMapperContainer):
    def __init__(self,
                 parent: AutoMapperBase,
                 **kwargs: AutoMapperAnyDataType
                 ) -> None:
        super().__init__(parent=parent)

        self.generate_mappers(mappers_dict=kwargs)
