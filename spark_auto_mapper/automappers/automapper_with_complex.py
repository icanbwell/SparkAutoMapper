from spark_auto_mapper.automappers.automapper_container import AutoMapperContainer
from spark_auto_mapper.data_types.automapper_data_type_complex_base import AutoMapperDataTypeComplexBase


class AutoMapperWithComplex(AutoMapperContainer):
    def __init__(self,
                 entity: AutoMapperDataTypeComplexBase
                 ) -> None:
        super().__init__()

        self.generate_mappers(mappers_dict={key: value for key, value in entity.value.items()})
