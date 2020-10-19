from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase


class AutoMapperWithComplex(AutoMapperContainer):
    def __init__(self, entity: AutoMapperDataTypeComplexBase) -> None:
        super().__init__()

        self.generate_mappers(
            mappers_dict={key: value
                          for key, value in entity.value.items()}
        )
