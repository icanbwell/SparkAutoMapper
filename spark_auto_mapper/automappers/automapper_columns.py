from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.automapper_container import AutoMapperContainer


class AutoMapperColumns(AutoMapperContainer):
    def __init__(self,
                 parent: AutoMapperBase,
                 **kwargs
                 ) -> None:
        super().__init__(parent=parent)

        # set up a bunch of withColumn for each parameter to AutoMapperFhirDataTypeComplexBase
        self.generate_mappers(kwargs)
