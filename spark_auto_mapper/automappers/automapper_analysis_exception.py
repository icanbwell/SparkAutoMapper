from typing import Optional, Any, List

from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult


class AutoMapperAnalysisException(Exception):
    """
    Exception thrown if there is an error in the AutoMapper
    """

    def __init__(
        self,
        *,
        automapper_name: Optional[str],
        msg: str,
        column_name: str,
        check_schema_result: Optional[CheckSchemaResult],
        column_values: Optional[List[Any]],
    ) -> None:
        """


        :param msg: message of error
        :param column_name: column that caused the error
        :param check_schema_result: result of checking the schemas
        :param column_values: value of columns
        """
        self.column_name: str = column_name
        self.check_schema_result: Optional[CheckSchemaResult] = check_schema_result
        self.column_values: Optional[List[Any]] = column_values
        self.automapper_name: Optional[str] = automapper_name
        super().__init__(msg)

    def __str__(self) -> str:
        """
        String representation
        """
        result: str = "AutoMapperAnalysisException: \n"
        result += f" in automapper [{self.automapper_name}] \n"
        result += f" in column [{self.column_name}] \n"
        result += super().__str__() + "\n"
        if self.check_schema_result:
            result += self.check_schema_result.to_string() + "\n"
        if self.column_values:
            result += "\n"
            result += "Sample source values: \n"
            result += "+------------------------+\n"
            result += f"   {self.column_name}  \n"
            result += "+------------------------+\n"
            for val in self.column_values:
                result += f"   {val}   \n"
            result += "+------------------------+\n"
        return result
