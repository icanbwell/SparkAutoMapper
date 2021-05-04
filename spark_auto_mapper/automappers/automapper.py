from pathlib import Path
from typing import List, Optional, Union, Dict

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import monotonically_increasing_id
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult
from spark_auto_mapper.automappers.automapper_exception import AutoMapperException
from spark_auto_mapper.automappers.column_spec_wrapper import ColumnSpecWrapper
from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.automappers.complex import AutoMapperWithComplex
from spark_auto_mapper.data_types.complex.complex_base import AutoMapperDataTypeComplexBase
from spark_auto_mapper.helpers.spark_helpers import SparkHelpers
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType

TEMPORARY_KEY = "__row_id"


class AutoMapper(AutoMapperContainer):
    # noinspection PyDefaultArgument
    def __init__(
        self,
        keys: Optional[List[str]] = None,
        view: Optional[str] = None,
        source_view: Optional[str] = None,
        keep_duplicates: bool = False,
        drop_key_columns: bool = True,
        checkpoint_after_columns: Optional[int] = None,
        checkpoint_path: Optional[Union[str, Path]] = None,
        reuse_existing_view: bool = False,
        use_schema: bool = True,
        include_extension: bool = False,
        include_null_properties: bool = False,
        use_single_select: bool = True,
        verify_row_count: bool = True,
        skip_schema_validation: List[str] = ["extension"],
        skip_if_columns_null_or_empty: Optional[List[str]] = None,
        keep_null_rows: bool = False,
        filter_by: Optional[str] = None,
        enable_logging: bool = True,
        check_schema_for_all_columns: bool = False
    ):
        """
        Creates an AutoMapper


        :param keys: joining keys
        :param view: view to return
        :param source_view: where to load the data from
        :param keep_duplicates: whether to leave duplicates at the end
        :param drop_key_columns: whether to drop the key columns at the end
        :param checkpoint_after_columns: checkpoint after how many columns have been processed
        :param checkpoint_path: Path where to store the checkpoints
        :param reuse_existing_view: If view already exists, whether to reuse it or create a new one
        :param use_schema: apply schema to columns
        :param include_extension: By default we don't include extension elements since they take up a lot of schema.
                If you're using extensions then set this
        :param include_null_properties: If you want to include null properties
        :param use_single_select: This is a faster way to run the AutoMapper since it will select all the columns at once.
                However this makes it harder to debug since you don't know what column failed
        :param verify_row_count: verifies that the count of rows remains the same before and after the transformation
        :param skip_schema_validation: skip schema checks on these columns
        :param skip_if_columns_null_or_empty: skip creating the record if any of these columns are null or empty
        :param keep_null_rows: whether to keep the null rows instead of removing them
        :param filter_by: (Optional) SQL expression that is used to filter
        """
        super().__init__()
        self.view: Optional[str] = view
        self.source_view: Optional[str] = source_view
        self.keys: Optional[List[str]] = keys
        self.keep_duplicates: bool = keep_duplicates
        self.drop_key_columns: bool = drop_key_columns
        self.checkpoint_after_columns: Optional[int] = checkpoint_after_columns
        self.checkpoint_path: Optional[Union[str, Path]] = checkpoint_path
        self.reuse_existing_view: bool = reuse_existing_view
        self.use_schema: bool = use_schema
        self.include_extension: bool = include_extension
        self.include_null_properties: bool = include_null_properties
        self.use_single_select: bool = use_single_select
        self.verify_row_count: bool = verify_row_count
        self.skip_schema_validation: List[str] = skip_schema_validation
        self.skip_if_columns_null_or_empty: Optional[
            List[str]] = skip_if_columns_null_or_empty
        self.keep_null_rows: bool = keep_null_rows
        self.filter_by: Optional[str] = filter_by
        self.enable_logging: bool = enable_logging
        self.check_schema_for_all_columns: bool = check_schema_for_all_columns

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def transform_with_data_frame_single_select(
        self, df: DataFrame, source_df: DataFrame, keys: List[str]
    ) -> DataFrame:
        # get all the column specs
        column_specs: List[Column] = [
            child_mapper.get_column_specs(source_df=source_df)[column_name]
            for column_name, child_mapper in self.mappers.items()
        ]

        try:
            # print("========== NEW 2 ==============")
            if not self.drop_key_columns:
                column_specs = [col(f"b.{c}") for c in keys] + column_specs

            if self.enable_logging:
                print(f"-------- automapper ({self.view}) column specs ------")
                print(self.to_debug_string(source_df=source_df))
                print(
                    f"-------- end automapper ({self.view}) column specs ------"
                )
                print(
                    f"-------- automapper ({self.source_view}) source_df schema ------"
                )
                source_df.printSchema()
                print(
                    f"-------- end automapper ({self.source_view}) source_df schema ------"
                )

            if self.check_schema_for_all_columns:
                for column_name, mapper in self.mappers.items():
                    check_schema_result: Optional[CheckSchemaResult
                                                  ] = mapper.check_schema(
                                                      parent_column=None,
                                                      source_df=source_df
                                                  )
                    if check_schema_result and len(
                        check_schema_result.result.errors
                    ) > 0:
                        print(
                            f"==== ERROR: Schema Mismatch [{column_name}] ==="
                            f"{str(check_schema_result)}"
                        )
                    else:
                        print(f"==== Schema Matches: [{column_name}] ====")

            # run all the selects
            df = source_df.alias("b").select(*column_specs)
            # write out final checkpoint for this automapper
            if self.checkpoint_path:
                checkpoint_path = Path(self.checkpoint_path
                                       ).joinpath(self.view
                                                  or "df").joinpath("final")
                df.write.parquet(str(checkpoint_path))
                df = df.sql_ctx.read.parquet(str(checkpoint_path))
        except AnalysisException:
            # iterate through each column to find the problem child
            for column_name, mapper in self.mappers.items():
                try:
                    print(f"========= Processing {column_name} =========== ")
                    column_spec = mapper.get_column_specs(source_df=source_df
                                                          )[column_name]
                    source_df.alias("b").select(column_spec).limit(1).count()
                    print(
                        f"========= Done Processing {column_name} =========== "
                    )
                except AnalysisException as e2:
                    print(
                        f"========= checking Schema {column_name} =========== "
                    )
                    check_schema_result = mapper.check_schema(
                        parent_column=None, source_df=source_df
                    )
                    msg: str = ""
                    if e2.desc.startswith("cannot resolve 'array"):
                        msg = "Looks like the elements of the array have different structures.  " \
                              "All items in an array should have the exact same structure.  " \
                              "You can pass in include_nulls to AutoMapperDataTypeComplexBase to force it to create " \
                              "null values for each element in the structure. "
                    msg += self.get_message_for_exception(
                        column_name + ": " + str(check_schema_result), df, e2,
                        source_df
                    )
                    raise AutoMapperException(msg) from e2
        except Exception as e:
            print("====  OOPS ===========")
            msg = self.get_message_for_exception("", df, e, source_df)
            raise Exception(msg) from e

        print(f"========= Finished AutoMapper {self.view} =========== ")
        return df

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def transform_with_data_frame(
        self, df: DataFrame, source_df: Optional[DataFrame], keys: List[str]
    ) -> DataFrame:
        current_child_number: int = 0
        # iterate over each child mapper and run it
        for column_name, child_mapper in self.mappers.items():
            current_child_number += 1
            try:
                # checkpoint if specified
                # https://livebook.manning.com/book/spark-in-action-second-edition/16-cache-and-checkpoint-enhancing-spark-s-performances/v-14/43
                if self.checkpoint_after_columns:
                    if (
                        current_child_number % self.checkpoint_after_columns
                    ) == 0:
                        if not self.checkpoint_path:
                            df = df.checkpoint(eager=True)
                        else:
                            checkpoint_path: Path = Path(
                                self.checkpoint_path
                            ).joinpath(self.view or "df"
                                       ).joinpath(str(current_child_number))
                            df.write.parquet(str(checkpoint_path))
                            df = df.sql_ctx.read.parquet(str(checkpoint_path))

                before_row_count: int = -1
                if self.verify_row_count:
                    before_row_count = df.count()
                # transform the next child mapper
                df = child_mapper.transform_with_data_frame(
                    df=df, source_df=source_df, keys=keys
                )
                if self.verify_row_count:
                    after_row_count: int = df.count()
                    assert before_row_count == after_row_count

            except AnalysisException as e:
                msg: str = ""
                if e.desc.startswith("cannot resolve 'array"):
                    msg = "Looks like the elements of the array have different structures.  " \
                          "All items in an array should have the exact same structure.  " \
                          "You can pass in include_nulls to AutoMapperDataTypeComplexBase to force it to create " \
                          "null values for each element in the structure. "
                if source_df is not None:
                    msg += self.get_message_for_exception(
                        column_name, df, e, source_df
                    )
                raise Exception(msg) from e
            except Exception as e:
                msg = self.get_message_for_exception(
                    column_name, df, e, source_df
                ) if source_df is not None else ""
                raise Exception(msg) from e

        # write out final checkpoint for this automapper
        if self.checkpoint_path:
            checkpoint_path = Path(self.checkpoint_path
                                   ).joinpath(self.view
                                              or "df").joinpath("final")
            df.write.parquet(str(checkpoint_path))
            df = df.sql_ctx.read.parquet(str(checkpoint_path))

        return df

    @staticmethod
    def get_message_for_exception(
        column_name: str, df: DataFrame, e: Exception, source_df: DataFrame
    ) -> str:
        # write out the full list of columns
        columns_in_source: List[str] = list(source_df.columns)
        columns_in_destination: List[str] = list(df.columns)
        msg: str = f", Processing column:[{column_name}]"
        msg += str(e)
        msg += f", Source columns:[{','.join(columns_in_source)}]"
        msg += f", Destination columns:[{','.join(columns_in_destination)}]"
        return msg

    def transform(self, df: DataFrame) -> DataFrame:
        # if source_view is specified then load that else assume that df is the source view
        source_df: DataFrame = df.sql_ctx.table(
            self.source_view
        ) if self.source_view else df
        # if keys are not specified then add a __row_id column to both the source and the destination
        #   and use that as key
        if not self.keys or len(self.keys) == 0:
            assert not self.view or not SparkHelpers.spark_table_exists(
                sql_ctx=df.sql_ctx, view=self.view
            )
            self.keys = [TEMPORARY_KEY]
            source_df = source_df.withColumn(
                TEMPORARY_KEY, monotonically_increasing_id()
            )

        # if view is specified then check if it exists
        destination_df: DataFrame = df.sql_ctx.table(self.view) \
            if self.view and self.reuse_existing_view and SparkHelpers.spark_table_exists(sql_ctx=df.sql_ctx,
                                                                                          view=self.view) \
            else source_df.select(self.keys)

        # if filter is specified then run it
        if self.filter_by is not None:
            source_df = source_df.where(self.filter_by)

        # rename key columns to avoid name clash if someone creates a column with that name
        renamed_key_columns: List[str] = []
        for key in self.keys:
            renamed_key_column: str = f"__{key}"
            source_df = source_df.withColumn(renamed_key_column, col(key))
            try:
                destination_df = destination_df.withColumn(
                    renamed_key_column, col(key)
                )
            except Exception as e:
                # write out the full list of columns
                columns_in_source: List[str] = list(source_df.columns)
                columns_in_destination: List[str] = list(
                    destination_df.columns
                )
                msg: str = str(e)
                msg += f", Source columns:[{','.join(columns_in_source)}]"
                msg += f", Destination columns:[{','.join(columns_in_destination)}]"
                raise Exception(msg) from e
            renamed_key_columns.append(renamed_key_column)

        # run the mapper
        if self.use_single_select:
            result_df: DataFrame = self.transform_with_data_frame_single_select(
                df=destination_df, source_df=source_df, keys=self.keys
            )
        else:
            result_df = self.transform_with_data_frame(
                df=destination_df,
                source_df=source_df,
                keys=renamed_key_columns
            )

        # now drop the __row_id if we added it
        result_df = result_df.drop(TEMPORARY_KEY)

        # drop the key columns
        if self.drop_key_columns:
            result_df = result_df.drop(*self.keys)

        # drop the renamed key columns
        result_df = result_df.drop(*renamed_key_columns)

        # replace any columns we had prepended with "___" to avoid a name clash with key columns
        for column_name in [
            c for c in result_df.columns if c.startswith("___")
        ]:
            result_df = result_df.withColumnRenamed(
                column_name, column_name.replace("___", "")
            )

        # drop any rows where all values are null
        if not self.keep_null_rows:
            result_df = result_df.dropna(how="all")

        # remove duplicates
        if not self.keep_duplicates:
            result_df = result_df.drop_duplicates()

        # if view was specified then create that view
        if self.view:
            result_df.createOrReplaceTempView(self.view)
        return result_df

    def register_child(self, dst_column: str, child: 'AutoMapperBase') -> None:
        self.mappers[dst_column] = child

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def columns(self, **kwargs: AutoMapperAnyDataType) -> 'AutoMapper':
        from spark_auto_mapper.automappers.columns import AutoMapperColumns
        # To work around protected keywords as column names, allow a trailing underscore in the definition that gets
        # stripped at registration time.
        col_spec = {
            column_name[:-1] if column_name and column_name != '_'
            and column_name.endswith("_") else column_name: column_def
            for column_name, column_def in kwargs.items()
        }
        columns_mapper: AutoMapperColumns = AutoMapperColumns(**col_spec)
        for column_name, child_mapper in columns_mapper.mappers.items():
            self.register_child(dst_column=column_name, child=child_mapper)
        return self

    # noinspection PyPep8Naming,PyMethodMayBeStatic
    def complex(self, entity: AutoMapperDataTypeComplexBase) -> 'AutoMapper':
        resource_mapper: AutoMapperWithComplex = AutoMapperWithComplex(
            entity=entity,
            use_schema=self.use_schema,
            include_extension=self.include_extension,
            include_null_properties=self.include_null_properties,
            skip_schema_validation=self.skip_schema_validation,
            skip_if_columns_null_or_empty=self.skip_if_columns_null_or_empty
        )

        for column_name, child_mapper in resource_mapper.mappers.items():
            self.register_child(dst_column=column_name, child=child_mapper)
        return self

    def __repr__(self) -> str:
        """
        Display for debugger

        :return: string representation for debugger
        """
        return self.to_debug_string()

    def to_debug_string(self, source_df: Optional[DataFrame] = None) -> str:
        """
        Displays the automapper as a string

        :param source_df: (Optional) source data frame
        :return: string representation
        """
        column_specs: Dict[str, Column] = self.get_column_specs(
            source_df=source_df
        )
        output: str = f"view={self.view}\n" if self.view else ""
        for column_name, column_spec in column_specs.items():
            output += ColumnSpecWrapper(column_spec).to_debug_string()

        return output

    @property
    def column_specs(self) -> Dict[str, Column]:
        """
        Useful to show in debugger

        :return dictionary of column specs
        """
        return {
            column_name: ColumnSpecWrapper(column_spec).column_spec
            for column_name, column_spec in
            self.get_column_specs(source_df=None).items()
        }
