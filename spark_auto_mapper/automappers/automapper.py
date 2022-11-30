import os
from logging import Logger, getLogger, StreamHandler, Formatter
from pathlib import Path
from sys import stderr
from typing import List, Optional, Union, Dict, Any

# noinspection PyPackageRequirements
from pyspark.sql import DataFrame, Column

# noinspection PyPackageRequirements
from pyspark.sql.functions import col

# noinspection PyPackageRequirements
from pyspark.sql.functions import monotonically_increasing_id

# noinspection PyPackageRequirements
from pyspark.sql.types import StructField

# noinspection PyPackageRequirements
from pyspark.sql.utils import AnalysisException

from spark_auto_mapper.automappers.automapper_analysis_exception import (
    AutoMapperAnalysisException,
)
from spark_auto_mapper.automappers.automapper_base import AutoMapperBase
from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult
from spark_auto_mapper.automappers.column_spec_wrapper import ColumnSpecWrapper
from spark_auto_mapper.automappers.complex import AutoMapperWithComplex
from spark_auto_mapper.automappers.container import AutoMapperContainer
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn
from spark_auto_mapper.data_types.complex.complex_base import (
    AutoMapperDataTypeComplexBase,
)
from spark_auto_mapper.helpers.spark_helpers import SparkHelpers
from spark_auto_mapper.type_definitions.defined_types import AutoMapperAnyDataType

_TEMPORARY_KEY = "__row_id"


class AutoMapper(AutoMapperContainer):
    """
    Main AutoMapper Class
    """

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
        logger: Optional[Logger] = None,
        check_schema_for_all_columns: bool = False,
        copy_all_unmapped_properties: bool = False,
        copy_all_unmapped_properties_exclude: Optional[List[str]] = None,
        log_level: Optional[Union[int, str]] = None,
        enable_schema_pruning: bool = False,
        remove_duplicates_by_columns: Optional[List[str]] = None,
        extension_fields: Optional[List[str]] = None,
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
        :param extension_fields: fields to enable in extensions
        :param include_null_properties: If you want to include null properties
        :param use_single_select: This is a faster way to run the AutoMapper since it will select
                all the columns at once.
                However, this makes it harder to debug since you don't know what column failed
        :param verify_row_count: verifies that the count of rows remains the same before and after the transformation
        :param skip_schema_validation: skip schema checks on these columns
        :param skip_if_columns_null_or_empty: skip creating the record if any of these columns are null or empty
        :param keep_null_rows: whether to keep the null rows instead of removing them
        :param filter_by: (Optional) SQL expression that is used to filter
        :param copy_all_unmapped_properties: copy any property that is not explicitly mapped
        :param copy_all_unmapped_properties_exclude: exclude these columns when copy_all_unmapped_properties is set
        :param logger: logger used to log informational messages
        :param enable_schema_pruning: remove properties from schema that are not present in the automapper
        :param remove_duplicates_by_columns: remove duplicate rows where the value of these columns match
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
            List[str]
        ] = skip_if_columns_null_or_empty
        self.keep_null_rows: bool = keep_null_rows
        self.filter_by: Optional[str] = filter_by
        self.logger: Logger = logger  # type: ignore
        self.log_level: Optional[Union[int, str]] = log_level or os.environ.get(
            "LOGLEVEL"
        )
        if not self.logger:
            self.logger = getLogger(__name__)
            if self.log_level is not None:
                self.logger.setLevel(self.log_level)
            if self.logger.handlers:
                pass
            else:
                stream_handler: StreamHandler = StreamHandler(stderr)  # type: ignore
                if log_level:
                    stream_handler.setLevel(level=log_level)
                # noinspection SpellCheckingInspection
                formatter: Formatter = Formatter(
                    "%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(lineno)d - %(funcName)s: %(message)s"
                )
                stream_handler.setFormatter(formatter)
                self.logger.addHandler(stream_handler)

        self.check_schema_for_all_columns: bool = check_schema_for_all_columns
        self.copy_all_unmapped_properties: bool = copy_all_unmapped_properties
        self.copy_all_unmapped_properties_exclude: Optional[
            List[str]
        ] = copy_all_unmapped_properties_exclude

        self.entity_name: Optional[str] = None
        self.enable_schema_pruning: bool = enable_schema_pruning
        if enable_schema_pruning:
            self.skip_schema_validation = []  # no need to filter out extensions
        self.remove_duplicates_by_columns: Optional[
            List[str]
        ] = remove_duplicates_by_columns
        self.extension_fields: Optional[List[str]] = extension_fields

    def _transform_with_data_frame_single_select(
        self, df: DataFrame, source_df: DataFrame, keys: List[str]
    ) -> DataFrame:
        """
        This internal function transforms the data frame using the mappings in a single select command


        :param df: destination data frame
        :param source_df: source data frame
        :param keys: key columns
        """
        # get all the column specs
        column_specs: List[Column] = [
            child_mapper.get_column_specs(source_df=source_df)[column_name]
            for column_name, child_mapper in self.mappers.items()
        ]

        try:
            # print("========== NEW 2 ==============")
            if self.copy_all_unmapped_properties:
                # find all source properties not mapped
                source_properties: List[str] = source_df.columns
                mapped_properties: List[str] = list(self.mappers.keys())
                unmapped_properties: List[str] = [
                    p for p in source_properties if p not in mapped_properties
                ]
                copy_all_unmapped_properties_exclude: List[str] = (
                    self.copy_all_unmapped_properties_exclude or []
                )
                column_schema: Dict[str, StructField] = (
                    {f.name: f for f in source_df.schema} if self.use_schema else {}
                )

                # for each unmapped property add a simple A.column()
                column_specs.extend(
                    [
                        AutoMapperDataTypeColumn(column_name)
                        .get_column_spec(
                            source_df=source_df,
                            current_column=None,
                            parent_columns=None,
                        )
                        .cast(column_schema[column_name].dataType)
                        if column_name in column_schema
                        else AutoMapperDataTypeColumn(column_name).get_column_spec(
                            source_df=source_df,
                            current_column=None,
                            parent_columns=None,
                        )
                        for column_name in unmapped_properties
                        if column_name not in copy_all_unmapped_properties_exclude
                    ]
                )

            if not self.drop_key_columns:
                column_specs = [col(f"b.{c}") for c in keys] + column_specs

            if self.log_level and self.log_level == "DEBUG":
                self.logger.debug(
                    f"-------- automapper ({self.view}) column specs ------"
                )
                self.logger.debug(self.to_debug_string(source_df=source_df))
                self.logger.debug(
                    f"-------- end automapper ({self.view}) column specs ------"
                )
                self.logger.debug(
                    f"-------- automapper ({self.source_view}) source_df schema ------"
                )
                # noinspection PyProtectedMember
                self.logger.debug(source_df._jdf.schema().treeString())
                self.logger.debug(
                    f"-------- end automapper ({self.source_view}) source_df schema ------"
                )

            if self.check_schema_for_all_columns:
                for column_name, mapper in self.mappers.items():
                    check_schema_result: Optional[
                        CheckSchemaResult
                    ] = mapper.check_schema(parent_column=None, source_df=source_df)
                    if (
                        check_schema_result
                        and len(check_schema_result.result.errors) > 0
                    ):
                        self.logger.info(
                            f"==== Schema Mismatch [{column_name}] ==="
                            f"{str(check_schema_result)}"
                        )
                    else:
                        self.logger.debug(f"==== Schema Matches: [{column_name}] ====")

            # run all the selects
            df = source_df.alias("b").select(*column_specs)

            if self.log_level and self.log_level == "DEBUG":
                print(
                    f"------------  Start Execution Plan for view {self.view} -----------"
                )
                df.explain(extended="cost")
                print("------------  End Execution Plan -----------")
            # write out final checkpoint for this automapper
            if self.checkpoint_path:
                checkpoint_path = (
                    Path(self.checkpoint_path)
                    .joinpath(self.view or "df")
                    .joinpath("final")
                )
                df.write.parquet(str(checkpoint_path))
                df = df.sql_ctx.read.parquet(str(checkpoint_path))
        except (AnalysisException, ValueError):
            self.logger.warning(
                f"-------- automapper ({self.view}) column specs ------"
            )
            self.logger.warning(self.to_debug_string(source_df=source_df))
            self.logger.warning(
                f"-------- end automapper ({self.view}) column specs ------"
            )
            if self.log_level and self.log_level == "DEBUG":
                self.logger.debug(
                    f"-------- automapper ({self.source_view}) source_df schema ------"
                )
                # noinspection PyProtectedMember
                self.logger.debug(source_df._jdf.schema().treeString())
                self.logger.debug(
                    f"-------- end automapper ({self.source_view}) source_df schema ------"
                )
            # iterate through each column to find the problem child
            for column_name, mapper in self.mappers.items():
                try:
                    self.logger.debug(
                        f"========= Processing {column_name} =========== "
                    )
                    column_spec = mapper.get_column_specs(source_df=source_df)[
                        column_name
                    ]
                    source_df.alias("b").select(column_spec).limit(1).count()
                    self.logger.debug(
                        f"========= Done Processing {column_name} =========== "
                    )
                except (AnalysisException, ValueError) as e2:
                    self.logger.error(
                        f"=========  Processing {column_name} FAILED =========== "
                    )
                    column_spec = mapper.get_column_specs(source_df=source_df)[
                        column_name
                    ]
                    self.logger.warning(
                        ColumnSpecWrapper(column_spec).to_debug_string()
                    )
                    self.logger.error(
                        f"========= checking schema for failed column {column_name} =========== "
                    )
                    check_schema_result = mapper.check_schema(
                        parent_column=None, source_df=source_df
                    )
                    msg: str = ""
                    if isinstance(e2, AnalysisException) and e2.desc.startswith(
                        "cannot resolve 'array"
                    ):
                        msg = (
                            "Looks like the elements of the array have different structures.  "
                            "All items in an array should have the exact same structure.  "
                            "You can pass in include_nulls to AutoMapperDataTypeComplexBase to force it to create "
                            "null values for each element in the structure. \n"
                        )
                        # find the data types of each item in the list
                    column_values: Optional[List[Any]] = None
                    # This can cause GC overhead limit reached error obfuscating the actual error
                    # try:
                    #     # get column value in first row
                    #     column_values: Optional[List[Any]] = (
                    #         [
                    #             row.asDict(recursive=True)[column_name]
                    #             for row in source_df.select(column_name)
                    #             .limit(5)
                    #             .collect()
                    #         ]
                    #         if bool(source_df.head(1))  # df is not empty
                    #         else None
                    #     )
                    # except Exception as e3:
                    #     print(e3)
                    #     column_values = None

                    msg += self._get_message_for_exception(
                        column_name=column_name,
                        check_schema_result=check_schema_result,
                        df=df,
                        e=e2,
                        source_df=source_df,
                        column_values=column_values,
                    )
                    raise AutoMapperAnalysisException(
                        automapper_name=self.view,
                        msg=msg,
                        column_name=column_name,
                        check_schema_result=check_schema_result,
                        column_values=column_values,
                    ) from e2
        except Exception as e:
            self.logger.error("====  OOPS ===========")
            msg = self._get_message_for_exception(
                column_name="",
                check_schema_result=None,
                df=df,
                e=e,
                source_df=source_df,
                column_values=None,
            )
            raise Exception(msg) from e

        self.logger.debug(f"========= Finished AutoMapper {self.view} =========== ")
        return df

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def transform_with_data_frame(
        self, df: DataFrame, source_df: Optional[DataFrame], keys: List[str]
    ) -> DataFrame:
        """
        Internal function called by base class to transform the data frame


        :param df: destination data frame
        :param source_df: source data frame
        :param keys: key columns
        :return data frame after the transform
        """
        current_child_number: int = 0
        # iterate over each child mapper and run it
        for column_name, child_mapper in self.mappers.items():
            current_child_number += 1
            try:
                # checkpoint if specified
                # https://livebook.manning.com/book/spark-in-action-second-edition/16-cache-and-checkpoint-enhancing-spark-s-performances/v-14/43
                if self.checkpoint_after_columns:
                    if (current_child_number % self.checkpoint_after_columns) == 0:
                        if not self.checkpoint_path:
                            df = df.checkpoint(eager=True)
                        else:
                            checkpoint_path: Path = (
                                Path(self.checkpoint_path)
                                .joinpath(self.view or "df")
                                .joinpath(str(current_child_number))
                            )
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
                    msg = (
                        "Looks like the elements of the array have different structures.  "
                        "All items in an array should have the exact same structure.  "
                        "You can pass in include_nulls to AutoMapperDataTypeComplexBase to force it to create "
                        "null values for each element in the structure. "
                    )
                if source_df is not None:
                    msg += self._get_message_for_exception(
                        column_name=column_name,
                        check_schema_result=None,
                        df=df,
                        e=e,
                        source_df=source_df,
                        column_values=None,
                    )
                raise Exception(msg) from e
            except Exception as e:
                msg = (
                    self._get_message_for_exception(
                        column_name=column_name,
                        check_schema_result=None,
                        df=df,
                        e=e,
                        source_df=source_df,
                        column_values=None,
                    )
                    if source_df is not None
                    else ""
                )
                raise Exception(msg) from e

        # write out final checkpoint for this automapper
        if self.checkpoint_path:
            checkpoint_path = (
                Path(self.checkpoint_path).joinpath(self.view or "df").joinpath("final")
            )
            df.write.parquet(str(checkpoint_path))
            df = df.sql_ctx.read.parquet(str(checkpoint_path))

        return df

    def _get_message_for_exception(
        self,
        *,
        column_name: str,
        check_schema_result: Optional[CheckSchemaResult],
        df: DataFrame,
        e: Exception,
        source_df: DataFrame,
        column_values: Optional[List[Any]],
    ) -> str:
        """
        This internal function create a message string for the given exception


        :param column_name: name of column
        :param df: destination data frame
        :param e: exception
        :param source_df: source data frame
        :param column_values: (Optional) values of the columns
        """
        # write out the full list of columns
        columns_in_source: List[str] = list(source_df.columns)
        columns_in_destination: List[str] = list(df.columns)
        msg: str = ""
        if column_name is not None:
            msg += f", Processing column:[{column_name}]\n"
        if check_schema_result is not None:
            msg += check_schema_result.to_string(
                include_info=True
                if self.log_level and self.log_level == "DEBUG"
                else False
            )
            msg += "\n"
        if column_values is not None:
            msg += f"column values: [{','.join(str(column_values))}]"
        msg += str(e)
        msg += f", Source columns:[{','.join(columns_in_source)}]"
        msg += f", Destination columns:[{','.join(columns_in_destination)}]"
        return msg

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Uses this AutoMapper to transform the specified data frame and return the new data frame


        :param df: source data frame
        :returns destination data frame
        """
        # if source_view is specified then load that else assume that df is the source view
        source_df: DataFrame = (
            df.sql_ctx.table(self.source_view) if self.source_view else df
        )
        # if keys are not specified then add a __row_id column to both the source and the destination
        #   and use that as key
        if not self.keys or len(self.keys) == 0:
            assert not self.view or not SparkHelpers.spark_table_exists(
                sql_ctx=df.sql_ctx, view=self.view
            ), f"View {self.view} already exists"
            if self.use_single_select:
                self.keys = []
            else:
                self.keys = [_TEMPORARY_KEY]
                source_df = source_df.withColumn(
                    _TEMPORARY_KEY, monotonically_increasing_id()
                )

        # if view is specified then check if it exists
        destination_df: DataFrame = (
            df.sql_ctx.table(self.view)
            if self.view
            and self.reuse_existing_view
            and SparkHelpers.spark_table_exists(sql_ctx=df.sql_ctx, view=self.view)
            else source_df.select(self.keys)
        )

        # if filter is specified then run it
        if self.filter_by is not None:
            source_df = source_df.where(self.filter_by)

        # rename key columns to avoid name clash if someone creates a column with that name
        renamed_key_columns: List[str] = []
        for key in self.keys:
            renamed_key_column: str = f"__{key}"
            source_df = source_df.withColumn(renamed_key_column, col(key))
            try:
                destination_df = destination_df.withColumn(renamed_key_column, col(key))
            except Exception as e:
                # write out the full list of columns
                columns_in_source: List[str] = list(source_df.columns)
                columns_in_destination: List[str] = list(destination_df.columns)
                msg: str = str(e)
                msg += f", Source columns:[{','.join(columns_in_source)}]"
                msg += f", Destination columns:[{','.join(columns_in_destination)}]"
                raise Exception(msg) from e
            renamed_key_columns.append(renamed_key_column)

        # run the mapper
        if self.use_single_select:
            result_df: DataFrame = self._transform_with_data_frame_single_select(
                df=destination_df, source_df=source_df, keys=self.keys
            )
        else:
            result_df = self.transform_with_data_frame(
                df=destination_df, source_df=source_df, keys=renamed_key_columns
            )

        # now drop the __row_id if we added it
        result_df = result_df.drop(_TEMPORARY_KEY)

        # drop the key columns
        if self.drop_key_columns:
            result_df = result_df.drop(*self.keys)

        # drop the renamed key columns
        result_df = result_df.drop(*renamed_key_columns)

        # replace any columns we had prepended with "___" to avoid a name clash with key columns
        for column_name in [c for c in result_df.columns if c.startswith("___")]:
            result_df = result_df.withColumnRenamed(
                column_name, column_name.replace("___", "")
            )

        # drop any rows where all values are null
        if not self.keep_null_rows:
            result_df = result_df.dropna(how="all")

        # remove duplicates
        if not self.keep_duplicates:
            result_df = result_df.drop_duplicates()

        if (
            self.remove_duplicates_by_columns
            and len(self.remove_duplicates_by_columns) > 0
        ):
            result_df = result_df.drop_duplicates(self.remove_duplicates_by_columns)

        # if view was specified then create that view
        if self.view:
            result_df.createOrReplaceTempView(self.view)
        return result_df

    def _register_child(self, dst_column: str, child: "AutoMapperBase") -> None:
        """
        Adds a new child mapper to this AutoMapper


        :param dst_column: column name
        :param child: child mapper to register
        """
        self.mappers[dst_column] = child

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def columns(self, **kwargs: AutoMapperAnyDataType) -> "AutoMapper":
        """
        Adds mappings for columns

        :example: mapper = AutoMapper(
                view="members",
                source_view="patients",
                keys=["member_id"],
                drop_key_columns=False,
            ).columns(
                dst1="src1",
                dst2=AutoMapperList(["address1"]),
                dst3=AutoMapperList(["address1", "address2"]),
                dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))]),
            )

        :param kwargs: A dictionary of mappings
        :return: The same AutoMapper
        """

        from spark_auto_mapper.automappers.columns import AutoMapperColumns

        # To work around protected keywords as column names, allow a trailing underscore in the definition that gets
        # stripped at registration time.
        col_spec = {
            column_name[:-1]
            if column_name and column_name != "_" and column_name.endswith("_")
            else column_name: column_def
            for column_name, column_def in kwargs.items()
        }
        columns_mapper: AutoMapperColumns = AutoMapperColumns(**col_spec)
        for column_name, child_mapper in columns_mapper.mappers.items():
            self._register_child(dst_column=column_name, child=child_mapper)
        return self

    # noinspection PyPep8Naming,PyMethodMayBeStatic
    def complex(self, entity: AutoMapperDataTypeComplexBase) -> "AutoMapper":
        """
        Adds mappings for an entity

        :example: mapper = AutoMapper(
                view="members",
                source_view="patients",
                keys=["member_id"],
                drop_key_columns=False,
            ).complex(
                MyClass(
                    name=A.column("last_name"),
                    age=A.number(A.column("my_age"))
                )
            )

        :param entity: An AutoMapper type
        :return: The same AutoMapper
        """

        resource_mapper: AutoMapperWithComplex = AutoMapperWithComplex(
            entity=entity,
            use_schema=self.use_schema,
            include_extension=self.include_extension,
            include_null_properties=self.include_null_properties,
            skip_schema_validation=self.skip_schema_validation,
            skip_if_columns_null_or_empty=self.skip_if_columns_null_or_empty,
            enable_schema_pruning=self.enable_schema_pruning,
            extension_fields=self.extension_fields,
        )

        self.entity_name = entity.__class__.__name__
        for column_name, child_mapper in resource_mapper.mappers.items():
            self._register_child(dst_column=column_name, child=child_mapper)
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
        column_specs: Dict[str, Column] = self.get_column_specs(source_df=source_df)
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
            for column_name, column_spec in self.get_column_specs(
                source_df=None
            ).items()
        }

    def __str__(self) -> str:
        return f"AutoMapper: view={self.view} for entity={self.entity_name}"
