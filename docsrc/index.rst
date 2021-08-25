.. Helix Project documentation master file, created by
   sphinx-quickstart on Thu Mar 25 11:58:19 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Spark AutoMapper
=================
Fluent API to map data from one view to another in Apache Spark.

Uses native Spark functions underneath so it is just as fast as hand writing the transformations.

Since this is just Python, you can use any Python editor. Everything is typed using Python typings so most editors will auto-complete and warn you when you do something wrong.

`SparkAutoMapper Github Repo <https://github.com/icanbwell/SparkAutoMapper>`_

Typical Usage
==================
1. First create an AutoMapper (:class:`~spark_auto_mapper.automappers.automapper.AutoMapper`).  The two required parameters are:

   a. source_view: The Spark view to read the source data from
   b. view: The destination Spark view to store the data.  This view should not already exist.

2. Create the mappings.  There are two kind of mappings:

   a. Complex (:meth:`~spark_auto_mapper.automappers.automapper.AutoMapper.complex`): Using strongly typed classes e.g., FHIR resources.  This is the preferred method as it provides syntax checking and auto-complete
   b. Columns (:meth:`~spark_auto_mapper.automappers.automapper.AutoMapper.columns`): You can define arbitrary columns in destination view.

3. In each mapping, access a column in the source view by using A.column("column_name") (:meth:`~spark_auto_mapper.helpers.automapper_helpers.AutoMapperHelpers.column`) or specify a literal value with A.text("apple") (:meth:`~spark_auto_mapper.helpers.automapper_helpers.AutoMapperHelpers.text`).

4. Add transformation functions to A.column("column_name") to transform the column. (:class:`~spark_auto_mapper.data_types.data_type_base.AutoMapperDataTypeBase`)

Example
========
AutoMapper with Strongly typed classes (Recommended)

.. code-block:: python

   mapper = AutoMapper(
        view="members",
        source_view="patients"
   ).complex(
      MyClass(
         name=A.column("last_name"),
         age=A.column("my_age").to_number()
      )
   )

The strongly typed class can be created as below (the constructor accepts whatever parameters you want and the class must implement the get_schema() method):

.. code-block:: python

   class MyClass(AutoMapperDataTypeComplexBase):
       def __init__(
           self, name: AutoMapperTextLikeBase, age: AutoMapperNumberDataType
       ) -> None:
           super().__init__(name=name, age=age)

       def get_schema(
           self, include_extension: bool
       ) -> Optional[Union[StructType, DataType]]:
           schema: StructType = StructType(
               [
                   StructField("name", StringType(), False),
                   StructField("age", LongType(), True),
               ]
           )
           return schema

AutoMapper with just columns

.. code-block:: python

    mapper = AutoMapper(
        view="members",
        source_view="patients",
    ).columns(
        dst1=A.column("src1"),
        dst2=AutoMapperList(["address1"]),
        dst3=AutoMapperList(["address1", "address2"]),
        dst4=AutoMapperList([A.complex(use="usual", family=A.column("last_name"))]),
    )

Running an AutoMapper
=====================
An AutoMapper is a `Spark Transformer <https://spark.apache.org/docs/latest/ml-pipeline.html#transformers>`_.  So you can call the transform function on an AutoMapper and pass in a dataframe.

.. code-block:: python

   result_df: DataFrame = mapper.transform(df=df)

NOTE: AutoMapper ignores the dataframe passed in since it uses views. (This allows more flexibility)

A view can be created in Spark by calling the `createOrReplaceTempView <https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html>`_ function.

.. code-block:: python

   df.createOrReplaceTempView("patients")

For example, you can create a view:

.. code-block:: python

    spark_session.createDataFrame(
        [
            (1, "Qureshi", "Imran", 45),
            (2, "Vidal", "Michael", 35),
        ],
        ["member_id", "last_name", "first_name", "my_age"],
    ).createOrReplaceTempView("patients")


Contents:
==================
.. toctree::
   :maxdepth: 6
   :titlesonly:


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
