[![Build and Test](https://github.com/imranq2/SparkAutoMapper/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/imranq2/SparkAutoMapper/actions/workflows/build_and_test.yml)

[![Upload Python Package](https://github.com/imranq2/SparkAutoMapper/actions/workflows/python-publish.yml/badge.svg)](https://github.com/imranq2/SparkAutoMapper/actions/workflows/python-publish.yml)

[![Known Vulnerabilities](https://snyk.io/test/github/imranq2/SparkAutoMapper/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/imranq2/SparkAutoMapper?targetFile=requirements.txt)

# SparkAutoMapper
Fluent API to map data from one view to another in Spark.  

Uses native Spark functions underneath so it is just as fast as hand writing the transformations.

Since this is just Python, you can use any Python editor.  Since everything is typed using Python typings, most editors will auto-complete and warn you when you do something wrong

## Usage
```shell script
pip install sparkautomapper
```

## Documentation
https://icanbwell.github.io/SparkAutoMapper/

## Local development setup
`spark.Dockerfile` pulls the `helix.spark` base image from b.well's **private**
services ECR (`856965016623.dkr.ecr.us-east-1.amazonaws.com`), per
[CIE-8032](https://icanbwell.atlassian.net/browse/CIE-8032). Before running
`make up`, `make build`, `make devdocker` or `make tests`, authenticate once per
session:

```shell script
aws sso login --profile services
```

The docker targets then run `make ecr-login` automatically. You can also run it
directly, and override the profile if yours is named differently:

```shell script
make ecr-login AWS_SERVICES_PROFILE=my-profile
```

Note that b.well employees with access to the services AWS account are required
to build the test/dev images. `pip install sparkautomapper` and `make run-pre-commit`
do **not** require AWS access.

## SparkAutoMapper input and output
You can pass either a dataframe to SparkAutoMapper or specify the name of a Spark view to read from.

You can receive the result as a dataframe or (optionally) pass in the name of a view where you want the result.

## Dynamic Typing Examples
#### Set a column in destination to a text value (read from pass in data frame and return the result in a new dataframe)
Set a column in destination to a text value
```python
from spark_auto_mapper.automappers.automapper import AutoMapper

mapper = AutoMapper(
    keys=["member_id"]
).columns(
    dst1="hello"
)
```

#### Set a column in destination to a text value (read from a Spark view and put result in another Spark view)
Set a column in destination to a text value
```python
from spark_auto_mapper.automappers.automapper import AutoMapper

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst1="hello"
)
```

#### Set a column in destination to an int value
Set a column in destination to a text value
```python
from spark_auto_mapper.automappers.automapper import AutoMapper

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst1=1050
)
```

#### Copy a column (src1) from source_view to destination view column (dst1)
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst1=A.column("src1")
)
```
Or you can use the shortcut for specifying a column (wrap column name in [])
```python
from spark_auto_mapper.automappers.automapper import AutoMapper

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst1="[src1]"
)
```

#### Convert data type for a column (or string literal)
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    birthDate=A.date(A.column("date_of_birth"))
)
```

#### Use a Spark SQL Expression (Any valid Spark SQL expression can be used)
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    gender=A.expression(
    """
    CASE
        WHEN `Member Sex` = 'F' THEN 'female'
        WHEN `Member Sex` = 'M' THEN 'male'
        ELSE 'other'
    END
    """
    )
)
```

#### Specify multiple transformations
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst1="[src1]",
    birthDate=A.date("[date_of_birth]"),
    gender=A.expression(
                """
    CASE
        WHEN `Member Sex` = 'F' THEN 'female'
        WHEN `Member Sex` = 'M' THEN 'male'
        ELSE 'other'
    END
    """
    )
)
```

#### Use variables or parameters
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

def mapping(parameters: dict):
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"]
    ).columns(
        dst1=A.column(parameters["my_column_name"])
    )
```

#### Use conditional logic
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A

def mapping(parameters: dict):
    mapper = AutoMapper(
        view="members",
        source_view="patients",
        keys=["member_id"]
    ).columns(
        dst1=A.column(parameters["my_column_name"])
    )
    
    if parameters["customer"] == "Microsoft":
        mapper = mapper.columns(
            important_customer=1,
            customer_name=parameters["customer"]
        )
    return mapper
```

#### Using nested array columns
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).withColumn(
    dst2=A.list(
        [
            "address1",
            "address2"
        ]
    )
)
```

#### Using nested struct columns
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst2=A.complex(
        use="usual",
        family="imran"
    )
)
```

#### Using lists of structs
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
mapper = AutoMapper(
    view="members",
    source_view="patients",
    keys=["member_id"]
).columns(
    dst2=A.list(
        [
            A.complex(
                use="usual",
                family="imran"
            ),
            A.complex(
                use="usual",
                family="[last_name]"
            )
        ]
    )
)
```

## Executing the AutoMapper 
```python
spark.createDataFrame(
    [
        (1, 'Qureshi', 'Imran'),
        (2, 'Vidal', 'Michael'),
    ],
    ['member_id', 'last_name', 'first_name']
).createOrReplaceTempView("patients")

source_df: DataFrame = spark.table("patients")

df = source_df.select("member_id")
df.createOrReplaceTempView("members")

result_df: DataFrame = mapper.transform(df=df)
```

## Statically Typed Examples
To improve the auto-complete and syntax checking even more, you can define Complex types:

Define a custom data type:
```python
from spark_auto_mapper.type_definitions.automapper_defined_types import AutoMapperTextInputType
from spark_auto_mapper.helpers.automapper_value_parser import AutoMapperValueParser
from spark_auto_mapper.data_types.date import AutoMapperDateDataType
from spark_auto_mapper.data_types.list import AutoMapperList
from spark_auto_mapper_fhir.fhir_types.automapper_fhir_data_type_complex_base import AutoMapperFhirDataTypeComplexBase


class AutoMapperFhirDataTypePatient(AutoMapperFhirDataTypeComplexBase):
    # noinspection PyPep8Naming
    def __init__(self,
                 id_: AutoMapperTextInputType,
                 birthDate: AutoMapperDateDataType,
                 name: AutoMapperList,
                 gender: AutoMapperTextInputType
                 ) -> None:
        super().__init__()
        self.value = dict(
            id=AutoMapperValueParser.parse_value(id_),
            birthDate=AutoMapperValueParser.parse_value(birthDate),
            name=AutoMapperValueParser.parse_value(name),
            gender=AutoMapperValueParser.parse_value(gender)
        )

```

Now you get auto-complete and syntax checking:
```python
from spark_auto_mapper.automappers.automapper import AutoMapper
from spark_auto_mapper.helpers.automapper_helpers import AutoMapperHelpers as A
mapper = AutoMapperFhir(
    view="members",
    source_view="patients",
    keys=["member_id"]
).withResource(
    resource=F.patient(
        id_=A.column("a.member_id"),
        birthDate=A.date(
            A.column("date_of_birth")
        ),
        name=A.list(
            F.human_name(
                use="usual",
                family=A.column("last_name")
            )
        ),
        gender="female"
    )
)
```

# Publishing a new package
1. Edit VERSION to increment the version
2. Create a new release
3. The GitHub Action should automatically kick in and publish the package
4. You can see the status in the Actions tab

### Prerequisite: these base-image tags must exist in the services ECR

`helix.spark`'s publish workflows push **only to Docker Hub** — there is no ECR push step —
so the tags below do not reach `856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark`
unless someone copies them. Until they do, `docker build` here fails on a missing image.

| tag to copy | expected digest |
|---|---|
| `3.5.5.0-slim` | `sha256:e2c4762e38e3f57bfa99afdd68621c0be46eb373475c5578113c0e683b193126` |

Copy with a manifest-preserving tool. These are multi-arch (`linux/amd64` + `linux/arm64`);
a `docker pull`/`tag`/`push` cycle from an Apple-silicon Mac would push arm64 only and
silently break amd64 CI runners.

```bash
aws sso login --profile services
aws ecr get-login-password --region us-east-1 --profile services \
  | crane auth login 856965016623.dkr.ecr.us-east-1.amazonaws.com --username AWS --password-stdin
DEST=856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark
crane copy icanbwell/helix.spark:3.5.5.0-slim "$DEST:3.5.5.0-slim"
# verify:
crane digest "$DEST:3.5.5.0-slim"
```

Source is `icanbwell/helix.spark` (the icanbwell-owned namespace mandated by CIE-8032); it is
digest-identical to the old `imranq2/helix.spark`, so the copy is the same image bytes.

The tag itself is **derived, not chosen**: `A.B.C` in the tag is the Apache Spark version in
the image and must match this repo's `pyspark` pin. Do not change the tag as part of a
registry migration.

> **Note — this repo's tag is inconsistent today.** `Pipfile` pins `pyspark==3.5.1`
> while the base image is `3.5.5.0-slim` (Spark 3.5.5), and `setup.py` says `3.5.5`.
> By the pin-matches-image rule the correct tag would be `3.5.1.11-slim`, which is also
> already present in the ECR. That is a *tag* change, so it needs an owner's decision and
> is deliberately NOT made here.

See `CIE-8032` for the full decision trail. Copying these tags does NOT by itself make CI
pass — this is a public repo on `ubuntu-latest` with no AWS identity, so a GitHub OIDC
trusted role scoped to `repo:icanbwell/SparkAutoMapper:*` is still required.
