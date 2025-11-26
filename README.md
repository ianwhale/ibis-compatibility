# Ibis compatibility checker

The purpose of this package is to determine which backends a give Ibis expression could run on.

See [here](https://ibis-project.org/) for more on Ibis.

## Quickstart

To run a quick example, first install:

``` bash
uv pip install git+https://github.com/ianwhale/ibis-compatibility.git ibis-framework[duckdb,examples]
```

Then you can run something like:

``` python
import ibis
from ibis import _
from ibis_compatibility import Checker


t = ibis.examples.penguins.fetch()

expr = (
    t.join(t, ["species"], how="left_semi")
    .filter(_.species != "Adelie")
    .group_by(["species", "island"])
    .aggregate(avg_bill_length=_.bill_length_mm.mean())
    .order_by(_.avg_bill_length.desc())
)

result = Checker().compatible_backends(expr)

print(result.backends)
```

Which will output:

```
['athena', 'bigquery', 'clickhouse', 'databricks', 'datafusion', 'druid', 'duckdb', 'exasol', 'flink', 'impala', 'mssql', 'mysql', 'oracle', 'postgres', 'pyspark', 'risingwave', 'snowflake', 'sqlite', 'trino']
```

We can also output which operations restrict certain backends with: 


``` python
print(result.restricted_operations)
```

Which will output:

```
{'Sort': ['polars']}
```

Meaning, the `order_by` call has restricted our expression's compatibility with the `polars` backend.
