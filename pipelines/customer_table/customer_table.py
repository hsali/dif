# SOURCE : csv, json, jdbc, parquet, avro, orc, delta, hudi, kudu, kafka, elastic, cassandra, hbase, mongodb, redis, s3, azure_blob, gcs, bigquery, snowflake, redshift, athena, dremio, drill, presto, hive, impala, teradata, db2, mssql, mysql, oracle, postgres, sqlite, h2, firebird, clickhouse, vertica, greenplum, netezza, sybase, informix, salesforce, zendesk, jira, servicenow, hubspot, pipelinewise, singer, dbt

# support multiple targets (e.g. database, file, cloud storage, etc.)

# SINK : csv, json, jdbc, parquet, avro, orc, delta, hudi, kudu, kafka, elastic, cassandra, hbase, mongodb, redis, s3, azure_blob, gcs, bigquery, snowflake, redshift, athena, dremio, drill, presto, hive, impala, teradata, db2, mssql, mysql, oracle, postgres, sqlite, h2, firebird, clickhouse, vertica, greenplum, netezza, sybase, informix, salesforce, zendesk, jira, servicenow, hubspot, pipelinewise, singer, dbt


def extract1(spark, source):
    if source["type"] == "csv":
        return spark.read.csv(source["path"], header=True, inferSchema=True)
    elif source["type"] == "json":
        return spark.read.json(source["path"])
    elif source["type"] == "jdbc":
        return (
            spark.read.format("jdbc")
            .options(
                url=source["url"],
                dbtable=source["table"],
                user=source["user"],
                password=source["password"],
            )
            .load()
        )
    else:
        raise ValueError("Unsupported source type")


def extract():
    print("extract")


def transform(data, transformations):
    for transformation in transformations:
        if transformation["type"] == "filter":
            pass
        elif transformation["type"] == "map":
            pass
        else:
            raise ValueError("Unsupported transformation type")


def load(data, sink, spark=None):
    if sink["type"] == "csv":
        data.write.csv(sink["path"], header=True)
    elif sink["type"] == "json":
        data.write.json(sink["path"])
    elif sink["type"] == "jdbc":
        data.write.format("jdbc").options(
            url=sink["url"],
            dbtable=sink["table"],
            user=sink["user"],
            password=sink["password"],
        ).save()
    else:
        raise ValueError("Unsupported sink type")


etl_config = {
    "job1": {
        "source": {
            "type": "csv",
            "default_option_template": "csv",  # will ignore template if custom options provided.
            "options": {"path": "/path/to/source.csv", "delimiter": ","},
            "sql": "select * from customer",
        },
        "sink": {
            "type": "jdbc",
            "options": {
                "url": "jdbc:postgresql://localhost:5432/mydb",
                "dbtable": "customer",
            },
        },
        "extract_callback": extract,
        "transform_callback": transform,
        "load_callback": load,  # list of callback
    },
    "job2": {
        "source": {
            "type": "spark_sql",
            "sql": "select * from customer",
            "options": {
                "database": "shopify",
                "table": "customer",
                "user": "user",
                "password": "password",
                "url": "jdbc:postgresql://localhost:5432/mydb",
            },
        },
        "sink": {
            "type": "jdbc",
            "connection": "shopify_abc1",
            "database_type": "mysql",  # postgress, oracle, mssql
            "options": {
                "url": "jdbc:postgresql://localhost:5432/mydb",
                "dbtable": "customer",
            },
        },
    },
}
