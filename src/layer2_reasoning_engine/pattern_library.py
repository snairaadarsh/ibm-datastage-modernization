"""
Layer 2: DataStage Expression Pattern Library
Maps IBM DataStage transformer expressions to PySpark / DataFusion equivalents.
This encodes the domain expertise of an experienced ETL migration engineer.
"""

# ─────────────────────────────────────────────────────────────────────────────
# STRING FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
STRING_PATTERNS = {
    # DS → PySpark
    "pyspark": {
        r"Upcase\((.+?)\)": r"upper(\1)",
        r"Downcase\((.+?)\)": r"lower(\1)",
        r"Trim\((.+?)\)": r"trim(\1)",
        r"LTrim\((.+?)\)": r"ltrim(\1)",
        r"RTrim\((.+?)\)": r"rtrim(\1)",
        r"Length\((.+?)\)": r"length(\1)",
        r"Left\((.+?),\s*(.+?)\)": r"substring(\1, 1, \2)",
        r"Right\((.+?),\s*(.+?)\)": r"substring(\1, -\2)",
        r"Mid\((.+?),\s*(.+?),\s*(.+?)\)": r"substring(\1, \2, \3)",
        r"Index\((.+?),\s*(.+?),\s*(.+?)\)": r"locate(\2, \1)",
        r"Field\((.+?),\s*(.+?),\s*(.+?)\)": r"split(\1, \2)[\3 - 1]",
        r"Char\((.+?)\)": r"cast(\1 as string)",
        r"Str\((.+?)\)": r"cast(\1 as string)",
        r"Num\((.+?)\)": r"cast(\1 as double)",
        r"Space\((.+?)\)": r"repeat(' ', \1)",
        r"Convert\((.+?),\s*(.+?),\s*(.+?)\)": r"translate(\3, \1, \2)",
    },
    # DS → DataFusion (Wrangler expressions)
    "datafusion": {
        r"Upcase\((.+?)\)": r"UPPER(\1)",
        r"Downcase\((.+?)\)": r"LOWER(\1)",
        r"Trim\((.+?)\)": r"TRIM(\1)",
        r"LTrim\((.+?)\)": r"LTRIM(\1)",
        r"RTrim\((.+?)\)": r"RTRIM(\1)",
        r"Length\((.+?)\)": r"LENGTH(\1)",
        r"Left\((.+?),\s*(.+?)\)": r"SUBSTR(\1, 1, \2)",
        r"Right\((.+?),\s*(.+?)\)": r"SUBSTR(\1, LENGTH(\1) - \2 + 1, \2)",
        r"Mid\((.+?),\s*(.+?),\s*(.+?)\)": r"SUBSTR(\1, \2, \3)",
        r"Char\((.+?)\)": r"CAST(\1 AS STRING)",
        r"Num\((.+?)\)": r"CAST(\1 AS DOUBLE)",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# DATE / TIME FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
DATE_PATTERNS = {
    "pyspark": {
        r"Year\((.+?)\)": r"year(\1)",
        r"Month\((.+?)\)": r"month(\1)",
        r"Day\((.+?)\)": r"dayofmonth(\1)",
        r"Hour\((.+?)\)": r"hour(\1)",
        r"Minute\((.+?)\)": r"minute(\1)",
        r"Second\((.+?)\)": r"second(\1)",
        r"DayOfWeek\((.+?)\)": r"dayofweek(\1)",
        r"WeekOfYear\((.+?)\)": r"weekofyear(\1)",
        r"Quarter\((.+?)\)": r"quarter(\1)",
        r"CurrentDate\(\)": r"current_date()",
        r"CurrentTime\(\)": r"current_timestamp()",
        r"CurrentTimestamp\(\)": r"current_timestamp()",
        r"DateFromComponents\((.+?),\s*(.+?),\s*(.+?)\)": r"make_date(\1, \2, \3)",
        r"TimestampFromComponents\((.+?),\s*(.+?),\s*(.+?),\s*(.+?),\s*(.+?),\s*(.+?)\)": r"make_timestamp(\1, \2, \3, \4, \5, \6)",
        r"DaysAfter\((.+?),\s*(.+?)\)": r"date_add(\2, \1)",
        r"DaysBefore\((.+?),\s*(.+?)\)": r"date_sub(\2, \1)",
        r"DateDiff\((.+?),\s*(.+?)\)": r"datediff(\1, \2)",
        r"MonthDiff\((.+?),\s*(.+?)\)": r"months_between(\1, \2)",
        r"DateToString\((.+?),\s*[\"'](.+?)[\"']\)": r"date_format(\1, '\2')",
        r"StringToDate\((.+?),\s*[\"'](.+?)[\"']\)": r"to_date(\1, '\2')",
    },
    "datafusion": {
        r"Year\((.+?)\)": r"YEAR(\1)",
        r"Month\((.+?)\)": r"MONTH(\1)",
        r"Day\((.+?)\)": r"DAY(\1)",
        r"CurrentDate\(\)": r"CURRENT_DATE()",
        r"CurrentTimestamp\(\)": r"CURRENT_TIMESTAMP()",
        r"DateFromComponents\((.+?),\s*(.+?),\s*(.+?)\)": r"DATE(\1, \2, \3)",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# MATH FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
MATH_PATTERNS = {
    "pyspark": {
        r"Abs\((.+?)\)": r"abs(\1)",
        r"Sqrt\((.+?)\)": r"sqrt(\1)",
        r"Round\((.+?),\s*(.+?)\)": r"round(\1, \2)",
        r"Floor\((.+?)\)": r"floor(\1)",
        r"Ceiling\((.+?)\)": r"ceil(\1)",
        r"Mod\((.+?),\s*(.+?)\)": r"(\1 % \2)",
        r"Power\((.+?),\s*(.+?)\)": r"pow(\1, \2)",
        r"Log\((.+?)\)": r"log(\1)",
        r"Exp\((.+?)\)": r"exp(\1)",
        r"Sign\((.+?)\)": r"signum(\1)",
        r"Checksum\((.+?)\)": r"hash(\1)",
        r"Random\(\)": r"rand()",
    },
    "datafusion": {
        r"Abs\((.+?)\)": r"ABS(\1)",
        r"Sqrt\((.+?)\)": r"SQRT(\1)",
        r"Round\((.+?),\s*(.+?)\)": r"ROUND(\1, \2)",
        r"Floor\((.+?)\)": r"FLOOR(\1)",
        r"Ceiling\((.+?)\)": r"CEIL(\1)",
        r"Checksum\((.+?)\)": r"HASH(\1)",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# NULL / CONDITIONAL FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
NULL_CONDITIONAL_PATTERNS = {
    "pyspark": {
        r"IsNull\((.+?)\)": r"(\1 is null)",
        r"IsNotNull\((.+?)\)": r"(\1 is not null)",
        r"IsVoid\((.+?)\)": r"(\1 is null)",
        r"Nvl\((.+?),\s*(.+?)\)": r"coalesce(\1, \2)",
        r"NullToValue\((.+?),\s*(.+?)\)": r"coalesce(\1, \2)",
    },
    "datafusion": {
        r"IsNull\((.+?)\)": r"(\1 IS NULL)",
        r"IsNotNull\((.+?)\)": r"(\1 IS NOT NULL)",
        r"Nvl\((.+?),\s*(.+?)\)": r"COALESCE(\1, \2)",
        r"NullToValue\((.+?),\s*(.+?)\)": r"COALESCE(\1, \2)",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# CONDITIONAL (If/Then/Else) — handled programmatically in reasoning_engine.py
# ─────────────────────────────────────────────────────────────────────────────
# Pattern: "If <cond> Then <val1> Else If <cond2> Then <val2> Else <val3>"
# → PySpark: when(<cond>, <val1>).when(<cond2>, <val2>).otherwise(<val3>)
# → DataFusion: CASE WHEN <cond> THEN <val1> WHEN <cond2> THEN <val2> ELSE <val3> END

# ─────────────────────────────────────────────────────────────────────────────
# JOIN TYPE MAPPINGS
# ─────────────────────────────────────────────────────────────────────────────
JOIN_TYPE_MAP = {
    "Inner":      {"pyspark": "inner",       "datafusion": "INNER"},
    "LeftOuter":  {"pyspark": "left",        "datafusion": "LEFT OUTER"},
    "RightOuter": {"pyspark": "right",       "datafusion": "RIGHT OUTER"},
    "FullOuter":  {"pyspark": "outer",       "datafusion": "FULL OUTER"},
    "Cross":      {"pyspark": "cross",       "datafusion": "CROSS"},
    "Lookup":     {"pyspark": "left",        "datafusion": "LEFT OUTER"},
}

# ─────────────────────────────────────────────────────────────────────────────
# WRITE MODE MAPPINGS (DataStage → Spark/BQ)
# ─────────────────────────────────────────────────────────────────────────────
WRITE_MODE_MAP = {
    "APPEND":    {"pyspark": "append",     "bq_mode": "WRITE_APPEND",   "datafusion": "INSERT"},
    "OVERWRITE": {"pyspark": "overwrite",  "bq_mode": "WRITE_TRUNCATE", "datafusion": "TRUNCATE_AND_INSERT"},
    "MERGE":     {"pyspark": "merge",      "bq_mode": "WRITE_APPEND",   "datafusion": "UPSERT"},
    "REPLACE":   {"pyspark": "overwrite",  "bq_mode": "WRITE_TRUNCATE", "datafusion": "TRUNCATE_AND_INSERT"},
}

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTOR → Spark reader/writer hints
# ─────────────────────────────────────────────────────────────────────────────
CONNECTOR_SPARK_MAP = {
    "PxOracleConnector":    {"format": "jdbc", "driver": "oracle.jdbc.OracleDriver", "url_prefix": "jdbc:oracle:thin:@"},
    "PxDB2Connector":       {"format": "jdbc", "driver": "com.ibm.db2.jcc.DB2Driver", "url_prefix": "jdbc:db2://"},
    "PxMSSQLConnector":     {"format": "jdbc", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "url_prefix": "jdbc:sqlserver://"},
    "PxPostgresConnector":  {"format": "jdbc", "driver": "org.postgresql.Driver", "url_prefix": "jdbc:postgresql://"},
    "PxTeradataConnector":  {"format": "jdbc", "driver": "com.teradata.jdbc.TeraDriver", "url_prefix": "jdbc:teradata://"},
    "PxSequentialFile":     {"format": "csv",  "driver": None, "url_prefix": None},
    "PxCsvConnector":       {"format": "csv",  "driver": None, "url_prefix": None},
    "PxS3Connector":        {"format": "parquet", "driver": None, "url_prefix": "s3a://"},
    "PxBigQueryConnector":  {"format": "bigquery", "driver": None, "url_prefix": None},
    "PxSnowflakeConnector": {"format": "snowflake", "driver": None, "url_prefix": None},
}

# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION FUNCTION MAPPINGS
# ─────────────────────────────────────────────────────────────────────────────
AGG_FUNCTION_MAP = {
    "pyspark": {
        "SUM":   "F.sum",
        "COUNT": "F.count",
        "AVG":   "F.avg",
        "MAX":   "F.max",
        "MIN":   "F.min",
        "STDDEV": "F.stddev",
        "VARIANCE": "F.variance",
        "FIRST": "F.first",
        "LAST":  "F.last",
        "COLLECT_LIST": "F.collect_list",
    },
    "datafusion": {
        "SUM":   "SUM",
        "COUNT": "COUNT",
        "AVG":   "AVG",
        "MAX":   "MAX",
        "MIN":   "MIN",
        "STDDEV": "STDDEV",
        "VARIANCE": "VARIANCE",
    },
}
