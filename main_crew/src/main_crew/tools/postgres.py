from typing import Any, Dict, Optional, Type
try:
    import psycopg2
except ImportError:
    raise ImportError(
        "`psycopg2` not installed. Please install using `pip install psycopg2`. If you face issues, try `pip install psycopg2-binary`."
    )

from crewai.tools import BaseTool  # CrewAI's base tool class
from pydantic import BaseModel, Field, PrivateAttr
import logging

# Set up a logger for debugging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Define the input schema for our tool
class PostgresToolInput(BaseModel):
    command: str = Field(
        ...,
        description=(
            "The command to execute. Options: 'show_tables', 'describe_table', "
            "'run_query', 'inspect_query', 'summarize_table', 'export_table_to_path'."
            "add schema name to the query like \"schema_name\".table_name"
            "use show_tables to get the list of tables with their descriptions and then use describe_table to get the structure of the table and it's columns"
            "do not use column names that are not in the table use describe_table to get the real column names"
        ),
    )
    table: Optional[str] = Field(None, description="Table name (if applicable).")
    query: Optional[str] = Field(None, description="SQL query (if applicable).")
    path: Optional[str] = Field(None, description="File path for export (if applicable).")

class PostgresTool(BaseTool):
    name: str = "PostgresTool"
    description: str = "A tool to connect to a PostgreSQL database and perform read-only operations."
    args_schema: Type[BaseModel] = PostgresToolInput

    # Use PrivateAttr for connection parameters
    _db_name: Optional[str] = PrivateAttr()
    _user: Optional[str] = PrivateAttr()
    _password: Optional[str] = PrivateAttr()
    _host: Optional[str] = PrivateAttr()
    _port: Optional[int] = PrivateAttr()
    _table_schema: str = PrivateAttr(default="public")
    _connection: Optional[psycopg2.extensions.connection] = PrivateAttr(default=None)

    def __init__(
        self,
        db_name: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        table_schema: str = "bse5uGrG2eJswURuaGL",
        **kwargs
    ):
        super().__init__(**kwargs)
        self._db_name = db_name
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._table_schema = table_schema

    @property
    def connection(self) -> psycopg2.extensions.connection:
        if self._connection is None:
            connection_kwargs: Dict[str, Any] = {}
            if self._db_name is not None:
                connection_kwargs["database"] = self._db_name
            if self._user is not None:
                connection_kwargs["user"] = self._user
            if self._password is not None:
                connection_kwargs["password"] = self._password
            if self._host is not None:
                connection_kwargs["host"] = self._host
            if self._port is not None:
                connection_kwargs["port"] = self._port
            if self._table_schema is not None:
                connection_kwargs["options"] = f"-c search_path={self._table_schema}"
            self._connection = psycopg2.connect(**connection_kwargs)
            self._connection.set_session(readonly=True)
        return self._connection

    def _run(self, command: str, table: Optional[str] = None, query: Optional[str] = None, path: Optional[str] = None) -> str:
        if command == "show_tables":
            return self.show_tables()
        elif command == "describe_table":
            if table is None:
                return "Error: 'table' parameter is required for describe_table."
            return self.describe_table(table)
        elif command == "summarize_table":
            if table is None:
                return "Error: 'table' parameter is required for summarize_table."
            return self.summarize_table(table)
        elif command == "inspect_query":
            if query is None:
                return "Error: 'query' parameter is required for inspect_query."
            return self.inspect_query(query)
        elif command == "export_table_to_path":
            if table is None:
                return "Error: 'table' parameter is required for export_table_to_path."
            return self.export_table_to_path(table, path)
        elif command == "run_query":
            if query is None:
                return "Error: 'query' parameter is required for run_query."
            return self.run_query(query)
        else:
            return ("Error: Invalid command. Supported commands: show_tables, describe_table, "
                    "summarize_table, inspect_query, export_table_to_path, run_query.")

    def show_tables(self) -> str:
        # Query to get table name and description only
        tables_stmt = f"""SELECT 
        table_name,
        table_schema,
        obj_description(pg_class.oid, 'pg_class') AS table_description
        FROM 
        information_schema.tables t
        JOIN 
        pg_catalog.pg_class ON t.table_name = pg_class.relname
        WHERE 
        table_schema = '{self._table_schema}'
        AND table_type = 'BASE TABLE';"""
        cursor = self.connection.cursor()
        cursor.execute(tables_stmt)
        tables = cursor.fetchall()
        result = []
        for table_name, table_schema, table_description in tables:
            result.append(f"Table: {table_name} - {table_description}")
        return "\n".join(result)

    def describe_table(self, table: str) -> str:
        stmt = (
            f"SELECT column_name, "
            f"pg_catalog.col_description(('\"{self._table_schema}\".\"{table}\"')::regclass, ordinal_position) AS column_description, "
            f"data_type, character_maximum_length "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{table}' AND table_schema = '{self._table_schema}';"
        )
        table_description = self.run_query(stmt)
        logger.debug(f"Table description: {table_description}")
        return f'"{self._table_schema}".{table}\n{table_description}'

    def summarize_table(self, table: str) -> str:
        stmt = f"""WITH column_stats AS (
                    SELECT
                        column_name,
                        data_type
                    FROM
                        information_schema.columns
                    WHERE
                        table_name = '{table}'
                        AND table_schema = '{self._table_schema}'
                )
                SELECT
                    column_name,
                    data_type,
                    COUNT(COALESCE(column_name::text, '')) AS non_null_count,
                    COUNT(*) - COUNT(COALESCE(column_name::text, '')) AS null_count,
                    SUM(COALESCE(column_name::numeric, 0)) AS sum,
                    AVG(COALESCE(column_name::numeric, 0)) AS mean,
                    MIN(column_name::numeric) AS min,
                    MAX(column_name::numeric) AS max,
                    STDDEV(COALESCE(column_name::numeric, 0)) AS stddev
                FROM
                    column_stats,
                    LATERAL (
                        SELECT *
                        FROM "{self._table_schema}".{table}
                    ) AS tbl
                WHERE
                    data_type IN ('integer', 'numeric', 'real', 'double precision')
                GROUP BY column_name, data_type
                UNION ALL
                SELECT
                    column_name,
                    data_type,
                    COUNT(COALESCE(column_name::text, '')) AS non_null_count,
                    COUNT(*) - COUNT(COALESCE(column_name::text, '')) AS null_count,
                    NULL AS sum,
                    NULL AS mean,
                    NULL AS min,
                    NULL AS max,
                    NULL AS stddev
                FROM
                    column_stats,
                    LATERAL (
                        SELECT *
                        FROM "{self._table_schema}".{table}
                    ) AS tbl
                WHERE
                    data_type NOT IN ('integer', 'numeric', 'real', 'double precision')
                GROUP BY column_name, data_type;
        """
        table_summary = self.run_query(stmt)
        logger.debug(f"Table summary: {table_summary}")
        return table_summary

    def inspect_query(self, query: str) -> str:
        stmt = f"EXPLAIN {query};"
        explain_plan = self.run_query(stmt)
        explain_plan = explain_plan.replace("EXPLAIN", "").replace("QUERY PLAN", "").replace("->", "").replace("(", "").replace(")", "").replace(" ", "").replace("\n", "").replace("\t", "").replace("\r", "").replace("\f", "").replace("\v", "")
        logger.debug(f"Explain plan: {explain_plan}")
        return explain_plan

    def export_table_to_path(self, table: str, path: Optional[str] = None) -> str:
        logger.debug(f"Exporting table {table} as CSV to path {path}")
        if path is None:
            path = f"{table}.csv"
        else:
            path = f"{path}/{table}.csv"
        export_statement = f"COPY {self._table_schema}.{table} TO '{path}' DELIMITER ',' CSV HEADER;"
        result = self.run_query(export_statement)
        logger.debug(f"Exported {table} to {path}")
        return result

    def run_query(self, query: str) -> str:
        formatted_sql = query.replace("`", "").split(";")[0]
        # Add schema prefix to table names if not already present
        if not formatted_sql.lower().startswith(('explain', 'with')):
            words = formatted_sql.split()
            for i, word in enumerate(words):
                if word.lower() in ('from', 'join', 'update', 'into') and i + 1 < len(words):
                    table_name = words[i + 1]
                    if '.' not in table_name and not table_name.startswith('"'):
                        words[i + 1] = f'"{self._table_schema}".{table_name}'
            formatted_sql = ' '.join(words)
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(formatted_sql)
            query_result = cursor.fetchall()

            result_rows = []
            if query_result is not None:
                for row in query_result:
                    if len(row) == 1:
                        result_rows.append(str(row[0]))
                    else:
                        result_rows.append(", ".join(str(x) for x in row))
                result_output = "\n".join(result_rows)
            else:
                result_output = "No output"
            logger.debug(f"Query result: {result_output}")
            return result_output
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return str(e)
