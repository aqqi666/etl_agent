from app.tools.connection import test_connection
from app.tools.inspection import list_databases, list_tables, describe_table, get_column_details
from app.tools.query import execute_query, preview_data
from app.tools.mutation import execute_sql
from app.tools.quality import check_data_quality
from app.tools.lineage import generate_lineage
from app.tools.render import render

ALL_TOOLS = [
    test_connection,
    list_databases,
    list_tables,
    describe_table,
    get_column_details,
    execute_query,
    preview_data,
    execute_sql,
    check_data_quality,
    generate_lineage,
    render,
]
