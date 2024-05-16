from ingestion.utils.test_zip.connection import connect_to_db
from pg8000.native import identifier


def select_all_from_given_table(table_name):
    """
    Takes: a table name as an arguement
    Returns: a python object of the data from the table
    """
    conn = connect_to_db()
    query = conn.run(
        f"""
            SELECT *
                FROM {identifier(table_name)};
        """
    )
    columns = [col["name"] for col in conn.columns]
    result = [dict(zip(columns, line)) for line in query]
    return result


def select_head_from_given_table(table_name):
    conn = connect_to_db()
    query = conn.run(
        f"""
            SELECT *
                FROM {identifier(table_name)}
                LIMIT 100;
        """
    )
    columns = [col["name"] for col in conn.columns]
    result = [dict(zip(columns, line)) for line in query]
    return result
