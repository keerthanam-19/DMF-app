import sys, os
sys.path.insert(0, os.path.abspath("."))
import db

check = db.execute_query("SELECT metric_id, metric_name FROM workspace.default.dq_metric_catalog WHERE LOWER(metric_name) LIKE '%expected%'")
print("Existing expected metrics in Databricks:\n", check)

if check.empty:
    sql = """
    INSERT INTO workspace.default.dq_metric_catalog 
    (metric_name, metric_category, description, is_active, created_ts, sql_template, comparison_operator)
    VALUES ('Expected Values Check', 'Validity', 'Validates that column values conform to a predefined list of expected values with typo and misspelling detection', true, CURRENT_TIMESTAMP(), 'SELECT COUNT(*) AS metric FROM {table} WHERE {column} IS NOT NULL AND {column} NOT IN ({expected_values})', 'IN')
    """
    success = db.execute_statement(sql)
    print("Inserted Expected Values Check into Databricks dq_metric_catalog:", success)
    db.clear_cache()
else:
    print("Expected Values Check already exists in Databricks!")
