-- ==========================================================
-- Dynamic Data Quality Monitoring Framework (Databricks)
-- Catalog: workspace
-- Schema: default
-- Backend Tables: source_table, dq_rules, dq_results
-- ==========================================================

USE CATALOG workspace;
USE SCHEMA default;

-- 1. source_table: Monitored datasets configured for DQ monitoring
CREATE TABLE IF NOT EXISTS workspace.default.source_table (
    table_id INT,
    database_name STRING,
    domain_name STRING,
    dataset_name STRING,
    layer STRING,
    owner STRING,
    schedule STRING,
    columns_list STRING,
    is_active BOOLEAN
);

-- 2. dq_rules: Data Quality validation rules & configurations
CREATE TABLE IF NOT EXISTS workspace.default.dq_rules (
    rule_id INT,
    table_id INT,
    dataset_name STRING,
    rule_type STRING,
    rule_name STRING,
    column_name STRING,
    threshold STRING,
    severity STRING,
    weight INT,
    is_active BOOLEAN,
    created_time TIMESTAMP
);

-- 3. dq_results: Live execution results of DQ validation checks
CREATE TABLE IF NOT EXISTS workspace.default.dq_results (
    execution_id STRING,
    table_id INT,
    dataset_name STRING,
    rule_id INT,
    rule_name STRING,
    column_name STRING,
    threshold STRING,
    actual_value STRING,
    status STRING, -- 'Passed', 'Warning', 'Failed'
    execution_time TIMESTAMP,
    details STRING
);

-- 4. dq_alerts: Active alerts and anomaly incidents
CREATE TABLE IF NOT EXISTS workspace.default.dq_alerts (
    alert_id STRING,
    table_id INT,
    dataset_name STRING,
    rule_name STRING,
    severity STRING, -- 'Critical', 'Warning', 'Info'
    issue_description STRING,
    detection_time TIMESTAMP,
    status STRING, -- 'Open', 'Acknowledged', 'Investigating', 'Resolved'
    owner STRING
);

-- 5. dq_overall_scores: Overall framework quality scores
CREATE TABLE IF NOT EXISTS workspace.default.dq_overall_scores (
    overall_score DOUBLE,
    score_delta DOUBLE,
    rolling_days INT,
    calculated_at TIMESTAMP
);

-- 6. dq_score_history: Quality score trend over time
CREATE TABLE IF NOT EXISTS workspace.default.dq_score_history (
    execution_time TIMESTAMP,
    dq_score DOUBLE
);

-- ==========================================================
-- SEED DATA (Monitored Datasets: customers, orders, order_items, products, employees, transactions)
-- ==========================================================

TRUNCATE TABLE workspace.default.source_table;
INSERT INTO workspace.default.source_table VALUES
(1, 'DELTA_LAKE', 'Sales & Revenue', 'orders', 'Gold', 'data-eng-orders', 'Every 4 hours', 'order_id, customer_id, order_date, order_updated_at, status, total_amount', true),
(2, 'DELTA_LAKE', 'Customer 360', 'customers', 'Gold', 'crm-team', 'Daily', 'customer_id, email, first_name, last_name, loyalty_tier, created_at', true),
(3, 'DELTA_LAKE', 'Order Processing', 'order_items', 'Silver', 'data-eng-orders', 'Hourly', 'item_id, order_id, product_id, quantity, unit_price', true),
(4, 'DELTA_LAKE', 'Catalog Management', 'products', 'Gold', 'merchandising', 'Every 4 hours', 'product_id, sku, category, unit_price, stock_quantity', true),
(5, 'DELTA_LAKE', 'Staff Operations', 'employees', 'Silver', 'internal-systems', 'Daily', 'employee_id, department, email, hire_date, is_active', true),
(6, 'DELTA_LAKE', 'Financial Ledger', 'transactions', 'Bronze', 'payments-eng', 'Hourly', 'txn_id, account_id, txn_date, txn_amount, merchant_id', true);

TRUNCATE TABLE workspace.default.dq_rules;
INSERT INTO workspace.default.dq_rules VALUES
(101, 1, 'orders', 'Freshness Check', 'Freshness Check', 'order_updated_at', '<= 4h', 'Critical', 20, true, TIMESTAMP '2026-08-01 00:00:00'),
(102, 1, 'orders', 'Null Check', 'Customer ID Null Check', 'customer_id', '= 0', 'Warning', 10, true, TIMESTAMP '2026-08-01 00:00:00'),
(103, 1, 'orders', 'Duplicate Check', 'Order ID Uniqueness', 'order_id', '= 0', 'Critical', 15, true, TIMESTAMP '2026-08-01 00:00:00'),
(104, 1, 'orders', 'Range Check', 'Valid Order Status Enum', 'status', "in ('shipped', 'pending', 'cancelled')", 'Info', 5, true, TIMESTAMP '2026-08-01 00:00:00'),
(105, 2, 'customers', 'Null Check', 'Email Null Percentage', 'email', '<= 2%', 'Critical', 15, true, TIMESTAMP '2026-08-01 00:00:00'),
(106, 2, 'customers', 'Completeness Check', 'Customer Record Completeness', 'customer_id', '>= 99%', 'Warning', 10, true, TIMESTAMP '2026-08-01 00:00:00'),
(107, 3, 'order_items', 'Duplicate Check', 'Item ID Uniqueness', 'item_id', '= 0', 'Critical', 15, true, TIMESTAMP '2026-08-01 00:00:00'),
(108, 4, 'products', 'Range Check', 'Price Non-Negative', 'unit_price', '>= 0', 'Warning', 10, true, TIMESTAMP '2026-08-01 00:00:00'),
(109, 5, 'employees', 'Null Check', 'Employee Email Required', 'email', '= 0', 'Info', 5, true, TIMESTAMP '2026-08-01 00:00:00'),
(110, 6, 'transactions', 'Schema Validation', 'Ledger Schema Validation', NULL, 'strict', 'Critical', 15, true, TIMESTAMP '2026-08-01 00:00:00');

TRUNCATE TABLE workspace.default.dq_results;
INSERT INTO workspace.default.dq_results VALUES
('RES-101', 1, 'orders', 101, 'Freshness Check', 'order_updated_at', '<= 4h', '6h 40m stale', 'Failed', TIMESTAMP '2026-09-30 14:12:00', 'Last updated timestamp exceeds SLA of 4 hours'),
('RES-102', 1, 'orders', 102, 'Customer ID Null Check', 'customer_id', '= 0', '0 nulls found', 'Passed', TIMESTAMP '2026-09-30 14:12:00', '0 null values in 48,210 rows'),
('RES-103', 1, 'orders', 103, 'Order ID Uniqueness', 'order_id', '= 0', '0 duplicates', 'Passed', TIMESTAMP '2026-09-30 14:12:00', 'No duplicate order keys found'),
('RES-104', 1, 'orders', 104, 'Valid Order Status Enum', 'status', "in ('shipped', 'pending', 'cancelled')", '3 invalid of 48,210', 'Warning', TIMESTAMP '2026-09-30 14:12:00', '3 rows contained status="returned"'),
('RES-105', 2, 'customers', 105, 'Email Null Percentage', 'email', '<= 2%', '3.8% nulls', 'Warning', TIMESTAMP '2026-09-30 14:10:00', '3.8% null emails exceeds 2% threshold'),
('RES-106', 2, 'customers', 106, 'Customer Record Completeness', 'customer_id', '>= 99%', '99.8% complete', 'Passed', TIMESTAMP '2026-09-30 14:10:00', 'All required attributes populated'),
('RES-107', 3, 'order_items', 107, 'Item ID Uniqueness', 'item_id', '= 0', '0 duplicates', 'Passed', TIMESTAMP '2026-09-30 14:05:00', 'All line item IDs unique'),
('RES-108', 4, 'products', 108, 'Price Non-Negative', 'unit_price', '>= 0', '0 negative prices', 'Passed', TIMESTAMP '2026-09-30 14:00:00', 'Valid price ranges'),
('RES-109', 5, 'employees', 109, 'Employee Email Required', 'email', '= 0', '0 null emails', 'Passed', TIMESTAMP '2026-09-30 13:45:00', 'Valid employee email roster'),
('RES-110', 6, 'transactions', 110, 'Ledger Schema Validation', NULL, 'strict', '2 unexpected columns', 'Warning', TIMESTAMP '2026-09-30 13:30:00', 'Columns client_ip, route_id detected');

TRUNCATE TABLE workspace.default.dq_alerts;
INSERT INTO workspace.default.dq_alerts VALUES
('ALT-001', 1, 'orders', 'Freshness Check', 'Critical', 'Data is 6h 40m stale — SLA is 4h', TIMESTAMP '2026-09-30 14:12:00', 'Open', 'data-eng-orders'),
('ALT-002', 2, 'customers', 'Null Check', 'Warning', '3.8% null values in email column — threshold is 2%', TIMESTAMP '2026-09-30 14:10:00', 'Acknowledged', 'crm-team'),
('ALT-003', 6, 'transactions', 'Duplicate Check', 'Critical', '142 duplicate transaction_id values detected', TIMESTAMP '2026-09-30 14:05:00', 'Open', 'payments-eng'),
('ALT-004', 1, 'orders', 'Range Check', 'Warning', '3 invalid order status values outside enum definition', TIMESTAMP '2026-09-30 14:00:00', 'Investigating', 'data-eng-orders'),
('ALT-005', 6, 'transactions', 'Schema Validation', 'Info', '2 new non-breaking columns detected in Bronze layer', TIMESTAMP '2026-09-30 13:30:00', 'Resolved', 'platform');

TRUNCATE TABLE workspace.default.dq_overall_scores;
INSERT INTO workspace.default.dq_overall_scores VALUES
(96.4, 1.2, 7, TIMESTAMP '2026-09-30 14:15:00');

TRUNCATE TABLE workspace.default.dq_score_history;
INSERT INTO workspace.default.dq_score_history VALUES
(TIMESTAMP '2026-09-17 00:00:00', 93.8),
(TIMESTAMP '2026-09-18 00:00:00', 94.1),
(TIMESTAMP '2026-09-19 00:00:00', 93.5),
(TIMESTAMP '2026-09-20 00:00:00', 94.6),
(TIMESTAMP '2026-09-21 00:00:00', 95.0),
(TIMESTAMP '2026-09-22 00:00:00', 94.9),
(TIMESTAMP '2026-09-23 00:00:00', 95.3),
(TIMESTAMP '2026-09-24 00:00:00', 95.6),
(TIMESTAMP '2026-09-25 00:00:00', 95.4),
(TIMESTAMP '2026-09-26 00:00:00', 95.2),
(TIMESTAMP '2026-09-27 00:00:00', 95.5),
(TIMESTAMP '2026-09-28 00:00:00', 95.7),
(TIMESTAMP '2026-09-29 00:00:00', 96.0),
(TIMESTAMP '2026-09-30 00:00:00', 96.4);
