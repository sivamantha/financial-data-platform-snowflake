-- =============================================================================
-- MASKING & ROW ACCESS POLICIES: Data governance demonstration
-- Run as: ACCOUNTADMIN
--
-- Demonstrates governance awareness with masking policies on financial data.
-- Policies mask sensitive-pattern fields from roles that shouldn't see them.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FIN_ANALYTICS_DB;

-- ── Column masking: email addresses ──────────────────────────
CREATE OR REPLACE MASKING POLICY FIN_ANALYTICS_DB.GOLD.EMAIL_MASK AS
    (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'DBT_TRANSFORM_ROLE')
            THEN val
        WHEN CURRENT_ROLE() = 'ANALYST_ROLE'
            THEN REGEXP_REPLACE(val, '.+@', '****@')
        ELSE '**MASKED**'
    END
;

-- ── Column masking: financial account numbers ────────────────
CREATE OR REPLACE MASKING POLICY FIN_ANALYTICS_DB.GOLD.ACCOUNT_NUMBER_MASK AS
    (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'DBT_TRANSFORM_ROLE')
            THEN val
        WHEN CURRENT_ROLE() = 'ANALYST_ROLE'
            THEN CONCAT('****-****-', RIGHT(val, 4))
        ELSE '**MASKED**'
    END
;

-- ── Column masking: numeric values (revenue, balances) ───────
CREATE OR REPLACE MASKING POLICY FIN_ANALYTICS_DB.GOLD.FINANCIAL_AMOUNT_MASK AS
    (val NUMBER(18,2)) RETURNS NUMBER(18,2) ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'DBT_TRANSFORM_ROLE')
            THEN val
        WHEN CURRENT_ROLE() = 'ANALYST_ROLE'
            THEN ROUND(val, -3)
        ELSE 0
    END
;

-- ── Row access policy ────────────────────────────────────────
-- Controls which SEC filing types analysts can see
CREATE OR REPLACE ROW ACCESS POLICY FIN_ANALYTICS_DB.GOLD.FILING_ACCESS_POLICY AS
    (filing_type STRING) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'DBT_TRANSFORM_ROLE')
            THEN TRUE
        WHEN CURRENT_ROLE() = 'ANALYST_ROLE'
            AND filing_type IN ('10-K', '10-Q', '8-K')
            THEN TRUE
        ELSE FALSE
    END
;

-- ── Tag-based governance (object tagging) ────────────────────
CREATE TAG IF NOT EXISTS FIN_ANALYTICS_DB.GOLD.DATA_CLASSIFICATION
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity classification'
;

CREATE TAG IF NOT EXISTS FIN_ANALYTICS_DB.GOLD.DATA_DOMAIN
    ALLOWED_VALUES 'SEC_FILINGS', 'MARKET_DATA', 'MACRO_ECONOMICS', 'FX_RATES'
    COMMENT = 'Business domain classification'
;

CREATE TAG IF NOT EXISTS FIN_ANALYTICS_DB.GOLD.PII_FLAG
    ALLOWED_VALUES 'TRUE', 'FALSE'
    COMMENT = 'Indicates presence of personally identifiable information'
;

-- ══════════════════════════════════════════════════════════════
-- USAGE EXAMPLES (apply after gold tables are created by dbt):
--
-- Apply masking to a column:
--   ALTER TABLE FIN_ANALYTICS_DB.GOLD.DIM_COMPANY
--       MODIFY COLUMN email SET MASKING POLICY FIN_ANALYTICS_DB.GOLD.EMAIL_MASK;
--
-- Apply row access policy:
--   ALTER TABLE FIN_ANALYTICS_DB.GOLD.FACT_SEC_FILINGS
--       ADD ROW ACCESS POLICY FIN_ANALYTICS_DB.GOLD.FILING_ACCESS_POLICY
--       ON (filing_type);
--
-- Apply tags:
--   ALTER TABLE FIN_ANALYTICS_DB.GOLD.FACT_SEC_FILINGS
--       SET TAG FIN_ANALYTICS_DB.GOLD.DATA_CLASSIFICATION = 'PUBLIC';
--   ALTER TABLE FIN_ANALYTICS_DB.GOLD.FACT_SEC_FILINGS
--       SET TAG FIN_ANALYTICS_DB.GOLD.DATA_DOMAIN = 'SEC_FILINGS';
-- ══════════════════════════════════════════════════════════════