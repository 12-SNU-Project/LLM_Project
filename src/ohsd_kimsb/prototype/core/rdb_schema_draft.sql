CREATE TABLE IF NOT EXISTS filings (
    filing_id                VARCHAR(128) PRIMARY KEY,
    company_name             VARCHAR(200) NOT NULL,
    fiscal_year              INTEGER,
    auditor_name             VARCHAR(200),
    auditor_report_date      VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS tables_registry (
    table_id                 VARCHAR(160) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    table_title              TEXT,
    semantic_table_type      VARCHAR(80),
    table_unit               VARCHAR(80),
    table_markdown           TEXT,
    footnotes                TEXT
);

CREATE TABLE IF NOT EXISTS table_rows (
    row_id                   VARCHAR(200) PRIMARY KEY,
    table_id                 VARCHAR(160) NOT NULL REFERENCES tables_registry(table_id),
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    row_index                INTEGER NOT NULL,
    raw_label                TEXT NOT NULL,
    normalized_label         TEXT,
    row_depth                INTEGER NOT NULL DEFAULT 0,
    parent_row_id            VARCHAR(200),
    is_section_header        BOOLEAN NOT NULL DEFAULT FALSE,
    row_group_label          VARCHAR(120),
    company_kind             VARCHAR(40)
);

-- Runtime SQL still uses flattened metric facts, but each fact now points back
-- to a preserved table-level markdown/footnote record.
CREATE TABLE IF NOT EXISTS metric_facts (
    value_id                 VARCHAR(220) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    fiscal_year              INTEGER,
    table_id                 VARCHAR(160) NOT NULL REFERENCES tables_registry(table_id),
    section_type             VARCHAR(80),
    statement_type           VARCHAR(80),
    table_role               VARCHAR(80) NOT NULL,
    table_subrole            VARCHAR(80),
    semantic_table_type      VARCHAR(80),
    table_title              TEXT,
    table_unit               VARCHAR(80),
    page_start               INTEGER,
    page_end                 INTEGER,
    row_id                   VARCHAR(200) NOT NULL,
    row_index                INTEGER NOT NULL,
    raw_label                TEXT NOT NULL,
    normalized_label         TEXT,
    parent_row_id            VARCHAR(200),
    is_section_header        BOOLEAN NOT NULL DEFAULT FALSE,
    row_group_label          VARCHAR(120),
    company_kind             VARCHAR(40),
    col_index                INTEGER NOT NULL,
    column_key               VARCHAR(160) NOT NULL,
    period                   VARCHAR(80),
    value_role               VARCHAR(80),
    value_raw                TEXT,
    value_numeric            NUMERIC(30, 6),
    unit                     VARCHAR(80),
    column_header_path       TEXT,
    is_primary_value         BOOLEAN NOT NULL DEFAULT FALSE
);

-- Keep the exact chunk text used for Chroma ingestion for debugging and
-- citation verification. Runtime retrieval still queries Chroma directly.
CREATE TABLE IF NOT EXISTS text_chunks (
    chunk_id                 VARCHAR(220) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    fiscal_year              INTEGER,
    section_type             VARCHAR(80),
    section_title            TEXT,
    near_table_id            VARCHAR(160),
    topic_hint               VARCHAR(120),
    text                     TEXT NOT NULL,
    is_structural_chunk      BOOLEAN NOT NULL DEFAULT FALSE,
    page_start               INTEGER,
    page_end                 INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tables_registry_filing ON tables_registry(filing_id);
CREATE INDEX IF NOT EXISTS idx_tables_registry_semantic ON tables_registry(semantic_table_type);
CREATE INDEX IF NOT EXISTS idx_table_rows_filing ON table_rows(filing_id);
CREATE INDEX IF NOT EXISTS idx_table_rows_table ON table_rows(table_id);
CREATE INDEX IF NOT EXISTS idx_table_rows_parent ON table_rows(parent_row_id);
CREATE INDEX IF NOT EXISTS idx_table_rows_label ON table_rows(normalized_label, raw_label);
CREATE INDEX IF NOT EXISTS idx_table_rows_company_kind ON table_rows(company_kind);
CREATE INDEX IF NOT EXISTS idx_table_rows_section_header ON table_rows(is_section_header);
CREATE INDEX IF NOT EXISTS idx_metric_facts_filing ON metric_facts(filing_id);
CREATE INDEX IF NOT EXISTS idx_metric_facts_year ON metric_facts(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_metric_facts_table ON metric_facts(table_id);
CREATE INDEX IF NOT EXISTS idx_metric_facts_role ON metric_facts(table_role, table_subrole);
CREATE INDEX IF NOT EXISTS idx_metric_facts_semantic ON metric_facts(semantic_table_type);
CREATE INDEX IF NOT EXISTS idx_metric_facts_statement ON metric_facts(statement_type);
CREATE INDEX IF NOT EXISTS idx_metric_facts_label ON metric_facts(normalized_label, raw_label);
CREATE INDEX IF NOT EXISTS idx_metric_facts_company_kind ON metric_facts(company_kind);
CREATE INDEX IF NOT EXISTS idx_metric_facts_column_key ON metric_facts(column_key);
CREATE INDEX IF NOT EXISTS idx_metric_facts_period ON metric_facts(period);
CREATE INDEX IF NOT EXISTS idx_metric_facts_primary ON metric_facts(is_primary_value);
CREATE INDEX IF NOT EXISTS idx_text_chunks_filing ON text_chunks(filing_id);
CREATE INDEX IF NOT EXISTS idx_text_chunks_section ON text_chunks(section_type);
CREATE INDEX IF NOT EXISTS idx_text_chunks_structural ON text_chunks(is_structural_chunk);
CREATE INDEX IF NOT EXISTS idx_text_chunks_page ON text_chunks(filing_id, page_start, page_end);

CREATE VIEW IF NOT EXISTS related_party_balance_view AS
SELECT
    m.filing_id,
    m.fiscal_year,
    m.table_id,
    m.semantic_table_type,
    m.table_title,
    m.table_unit,
    r.row_id,
    r.row_index,
    r.raw_label,
    r.normalized_label,
    r.row_depth,
    r.parent_row_id,
    r.is_section_header,
    r.row_group_label,
    r.company_kind,
    m.period,
    m.column_key,
    m.value_role,
    m.value_raw,
    m.value_numeric,
    m.unit,
    m.page_start,
    m.page_end
FROM metric_facts m
JOIN table_rows r ON r.row_id = m.row_id
WHERE m.is_primary_value = 1
  AND COALESCE(m.semantic_table_type, '') = 'related_party_balance_table';

CREATE VIEW IF NOT EXISTS related_party_transaction_view AS
SELECT
    m.filing_id,
    m.fiscal_year,
    m.table_id,
    m.semantic_table_type,
    m.table_title,
    m.table_unit,
    r.row_id,
    r.row_index,
    r.raw_label,
    r.normalized_label,
    r.row_depth,
    r.parent_row_id,
    r.is_section_header,
    r.row_group_label,
    r.company_kind,
    m.period,
    m.column_key,
    m.value_role,
    m.value_raw,
    m.value_numeric,
    m.unit,
    m.page_start,
    m.page_end
FROM metric_facts m
JOIN table_rows r ON r.row_id = m.row_id
WHERE m.is_primary_value = 1
  AND COALESCE(m.semantic_table_type, '') = 'related_party_transaction_table';

CREATE VIEW IF NOT EXISTS subsidiary_status_view AS
SELECT
    m.filing_id,
    m.fiscal_year,
    m.table_id,
    m.semantic_table_type,
    m.table_title,
    m.table_unit,
    r.row_id,
    r.row_index,
    r.raw_label,
    r.normalized_label,
    r.row_depth,
    r.parent_row_id,
    r.is_section_header,
    r.row_group_label,
    r.company_kind,
    m.period,
    m.column_key,
    m.value_role,
    m.value_raw,
    m.value_numeric,
    m.unit,
    m.page_start,
    m.page_end
FROM metric_facts m
JOIN table_rows r ON r.row_id = m.row_id
WHERE m.is_primary_value = 1
  AND COALESCE(m.semantic_table_type, '') = 'subsidiary_status_table';

CREATE VIEW IF NOT EXISTS subsidiary_summary_view AS
SELECT
    m.filing_id,
    m.fiscal_year,
    m.table_id,
    m.semantic_table_type,
    m.table_title,
    m.table_unit,
    r.row_id,
    r.row_index,
    r.raw_label,
    r.normalized_label,
    r.row_depth,
    r.parent_row_id,
    r.is_section_header,
    r.row_group_label,
    r.company_kind,
    m.period,
    m.column_key,
    m.value_role,
    m.value_raw,
    m.value_numeric,
    m.unit,
    m.page_start,
    m.page_end
FROM metric_facts m
JOIN table_rows r ON r.row_id = m.row_id
WHERE m.is_primary_value = 1
  AND COALESCE(m.semantic_table_type, '') = 'subsidiary_summary_financial_table';
