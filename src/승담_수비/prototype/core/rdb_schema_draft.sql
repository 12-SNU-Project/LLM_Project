-- Draft schema for audit-report structured extraction
-- Target entities: filings, sections, tables, table_rows, table_values

CREATE TABLE IF NOT EXISTS filings (
    filing_id                VARCHAR(128) PRIMARY KEY,
    company_name             VARCHAR(200) NOT NULL,
    fiscal_year              INTEGER,
    report_type              VARCHAR(100) NOT NULL DEFAULT '감사보고서',
    auditor_name             VARCHAR(200),
    auditor_report_date      VARCHAR(40),
    source_file              TEXT,
    source_encoding          VARCHAR(40),
    parser_backend           VARCHAR(40),
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sections (
    section_id               VARCHAR(160) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    section_type             VARCHAR(80) NOT NULL,
    section_title            TEXT NOT NULL,
    start_block_id           VARCHAR(80) NOT NULL,
    end_block_id             VARCHAR(80),
    order_index              INTEGER NOT NULL,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tables (
    table_id                 VARCHAR(160) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    section_id               VARCHAR(160) REFERENCES sections(section_id),
    section_type             VARCHAR(80),
    statement_type           VARCHAR(80),
    table_role               VARCHAR(80) NOT NULL,
    title                    TEXT,
    unit                     VARCHAR(80),
    year_candidates_json     TEXT,  -- JSON array of years
    source_block_id          VARCHAR(80),
    context_before           TEXT,
    context_after            TEXT,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS table_rows (
    row_id                   VARCHAR(200) PRIMARY KEY,
    table_id                 VARCHAR(160) NOT NULL REFERENCES tables(table_id),
    row_index                INTEGER NOT NULL,
    raw_label                TEXT NOT NULL,
    normalized_label         TEXT,
    row_depth                INTEGER NOT NULL DEFAULT 0,
    parent_row_id            VARCHAR(200) REFERENCES table_rows(row_id),
    is_section_header        BOOLEAN NOT NULL DEFAULT FALSE,
    note_reference_json      TEXT,  -- JSON array of note ids
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS table_values (
    value_id                 VARCHAR(220) PRIMARY KEY,
    table_id                 VARCHAR(160) NOT NULL REFERENCES tables(table_id),
    row_id                   VARCHAR(200) NOT NULL REFERENCES table_rows(row_id),
    col_index                INTEGER NOT NULL,
    period                   VARCHAR(80),
    value_raw                TEXT,
    value_numeric            NUMERIC(30, 6),
    unit                     VARCHAR(80),
    column_header_path       TEXT,
    is_primary_value         BOOLEAN NOT NULL DEFAULT FALSE,
    note_reference_json      TEXT,  -- JSON array of note ids
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS text_chunks (
    chunk_id                 VARCHAR(220) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    fiscal_year              INTEGER,
    section_type             VARCHAR(80),
    section_title            TEXT,
    auditor_name             VARCHAR(200),
    near_table_id            VARCHAR(160) REFERENCES tables(table_id),
    topic_hint               VARCHAR(120),
    text                     TEXT NOT NULL,
    start_block_id           VARCHAR(80),
    end_block_id             VARCHAR(80),
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sections_filing ON sections(filing_id);
CREATE INDEX IF NOT EXISTS idx_tables_filing ON tables(filing_id);
CREATE INDEX IF NOT EXISTS idx_tables_section ON tables(section_id);
CREATE INDEX IF NOT EXISTS idx_table_rows_table ON table_rows(table_id);
CREATE INDEX IF NOT EXISTS idx_table_values_table ON table_values(table_id);
CREATE INDEX IF NOT EXISTS idx_table_values_row ON table_values(row_id);
CREATE INDEX IF NOT EXISTS idx_table_values_period ON table_values(period);
CREATE INDEX IF NOT EXISTS idx_text_chunks_filing ON text_chunks(filing_id);
CREATE INDEX IF NOT EXISTS idx_text_chunks_section ON text_chunks(section_type);
CREATE INDEX IF NOT EXISTS idx_text_chunks_topic ON text_chunks(topic_hint);
