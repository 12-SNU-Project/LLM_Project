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

CREATE TABLE IF NOT EXISTS blocks (
    block_id                 VARCHAR(80) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    block_type               VARCHAR(40) NOT NULL,
    text                     TEXT,
    html_fragment            TEXT,
    dom_path                 TEXT,
    order_index              INTEGER NOT NULL,
    page_index               INTEGER,
    prev_block_id            VARCHAR(80),
    next_block_id            VARCHAR(80),
    section_id               VARCHAR(160),
    section_type             VARCHAR(80),
    section_title            TEXT,
    metadata_json            TEXT,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sections (
    section_id               VARCHAR(160) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    parent_section_id        VARCHAR(160) REFERENCES sections(section_id),
    section_level            INTEGER NOT NULL DEFAULT 1,
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
    table_subrole            VARCHAR(80),
    title                    TEXT,
    unit                     VARCHAR(80),
    year_candidates_json     TEXT,
    source_block_id          VARCHAR(80),
    page_start               INTEGER,
    page_end                 INTEGER,
    source_file              TEXT,
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
    note_reference_json      TEXT,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS table_values (
    value_id                 VARCHAR(220) PRIMARY KEY,
    table_id                 VARCHAR(160) NOT NULL REFERENCES tables(table_id),
    row_id                   VARCHAR(200) NOT NULL REFERENCES table_rows(row_id),
    col_index                INTEGER NOT NULL,
    column_key               VARCHAR(160) NOT NULL,
    period                   VARCHAR(80),
    value_role               VARCHAR(80),
    value_raw                TEXT,
    value_numeric            NUMERIC(30, 6),
    unit                     VARCHAR(80),
    column_header_path       TEXT,
    is_primary_value         BOOLEAN NOT NULL DEFAULT FALSE,
    note_reference_json      TEXT,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS text_chunks (
    chunk_id                 VARCHAR(220) PRIMARY KEY,
    filing_id                VARCHAR(128) NOT NULL REFERENCES filings(filing_id),
    fiscal_year              INTEGER,
    section_id               VARCHAR(160) REFERENCES sections(section_id),
    section_type             VARCHAR(80),
    section_title            TEXT,
    auditor_name             VARCHAR(200),
    near_table_id            VARCHAR(160) REFERENCES tables(table_id),
    topic_hint               VARCHAR(120),
    text                     TEXT NOT NULL,
    start_block_id           VARCHAR(80),
    end_block_id             VARCHAR(80),
    page_start               INTEGER,
    page_end                 INTEGER,
    source_file              TEXT,
    created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_blocks_filing ON blocks(filing_id);
CREATE INDEX IF NOT EXISTS idx_blocks_page ON blocks(filing_id, page_index);
CREATE INDEX IF NOT EXISTS idx_sections_filing ON sections(filing_id);
CREATE INDEX IF NOT EXISTS idx_sections_parent ON sections(parent_section_id);
CREATE INDEX IF NOT EXISTS idx_tables_filing ON tables(filing_id);
CREATE INDEX IF NOT EXISTS idx_tables_section ON tables(section_id);
CREATE INDEX IF NOT EXISTS idx_tables_page ON tables(filing_id, page_start, page_end);
CREATE INDEX IF NOT EXISTS idx_tables_role ON tables(table_role, table_subrole);
CREATE INDEX IF NOT EXISTS idx_table_rows_table ON table_rows(table_id);
CREATE INDEX IF NOT EXISTS idx_table_values_table ON table_values(table_id);
CREATE INDEX IF NOT EXISTS idx_table_values_row ON table_values(row_id);
CREATE INDEX IF NOT EXISTS idx_table_values_period ON table_values(period);
CREATE INDEX IF NOT EXISTS idx_table_values_column_key ON table_values(column_key);
CREATE INDEX IF NOT EXISTS idx_text_chunks_filing ON text_chunks(filing_id);
CREATE INDEX IF NOT EXISTS idx_text_chunks_section ON text_chunks(section_type);
CREATE INDEX IF NOT EXISTS idx_text_chunks_topic ON text_chunks(topic_hint);
CREATE INDEX IF NOT EXISTS idx_text_chunks_page ON text_chunks(filing_id, page_start, page_end);
