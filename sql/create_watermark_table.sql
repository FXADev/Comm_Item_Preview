CREATE TABLE dbo.etl_watermark (
    source_name     varchar(30)  PRIMARY KEY,
    last_extract_ts datetime     NOT NULL
);
GO
