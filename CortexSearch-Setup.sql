
USE ROLE ACCOUNTADMIN;

CREATE ROLE CotexRole;


GRANT ROLE CotexRole TO USER WEBUSR;

use role CotexRole;

SELECT CURRENT_ROLE();

GRANT USAGE ON DATABASE CORTEX_SEARCH_DB TO ROLE COTEXROLE;
GRANT USAGE ON SCHEMA CORTEX_SEARCH_DB.CORTEX_SEARCH_SCHEMA TO ROLE COTEXROLE;
GRANT USAGE ON WAREHOUSE CORTEX_SEARCH_WH TO ROLE ACCOUNTADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE CORTEX_SEARCH_DB.CORTEX_SEARCH_SCHEMA.books_dataset_service TO ROLE COTEXROLE;

GRANT ALL PRIVILEGES ON DATABASE CORTEX_SEARCH_DB TO ROLE COTEXROLE;
GRANT ALL PRIVILEGES ON SCHEMA CORTEX_SEARCH_SCHEMA TO ROLE COTEXROLE;
GRANT ALL PRIVILEGES ON WAREHOUSE CORTEX_SEARCH_WH TO ROLE COTEXROLE;


----------------------db, schema , wh , table, stage ---------------------------------------

GRANT CREATE DATABASE ON ACCOUNT TO ROLE CotexRole;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE CotexRole;

-- create database
CREATE OR REPLACE DATABASE CORTEX_SEARCH_DB;

-- create schema
CREATE OR REPLACE SCHEMA CORTEX_SEARCH_SCHEMA;

-- create WH
CREATE OR REPLACE WAREHOUSE CORTEX_SEARCH_WH WITH
    WAREHOUSE_SIZE='X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED=TRUE;

USE WAREHOUSE cortex_search_wh;

CREATE OR REPLACE STAGE books_data_stage
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

list @books_data_stage;

CREATE OR REPLACE TABLE books_dataset_raw (
    Title TEXT,
    Authors TEXT,
    Description TEXT,
    Category TEXT,
    Publisher TEXT,
    Price_Starting_With FLOAT,
    Publish_Date_Month TEXT,
    Publish_Date_Year INT
);

COPY INTO books_dataset_raw
FROM @books_data_stage/BooksDatasetClean.csv
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

select * from books_dataset_raw;

--------------------------------function created---------------------------------------

CREATE OR REPLACE FUNCTION cortex_search_db.cortex_search_schema.books_chunk(
    description string, title string, authors string, category string, publisher string
)
    returns table (chunk string, title string, authors string, category string, publisher string)
    language python
    runtime_version = '3.9'
    handler = 'text_chunker'
    packages = ('snowflake-snowpark-python','langchain')
    as
$$
from langchain.text_splitter import RecursiveCharacterTextSplitter
import copy
from typing import Optional

class text_chunker:

    def process(self, description: Optional[str], title: str, authors: str, category: str, publisher: str):
        if description == None:
            description = "" # handle null values

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 2000,
            chunk_overlap  = 300,
            length_function = len
        )
        chunks = text_splitter.split_text(description)
        for chunk in chunks:
            yield (title + "\n" + authors + "\n" + chunk, title, authors, category, publisher) # always chunk with title
$$;

-------------------------------------------table created--------------------------------

CREATE or REPLACE TABLE cortex_search_db.cortex_search_schema.book_description_chunks AS (
    SELECT
        books.*,
        t.CHUNK as CHUNK
    FROM cortex_search_db.cortex_search_schema.books_dataset_raw books,
        TABLE(cortex_search_db.cortex_search_schema.books_chunk(books.description, books.title, books.authors, books.category, books.publisher)) t
);

SELECT chunk, * FROM book_description_chunks LIMIT 10;

SELECT * FROM cortex_search_db.cortex_search_schema.book_description_chunks;  -- cortex search service


------------------------cortex search service created--------------------------


CREATE OR REPLACE CORTEX SEARCH SERVICE CORTEX_SEARCH_DB.CORTEX_SEARCH_SCHEMA.BOOKS_DATASET_SERVICE
  ON DESCRIPTION
  ATTRIBUTES CATEGORY, PUBLISHER, AUTHORS , DESCRIPTION , TITLE
  WAREHOUSE = CORTEX_SEARCH_WH
  TARGET_LAG = '1 hour'
   AS (
        SELECT *
        FROM cortex_search_db.cortex_search_schema.book_description_chunks
   );

SHOW CORTEX SEARCH SERVICES;

----------------------------------------------------------------------------



----------------------------testing query ---------------------------------

SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'CORTEX_SEARCH_DB.CORTEX_SEARCH_SCHEMA.BOOKS_DATASET_SERVICE',
      '{
         "query": "AI and Machine Learning",
         "columns":[
            "DESCRIPTION",
            "TITLE",
            "AUTHORS","PUBLISHER"
         ],
         "filter": {"@eq": {"CATEGORY": " Technology & Engineering , Military Science"} },
         "limit":1
      }'
  )
)['results'] as results;


















