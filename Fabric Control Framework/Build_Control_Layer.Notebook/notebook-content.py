# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Lakehouse Control Tables Construction Notebook
# 
# ### Overview
# This notebook is dedicated to setting up the foundational Delta tables in the control layer of a lakehouse architecture. It includes the creation of several key tables: `source_system`, `object_metadata`, `field_metadata`, and `extraction_log`. Each table plays a vital role in managing metadata and logging for the data lakehouse operations.
# 
# ### Objective
# To create and define the structure of control tables which are essential for the efficient management, tracking, and orchestration of data flows within the lakehouse.
# 
# ### Usage
# This notebook is typically run as a setup or initialization step in the data lakehouse architecture, setting up necessary metadata tables for future data operations and logging.

# MARKDOWN ********************

# ## Creation of Source System Control Table
# 
# ### Purpose
# Defines and creates the `source_system` table in the Delta format. This table stores metadata about different source systems feeding data into the lakehouse.
# 
# ### Schema
# - `source_id`: String (Unique identifier for each source system)
# - `source_name`: String (Name of the source system)
# - `source_type`: String (Type of the source system)
# - `secure_connection`: String (Details about the secure connection)
# - `keyvault_name`: String (Name of the keyvault used for storing secure_connection)
# 
# ### Usage
# Run this cell to initialize the `source_system` table. It is a foundational step in setting up the control environment for data integration processes.

# CELL ********************

# Create source_system control table
spark.sql("""
CREATE TABLE control.source_system (
    source_id STRING,
    source_name STRING,
    source_type STRING,
    secure_connection STRING,
    keyvault_name STRING
) USING DELTA
""")

# MARKDOWN ********************

# ## Creation of Object Metadata Table
# 
# ### Purpose
# This cell creates the `object_metadata` table, which holds metadata about various data objects (like tables, files) from different sources.
# 
# ### Schema
# - `object_id`: String (Unique identifier for each data object)
# - `source_id`: String (Reference to the source system)
# - `object_schema`: String (Schema of the data object)
# - `object_name`: String (Name of the data object)
# - `active`: Boolean (Indicates if the object is active)
# - `incremental`: Boolean (Indicates if the object supports incremental loading)
# - `incremental_reload`: Boolean (Indicates if incremental reload is applicable)
# - `fabric_lakehouse_load`: Boolean (Indicates if the object is part of fabric lakehouse loading)
# - `fabric_warehouse_load`: Boolean (Indicates if the object is part of fabric warehouse loading)
# - `databricks_lakehouse_load`: Boolean (Indicates if the object is part of Databricks lakehouse loading)
# - `load_group`: Int (Group identifier for loading)
# - `extraction_where_query`: String (Conditional query for data extraction)
# - `watermark_column`: String (Column used for watermarking in incremental loads)
# - `watermark_type`: String (Data type of the watermark column)
# - `candidate_key`: String (Key used for identifying unique records)
# 
# ### Usage
# Execute this cell to set up the `object_metadata` table, crucial for tracking and managing different data objects in the lakehouse architecture.

# CELL ********************

# Create ObjectMetadata table
spark.sql("""
CREATE TABLE control.object_metadata (
    object_id STRING,
    source_id STRING,
    object_schema STRING,
    object_name STRING,
    active BOOLEAN,
    incremental BOOLEAN,
    incremental_reload BOOLEAN,
    fabric_lakehouse_load BOOLEAN,
    fabric_warehouse_load BOOLEAN,
    databricks_lakehouse_load BOOLEAN,
    load_group INT,
    extraction_where_query STRING,
    watermark_column STRING,
    watermark_type STRING,
    candidate_key STRING
) USING DELTA
""")

# MARKDOWN ********************

# ## Creation of Field Metadata Table
# 
# ### Purpose
# Establishes the `field_metadata` table, which maintains detailed metadata at the field level for each data object.
# 
# ### Schema
# - `field_id`: String (Identifier for each field)
# - `object_id`: String (Reference to the data object)
# - `source_id`: String (Reference to the source system)
# - `active`: Boolean (Indicates if the field is active)
# - `column_ordinal`: Integer (Position of the column in the source data)
# - `source_field_name`: String (Name of the field in the source system)
# - `source_field_type`: String (Data type of the field in the source system)
# - `is_nullable`: Boolean (Indicates if the field can contain null values)
# - `is_primary_key`: Boolean (Indicates if the field is a primary key)
# - `target_field_name`: String (Name of the field in the target system)
# - `target_field_type`: String (Data type of the field in the target system)
# 
# ### Usage
# This cell is essential for creating a comprehensive metadata repository at the field level, facilitating fine-grained management and understanding of data structures.

# CELL ********************

# Create FieldMetadata table
spark.sql("""
CREATE TABLE control.field_metadata (
    field_id STRING,
    object_id STRING,
    source_id STRING,
    active BOOLEAN,
    column_ordinal INT,
    source_field_name STRING,
    source_field_type STRING,
    is_nullable BOOLEAN,
    is_primary_key BOOLEAN,
    target_field_name STRING,
    target_field_type STRING
) USING DELTA
""")

# MARKDOWN ********************

# ## Creation of Extraction Log Table
# 
# ### Purpose
# Creates the `extraction_log` table, designed to log details of data extraction processes from various sources.
# 
# ### Schema
# - `extraction_id`: String (Unique identifier for each extraction process)
# - `source_id`: String (Reference to the source system)
# - `object_id`: String (Reference to the data object)
# - `extraction_status`: String (Status of the extraction process)
# - `execution_id`: String (Identifier for the execution instance)
# - `execution_start_time`: Timestamp (Start time of the extraction execution)
# - `execution_end_time`: Timestamp (End time of the extraction execution)
# - `rows_read`: BigInt (Number of rows read during extraction)
# - `copy_duration`: Float (Duration of the extraction process)
# - `extraction_success`: Boolean (Indicates if the extraction was successful)
# - `errors`: String (Details of any errors encountered)
# - `old_watermark_date`: Timestamp (Previous watermark date for incremental loads)
# - `watermark_date`: Timestamp (Current watermark date for incremental loads)
# - `sink_location`: String (Location where the extracted data is stored)
# 
# ### Usage
# Run this cell to enable detailed logging of extraction processes, which is crucial for monitoring, troubleshooting, and optimizing data ingestion workflows.

# CELL ********************

# Create ExtractionLog table
spark.sql("""
CREATE TABLE control.extraction_log (
    extraction_id STRING,
    source_id STRING,
    object_id STRING,
    extraction_status STRING,
    execution_id STRING,
    execution_start_time TIMESTAMP,
    execution_end_time TIMESTAMP,
    rows_read BIGINT,
    copy_duration FLOAT,
    extraction_success BOOLEAN,
    errors STRING,
    old_watermark_date TIMESTAMP,
    watermark_date TIMESTAMP,
    sink_location STRING
) USING DELTA
""")
