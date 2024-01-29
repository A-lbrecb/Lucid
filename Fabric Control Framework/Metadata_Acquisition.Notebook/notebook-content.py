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

# # Metadata Acquisition Notebook
# 
# ## Overview
# This notebook is designed for the acquisition, transformation, and loading of metadata into a Delta table. It is part of a larger data engineering workflow, focusing on managing and processing metadata from various source systems. This notebook includes steps for data loading, transformation, joining with existing metadata tables, and finally storing the processed data in a structured format.
# 
# ## Objectives
# - Load and transform metadata from source systems
# - Perform data cleaning and preparation for integration
# - Join with existing metadata structures for enrichment
# - Load the transformed data into a Delta table for further use
# 
# ## Prerequisites
# - Access to source metadata
# - Spark environment setup
# - Necessary libraries (PySpark, Delta tables, etc.)
# 
# ## Expected Outcomes
# - A clean, transformed, and enriched metadata dataset ready for analysis and reporting


# MARKDOWN ********************

# ### Import Libraries
# - Purpose: Import necessary Python libraries and modules for data manipulation and interaction with cloud data services.
# - Libraries include PySpark for data processing, Delta tables for managing versioned data, and PySpark SQL functions for data manipulation.

# CELL ********************

%run Utils

# MARKDOWN ********************

# ## Retrieve Secure Connection Reference
# 
# The following cell accepts a parameter input for the source name.

# PARAMETERS CELL ********************

# Assigning the parameters to variables
source_name = 'Lucid_Sample_Data'

# MARKDOWN ********************

# ## Determine Source System Type
# 
# The following cell queries the source system control table to determine the source type and processing pattern for metadata capture

# CELL ********************

# Build query to determine source type
source_config = f"""
            SELECT 
                source_name,
                source_type,
                keyvault_name,
                secure_connection
            FROM 
                control.source_system
            WHERE 
                source_name = '{source_name}'
        """

# Execute source type configuration query
source_config = spark.sql(source_config)

# Retrieve the first row of the DataFrame
source_config = source_config.first()

# Preview records
print(source_config)

# MARKDOWN ********************

# ### Retrieving Connection String from Azure Key Vault
# 
# This code snippet demonstrates how to retrieve a JDBC connection string securely stored in Azure Key Vault. It uses a custom function `get_secret_with_token` provided by `PyTridentTokenLibrary`, along with `mssparkutils` for Azure token retrieval.

# CELL ********************

# Assign values to keyvault variables
keyvault_name = source_config['keyvault_name']
secure_connection = source_config['secure_connection']

# Create an instance of the KeyVaultConnector class
keyvault_connector = KeyVaultConnector()

# Retrieve the connection string
connection_string = keyvault_connector.get_connection_details(keyvault_name, secure_connection)

# MARKDOWN ********************

# ### Execute a Metadata Acquisition SQL Query Using JDBC in a Spark Session
# 
# This code snippet demonstrates how to execute a SQL query to retrieve metadata from a database using JDBC in a Spark session. It assumes that the full JDBC connection string is securely retrieved and stored in `secret_value`. The JDBC connection string includes credentials and other connection details necessary for accessing the database.

# CELL ********************

# Assign values to source type variable
source_type = source_config['source_type']
connection_details = connection_string

# Use the factory method to get the appropriate metadata acquisition class type
metadata_acquisition_class = SourceMetadataAcquisition.get_metadata_acquisition_class(source_type)

# Create an instance of the specific metadata acquisition class
source_metadata_instance = metadata_acquisition_class(connection_details)

# Retrieve the source system metadata
source_metadata_df = source_metadata_instance.fetch_metadata()

# Display the DataFrame for verification
display(source_metadata_df)

# MARKDOWN ********************

# ### Populating the `object_metadata` Table
# 
# This section of the notebook is dedicated to populating the `object_metadata` table. The process entails several key steps to ensure the table is accurately and efficiently updated with metadata from source systems:
# 
# 1. **Metadata Extraction**: Metadata is directly obtained from the source system using a JDBC connection. This metadata encompasses details such as object schema, object name, and other pertinent attributes.
# 
# 2. **Join with Source System**: The extracted metadata undergoes a join operation with the `source_system` table. This step is essential for correlating each object with its respective source system, denoted by `source_system_id`.
# 
# 3. **Data Preparation**: The data is prepared for the upsert process. This involves selecting the necessary fields, assigning default values to specific columns (like `active`, `incremental`, etc.), and generating unique identifiers for each object using the `uuid()` function.
# 
# 4. **Upsert into Delta Table**: An upsert (merge) operation is executed on the `control.object_metadata` Delta table. The upsert logic is as follows:
#    - **Insertion of New Records**: If an object is present in the source metadata but not in the `object_metadata` table, it's inserted as a new record.
#    - **Update for Non-existent Source Objects**: For objects that exist in the `object_metadata` table but are not found in the current source metadata, the `active` status is set to `False` (0). This update ensures that objects no longer present in the source system are marked accordingly.
#    - **No Update for Existing Records**: Existing records in the `object_metadata` table that match the source metadata are not updated.
# 
# This methodical approach ensures that the `object_metadata` table is kept current with the latest information from the source systems. It upholds data integrity and consistency while efficiently managing changes in the source system metadata.

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Load the source_system table
source_system = spark.table("control.source_system")

# Extract distinct object list from the direct metadata query
distinct_metadata_objects = source_metadata_df.select(
    "object_schema",
    "object_name",
    F.lit(source_name).alias("source_system_name")  # Assuming source_name is known
).distinct()

# Join to get the source_id
joined_data = distinct_metadata_objects.join(
    source_system,
    distinct_metadata_objects.source_system_name == source_system.source_name,
    "left"
).select(
    F.expr("uuid()").alias("object_id"),
    "source_id",
    "object_schema",
    "object_name",
    F.lit(True).alias("active"),
    F.lit(False).alias("incremental"),
    F.lit(False).alias("incremental_reload"),
    F.lit(True).alias("fabric_lakehouse_load"),
    F.lit(False).alias("fabric_warehouse_load"),
    F.lit(False).alias("databricks_lakehouse_load"),
    F.lit(1).alias("load_group"),
    F.lit(None).cast("string").alias("extraction_where_query"),
    F.lit(None).cast("string").alias("watermark_column"),
    F.lit(None).cast("string").alias("watermark_type"),
    F.lit(None).cast("string").alias("candidate_key")
)

# Define the Delta table for object_metadata
object_metadata_delta = DeltaTable.forName(spark, "control.object_metadata")

# Execute upsert (merge) operation
object_metadata_delta.alias("target").merge(
    joined_data.alias("source"),
    "target.source_id = source.source_id AND target.object_schema = source.object_schema AND target.object_name = source.object_name"
).whenMatchedUpdate(
    condition="source.object_id IS NULL",
    set={"active": F.lit(False)}
).whenNotMatchedInsertAll().execute()

# MARKDOWN ********************

# ### Populating the `field_metadata` Table
# 
# This section of the notebook is devoted to updating the `field_metadata` table. The updated process includes the following steps:
# 
# 1. **Direct Metadata Retrieval**: Initiates by extracting field-level metadata directly from the source system, which includes information such as field names, types, and other relevant details.
# 
# 2. **Complex Joins**: The extracted metadata is joined with the `object_metadata` table. This join is crucial to map each field to the corresponding object in the system, identified by `object_id` and `source_id`.
# 
# 3. **Data Transformation**: The joined data is transformed to fit the structure of the `field_metadata` table. This transformation includes selecting the necessary fields, assigning default values, generating unique field identifiers using `uuid()`, and other necessary data preparations.
# 
# 4. **Merge Operation**: Performs a refined upsert (merge) operation into the `control.field_metadata` Delta table. The merge logic is as follows:
#    - **Insertion of New Records**: When a field is present in the source metadata but not in the `field_metadata` table, it is inserted as a new record.
#    - **Update for Inactive Fields**: If a field exists in the `field_metadata` table but not in the current source metadata, the `active` status is updated to `False` (0), indicating that the field is no longer active in the source system.
#    - **No Update for Existing Active Fields**: Existing active records in the `field_metadata` table that match the source metadata are not modified.
# 
# This approach ensures that the `field_metadata` table is consistently updated to reflect the most current field-level structure of the source systems. It effectively manages the synchronization of field metadata, maintaining data integrity and accuracy.

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Load the object_metadata tables
object_metadata = spark.table("control.object_metadata")

# Join to get the source_id and object_id
joined_data = source_metadata_df.join(
    object_metadata,
    (source_metadata_df["object_schema"] == object_metadata["object_schema"]) & 
    (source_metadata_df["object_name"] == object_metadata["object_name"]),
    "left"
).select(
    F.expr("uuid()").alias("field_id"),
    "object_metadata.object_id",
    "object_metadata.source_id",
    F.lit(True).alias("active"),
    "column_ordinal",
    "source_field_name",
    "source_field_type",
    "is_nullable",
    "is_primary_key",
    F.lit(None).cast("string").alias("target_field_name"),
    F.lit(None).cast("string").alias("target_field_type")
)

# Define the Delta table for field_metadata
field_metadata_delta = DeltaTable.forName(spark, "control.field_metadata")

# Execute upsert (merge) operation
field_metadata_delta.alias("target").merge(
    joined_data.alias("source"),
    "target.source_id = source.source_id AND target.object_id = source.object_id AND target.source_field_name = source.source_field_name"
).whenMatchedUpdate(
    condition="target.active = True AND source.field_id IS NULL",
    set={"active": F.lit(False)}
).whenNotMatchedInsertAll().execute()
