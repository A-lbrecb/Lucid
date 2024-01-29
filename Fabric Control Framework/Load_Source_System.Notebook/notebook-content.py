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

# ## Data Engineering Notebook for Source System Configuration
# 
# ### Overview
# This notebook is dedicated to managing source system configurations within the control framework. It is designed to interact with Delta Lake tables, specifically focusing on the control and management of source systems. The notebook includes the definition of the `SourceSystemConfig` class, which is pivotal for handling source system configurations and executing data processing tasks.
# 
# ### Key Components
# - **SourceSystemConfig Class**: A class designed to interact with the control layer, fetching details about source systems.
# - **Data Processing Logic**: Code for initializing Spark sessions, checking the existence of tables, and performing data merging operations in Delta tables.
# 
# ### Objective
# The primary objective of this notebook is to streamline the process of managing and updating source system configurations in a Delta Lake environment. This includes the creation of new configurations and the updating of existing ones.
# 
# ### Usage
# This notebook is used by data engineers and architects to ensure efficient and accurate data processing and management in a cloud-based environment.


# MARKDOWN ********************

# ## Parameters Initialization
# 
# ### Purpose
# This cell initializes the parameters required for processing the source system configurations. These parameters include details like source name, source type, and connection information.
# 
# ### Usage
# Modify the parameters in this cell according to the specific requirements of the source system being configured or updated.

# CELL ********************

# Assigning the parameters to variables
source_name = "Lucid_Sample_Data"
source_type = "SQL"
keyvault_name = 'lucid-control-akv-dev-01'
connection_details = "lucid-sample-sqldb-dev-01-JDBC-Connection"

# MARKDOWN ********************

# ## Source System Delta Table Operations
# 
# ### Purpose
# Checks the existence of a Delta table. If the table doesn't exist, it creates a new one. Then, it prepares a DataFrame for merging with the Delta table.
# 
# ### Usage
# Receives parameter input and executes a merge to load source system configuration.
# 
# ### Implementation
# - **Delta Table Check**: Checking for the existence of the 'control.source_system' Delta table and creating it if it does not exist.
# - **Data Merging**: Preparing and merging data into the Delta table with specified logic for handling matched and unmatched records.

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import expr

# Create the table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS control.source_system (
    source_id STRING,
    source_name STRING,
    source_type STRING,
    secure_connection STRING,
    keyvault_name STRING
) USING DELTA
""")


# Create a DataFrame with the parameter values
data_to_merge = [(source_name, source_type, keyvault_name, connection_details)]
columns = ["source_name", "source_type", "keyvault_name", "secure_connection"]
df = spark.createDataFrame(data_to_merge, columns)

# Generate a UUID for the new data
df = df.withColumn("source_id", expr("uuid()"))

# Merge statement
deltaTable = DeltaTable.forName(spark, "control.source_system")
(deltaTable
    .alias("target")
    .merge(
        df.alias("source"),
        "target.source_name = source.source_name AND target.source_type = source.source_type")
    .whenMatchedUpdate(set={
        "target.secure_connection": "source.secure_connection",
        "target.keyvault_name": "source.keyvault_name"
    })  # Update only these columns
    .whenNotMatchedInsertAll()
    .execute()
)
