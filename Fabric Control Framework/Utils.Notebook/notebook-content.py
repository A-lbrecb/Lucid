# Synapse Analytics notebook source


# MARKDOWN ********************

# ## SourceSystemConfig Class
# 
# ### Purpose
# The `SourceSystemConfig` class is essential for retrieving source system configuration from the control layer. It's particularly useful for executing Spark SQL queries and managing data processing tasks.
# 
# ### Usage
# This class is instantiated to interact with the control layer, providing functionalities to fetch detailed information about source systems. The `get_source_details` and `get_processing_list` methods are pivotal for acquiring configuration and processing details.
# 
# ### Methods
# - `__init__(self)`: Initializes the `SourceSystemConfig` class, setting up a Spark session.
# - `get_source_details(self, source_name)`: Fetches configuration details for a given source system. It takes `source_name` as an argument and returns a DataFrame with the configuration details.
# - `get_processing_list(self, source_name, load_group)`: Gathers processing information for a specified source system and load group. This method returns a DataFrame containing detailed configuration information.
# 
# ### Example Usage
# ```python
# # Initialization of the SourceSystemConfig class
# source_system_config = SourceSystemConfig()
# 
# # Retrieving configuration details for a specific source system
# source_name = "YourSourceSystemName"  # Replace with the actual source system name
# config_details_df = source_system_config.get_source_details(source_name)
# config_details_df.show()
# 
# # Fetching processing list details
# load_group = 1  # Specify the load group
# processing_list_df = source_system_config.get_processing_list(source_name, load_group)
# processing_list_df.show()

# CELL ********************

class SourceSystemConfig:
    """
    Class for retrieving source system configuration from the control layer.

    Attributes:
        spark_session: A Spark session object to execute Spark SQL queries.
    """

    def __init__(self):
        """
        Initialize the SourceSystemConfig class with a Spark session.

        """
        self.spark = spark

    def get_source_details(self, source_name):
        """
        Retrieves configuration details for a specified source system.

        Args:
            source_name (str): The name of the source system.

        Returns:
            DataFrame: A Spark DataFrame containing the configuration details of the specified source system.
        """
        query = f"""
            SELECT 
                source_id,
                source_name,
                source_type,
                keyvault_name,
                secure_connection
            FROM 
                control.source_system
            WHERE 
                source_name = '{source_name}'
        """
        return self.spark.sql(query)
    
    def get_processing_list(self, source_name, load_group):
        """
        Retrieves configuration details for a specified source system.

        Args:
            source_name (str): The name of the source system.
            load_group (int): The load group identifier.

        Returns:
            DataFrame: A Spark DataFrame containing the configuration details of the specified source system.
        """
        query = f"""
            SELECT 
                om.object_id,
                ss.source_id,
                ss.source_name,
                om.object_schema,
                om.object_name,
                om.active,
                ss.source_type,
                om.incremental,
                om.incremental_reload,
                om.watermark_column,
                om.watermark_type,
                COALESCE(el.watermark_date, '1900-01-01 00:00:00') AS watermark_date,
                om.extraction_where_query,
                om.load_group,
                om.fabric_lakehouse_load,
                om.fabric_warehouse_load,
                om.databricks_lakehouse_load,
                FL.spark_column_names,
                ss.keyvault_name,
                ss.secure_connection
            FROM 
                control.object_metadata om
            JOIN 
                control.source_system ss ON om.source_id = ss.source_id
            LEFT JOIN (
                SELECT 
                    object_id, 
                    MAX(watermark_date) AS watermark_date
                FROM 
                    control.extraction_log
                WHERE 
                    extraction_success = True
                GROUP BY 
                    object_id
            ) el ON om.object_id = el.object_id
            LEFT JOIN (
                SELECT 
                    fm.object_id, 
                    CONCAT_WS(',', COLLECT_LIST(CASE 
                        WHEN LEFT(fm.source_field_name, 1) <> '[' THEN CONCAT('[', fm.source_field_name, ']') 
                        ELSE fm.source_field_name 
                    END)) AS spark_column_names
                FROM 
                    control.field_metadata fm
                WHERE 
                    fm.active = True
                GROUP BY 
                    fm.object_id
            ) FL ON om.object_id = FL.object_id
            WHERE 
                om.active = True
                AND ss.source_name = '{source_name}'
                AND om.load_group = {load_group}
        """
        return self.spark.sql(query)

# MARKDOWN ********************

# ## KeyVaultConnector Class
# 
# ### Purpose
# The `KeyVaultConnector` class is designed to retrieve connection details securely from Azure Key Vault. It's crucial for maintaining secure access to database connections and other sensitive information stored in Key Vault.
# 
# ### Usage
# This class is used to fetch connection strings or other secrets from Azure Key Vault. The `get_connection_details` method facilitates retrieving the necessary information using the Key Vault name and the secret's name.
# 
# ### Methods
# - `__init__(self)`: Initializes the `KeyVaultConnector` class.
# - `get_connection_details(self, keyvault_name, secure_connection)`: Retrieves the connection string from Azure Key Vault. It takes `keyvault_name` and `secure_connection` (name of the secret) as arguments and returns the connection string.
# 
# ### Example Usage
# ```python
# # Create an instance of KeyVaultConnector
# keyvault_connector = KeyVaultConnector()
# 
# # Retrieve connection details from Azure Key Vault
# keyvault_name = "YourKeyVaultName"  # Replace with your Key Vault name
# secure_connection = "YourSecretName"  # Replace with your secret name
# connection_string = keyvault_connector.get_connection_details(keyvault_name, secure_connection)
# 
# print("Connection String:", connection_string)

# CELL ********************

class KeyVaultConnector:
    """
    Class for retrieving connection details from Azure Key Vault.

    Attributes:
        None
    """

    def __init__(self):
        """
        Initialize the KeyVaultConnector class.
        """
        pass

    def get_connection_details(self, keyvault_name, secure_connection):
        """
        Retrieves the connection string from Azure Key Vault.

        Args:
            keyvault_name (str): The name of the key vault.
            secure_connection (str): The name of the secret in the key vault.

        Returns:
            str: The connection string retrieved from Azure Key Vault.
        """
        
        from trident_token_library_wrapper import PyTridentTokenLibrary as tl
        import logging
        
        print("Retrieving secret from Key Vault")

        # Retrieve the access token for Azure Key Vault
        access_token = mssparkutils.credentials.getToken("keyvault")

        # Construct the vault URL using the keyvault_name
        print("Building URL for Key Vault")
        vault_url = f"https://{keyvault_name}.vault.azure.net/"

        # Retrieve the connection string from Azure Key Vault
        connection_string = tl.get_secret_with_token(vault_url, secure_connection, access_token)
        
        logging.info(f"Connection String: {connection_string}")
        print("Connection string retrieved from Key Vault")

        return connection_string

# MARKDOWN ********************

# ## SourceMetadataAcquisition Class
# 
# ### Purpose
# The `SourceMetadataAcquisition` class serves as a base for acquiring metadata from various source types. It's designed to be extended by subclasses for specific source types, such as SQL, providing a framework for metadata retrieval.
# 
# ### Usage
# This base class is not used directly but is extended by other classes. The static method `get_metadata_acquisition_class` is used to obtain the appropriate subclass based on the source type.
# 
# ### Methods
# - `__init__(self)`: Initializes the `SourceMetadataAcquisition` base class.
# - `get_metadata_acquisition_class(source_type)`: Static method to get the appropriate metadata acquisition class based on the provided `source_type`.
# 
# ### Example Usage
# ```python
# # Example of how to use get_metadata_acquisition_class
# source_type = "SQL"  # Specify the source type
# metadata_class = SourceMetadataAcquisition.get_metadata_acquisition_class(source_type)
# 
# # Assuming SqlServerMetadataAcquisition is a subclass of SourceMetadataAcquisition
# metadata_instance = metadata_class()
# metadata = metadata_instance.fetch_metadata()
# # Process metadata as required

# CELL ********************

class SourceMetadataAcquisition:
    def __init__(self):
        """
        Initializes the SourceMetadataAcquisition base class.
        """
        pass

    @staticmethod
    def get_metadata_acquisition_class(source_type):
        """
        Factory method to get the appropriate metadata extractor based on the source type.

        Args:
            connection_details (str): Connection details for the source system.
        Returns:
            DataExtractor: An instance of a subclass of DataExtractor.
        """
        # Check the source type and return the appropriate extractor
        if source_type == "SQL":
            return SqlServerMetadataAcquisition
        # Add more conditions here for other source types
        else:
            raise ValueError("Unsupported source type")

# MARKDOWN ********************

# ## SqlServerMetadataAcquisition Class
# 
# ### Purpose
# The `SqlServerMetadataAcquisition` class, inheriting from `SourceMetadataAcquisition`, is specifically designed to fetch metadata from SQL Server databases. It uses JDBC for establishing connections and retrieving metadata, making it an essential component for SQL Server integrations.
# 
# ### Usage
# This class is utilized for connecting to SQL Server and fetching metadata like schema, table names, column details, and primary key information. It extends the `SourceMetadataAcquisition` class and implements the `fetch_metadata` method for SQL Server.
# 
# ### Methods
# - `fetch_metadata(self, connection_details)`: Fetches metadata from SQL Server using the provided JDBC connection string. The method returns a DataFrame containing the metadata. It expects `connection_details` as an argument, which is the JDBC connection string for SQL Server.
# 
# ### Example Usage
# ```python
# # Assuming an instance of SqlServerMetadataAcquisition is created
# sql_server_metadata = SqlServerMetadataAcquisition()
# 
# # JDBC connection string for SQL Server
# connection_details = "your_jdbc_connection_string_here"
# 
# # Fetching metadata from SQL Server
# metadata_df = sql_server_metadata.fetch_metadata(connection_details)
# 
# # Display the DataFrame for verification
# metadata_df.show()


# CELL ********************

class SqlServerMetadataAcquisition(SourceMetadataAcquisition):
    def __init__(self, connection_details):
        
        # Call the superclass constructor
        super().__init__()
        
        self.connection_details = connection_details
        print("SqlServerMetadataAcquisition initialized.")
    
    def fetch_metadata(self):
        """
        Fetches metadata from SQL Server using JDBC.

        Args:
            connection_details (str): JDBC connection string for SQL Server.

        Returns:
            DataFrame: A DataFrame containing the metadata.
        """
        
        from pyspark.sql import SQLContext
        
        properties = {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        query = """
        SELECT
            SCHEMA_NAME(tbl.schema_id) AS object_schema,
            tbl.name AS object_name,
            clm.column_id AS column_ordinal,
            clm.name AS source_field_name,
            TYPE_NAME(clm.user_type_id) AS source_field_type,
            clm.is_nullable AS is_nullable,
            CASE 
                WHEN ic.is_primary_key = 1 THEN CAST(1 AS BIT) 
                ELSE CAST(0 AS BIT) 
            END AS is_primary_key
        FROM 
            sys.tables AS tbl
        INNER JOIN 
            sys.columns AS clm ON tbl.object_id = clm.object_id
        LEFT OUTER JOIN 
            (SELECT 
                ic.object_id, 
                ic.column_id,
                i.is_primary_key
             FROM 
                sys.index_columns AS ic
             INNER JOIN 
                sys.indexes AS i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             WHERE 
                i.is_primary_key = 1
            ) AS ic ON clm.object_id = ic.object_id AND clm.column_id = ic.column_id
        WHERE 
            tbl.type = 'U'
        """

        return sqlContext.read.jdbc(url=connection_details, table=f"({query}) AS myTable", properties=properties)

# MARKDOWN ********************

# ## DataExtractor Base Class
# 
# ### Purpose
# The `DataExtractor` class serves as a foundational base class for data extraction processes. It defines a standard interface and common functionalities that can be inherited and specialized by subclasses for various data sources.
# 
# ### Class Definition
# - The class includes a constructor for initializing with connection details, applicable to all data sources.
# - It declares an abstract method `extract_data` that must be implemented by subclasses to handle specific extraction logic for different source types.
# 
# ### Method: extract_data
# - This is an abstract method intended to be overridden by subclasses.
# - It outlines the structure for how data should be extracted, ensuring consistency across different source types.
# 
# ### Usage
# - `DataExtractor` is not intended to be instantiated directly but rather to be extended by subclasses that implement the specific data extraction logic for each source type.
# 
# ### Example Usage
# ```python
# # Example subclass implementation
# class MyDataExtractor(DataExtractor):
#     def extract_data(self, object_row):
#         # Implementation for a specific data source
#         pass
# 
# # Example of using the subclass
# extractor = MyDataExtractor(connection_details)
# extractor.extract_data(object_row)

# CELL ********************

class DataExtractor:
    def __init__(self):
        """
        Initializes the DataExtractor with connection details.

        """
        pass

    @staticmethod
    def get_data_extractor(source_type, connection_details):
        """
        Factory method to get the appropriate data extractor based on the source type.

        Args:
            source_type (str): The type of the data source.
            connection_details (str): Connection details for the source system.

        Returns:
            DataExtractor: An instance of a subclass of DataExtractor.
        """
        # Check the source type and return the appropriate extractor
        if source_type == 'SQL':
            return SQLDataExtractor(connection_details)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

# MARKDOWN ********************

# ## SQLDataExtractor Subclass
# 
# ### Purpose
# The `SQLDataExtractor` class, a subclass of `DataExtractor`, is tailored for extracting data from SQL Server sources. It includes specific logic for handling SQL queries, incremental loads, and watermark processing.
# 
# ### Class Functionality
# - Inherits from `DataExtractor` and implements the `extract_data` method.
# - Handles SQL Server-specific data extraction, including the construction and execution of SQL queries.
# - Manages incremental data extraction based on watermark information.
# 
# ### Method: extract_data
# - Overrides the base class method to implement SQL extraction logic.
# - Takes additional parameters for logging and data loading, utilizing `ExtractionLogger` and `DataLoader` classes.
# 
# ### Usage
# - Instantiate `SQLDataExtractor` with connection details for a SQL Server.
# - Call the `extract_data` method with the necessary parameters, including an object row from the source configuration, to perform the extraction.
# 
# ### Example Usage
# ```python
# # Initialization with connection details
# sql_extractor = SQLDataExtractor(connection_details)
# 
# # Extracting data for a given object row
# sql_extractor.extract_data(object_row, logger_instance, data_loader_instance)

# CELL ********************

import uuid
import datetime as dt
import logging

class SQLDataExtractor(DataExtractor):
    def __init__(self, connection_details):
        
        # Call the superclass constructor
        super().__init__()
        
        self.connection_details = connection_details
        print("SQLDataExtractor initialized.")

    def extract_data(self, object_row, logger, data_loader):
        """
        Implements the data extraction process for SQL data sources.

        Args:
            object_row (dict): A dictionary containing object metadata.
            logger (ExtractionLogger): An instance of the ExtractionLogger for logging.
            data_loader (DataLoader): An instance of DataLoader to load the extracted data.
        """
        # Print the start of the extraction process
        print(f"Extracting data for object_id: {object_row['object_id']}")
        
        logging.info(f"Starting processing for object_id: {object_row['object_id']}")
        
        # Validate required fields in object_row
        print("Checking required fields have been provided.")
        
        required_fields = ['source_id', 'object_id', 'object_schema', 'object_name', 'incremental', 'spark_column_names', 'fabric_lakehouse_load']
        missing_fields = [field for field in required_fields if field not in object_row or object_row[field] is None]
        if missing_fields:
            raise ValueError(f"Missing required fields in object_row: {', '.join(missing_fields)}")

        # Print the start of the extraction process
        print(f"Extracting data for object_id: {object_row['object_id']}")
        
        logging.info(f"Starting processing for object_id: {object_row['object_id']}")

        # Assign default variables for processing
        print("Initializing and assigning default variables for processing.")
        
        extraction_id = str(uuid.uuid4())
        execution_start_time = dt.datetime.now()
        new_watermark_date = None
        error_message = None
        rows_read = None

        # Begin try logic for data processing
        try:
            print(f"Starting processing for object_id: {object_row['object_id']}.")
            
            logging.info(f"Starting processing for object_id: {object_row['object_id']}")

            # Assign variables from object_row
            print("Assigning variables from provided object row.")
            
            source_id = object_row['source_id']
            object_id = object_row['object_id']
            object_schema = object_row['object_schema']
            object_name = object_row['object_name']
            incremental = object_row['incremental']
            watermark_date = object_row['watermark_date']
            watermark_column = object_row['watermark_column']
            extraction_where_query = object_row['extraction_where_query']
            spark_column_names = object_row['spark_column_names']
            fabric_lakehouse_load = object_row['fabric_lakehouse_load']
            delta_table_name = f"bronze.{object_row['object_schema']}_{object_row['object_name']}"

            # Check incremental processing logic
            print("Checking incremental processing requirements.")

            if incremental == 1:
                
                # Building incremental watermark query
                print("Incremental processing required. Building incremental watermark retrieval query.")
                
                watermark_query = f" WHERE {watermark_column} > '{watermark_date}'"
                if extraction_where_query:
                    watermark_query += f" AND {extraction_where_query}"

                watermark_sql_query = f"""
                    SELECT 
                        MAX({watermark_column}) AS watermark
                    FROM 
                        [{object_schema}].[{object_name}]
                    {watermark_query}
                """
                
                # Executing watermark query against source
                print("Executing watermark retrieval query")

                watermark_df = spark.read.format("jdbc").option("url", self.connection_details).option("query", watermark_sql_query).load()
                if watermark_df.count() > 0:
                    watermark_row = watermark_df.collect()[0][0]
                    if watermark_row is not None:
                        new_watermark_date = watermark_row.strftime('%Y-%m-%d %H:%M:%S')

            # Building dynamic data acquisition query
            print("Building dynamic data acquisition query.")
            
            dynamic_sql_query = f"SELECT {spark_column_names} FROM [{object_schema}].[{object_name}]"
            if incremental and new_watermark_date:
                dynamic_sql_query += f" WHERE {watermark_column} > '{new_watermark_date}'"
            if extraction_where_query:
                dynamic_sql_query += f" AND {extraction_where_query}"

            # Executing data acquisition query
            print("Executing data acquisition query against source system.")
            
            data_df = spark.read.format("jdbc").option("url", self.connection_details).option("query", dynamic_sql_query).load()
            # data_df.show(5)

            # If data loading is required
            if object_row['fabric_lakehouse_load'] == 1:
                data_loader.load_data(data_df, object_row)

            # Record number of rows read from source
            rows_read = data_df.count()

        except Exception as e:
            error_message = str(e)
            logging.error(f"Error processing object_id: {object_row['object_id']}. Error: {error_message}")

        finally:
            execution_end_time = dt.datetime.now()
            success = error_message is None
            log_row_data = (extraction_id, object_row['object_id'], object_row['source_id'], None, execution_start_time, execution_end_time, rows_read, (execution_end_time - execution_start_time).total_seconds(), success, error_message, object_row['watermark_date'], new_watermark_date, delta_table_name, 'Succeeded' if success else 'Failed')
            logger.log_extraction_details(log_row_data)
            
            # Print the completion of the extraction process
            print(f"Extraction process completed for object_id: {object_row['object_id']}")

# MARKDOWN ********************

# ## DataLoader Base Class
# 
# ### Purpose
# The `DataLoader` class is designed for loading extracted data into different storage systems, such as a data lakehouse. It provides a flexible and extendable way to handle various data loading patterns.
# 
# ### Functionality
# - The class currently supports loading data into a Delta table, specifically handling the `fabric_lakehouse_load` pattern.
# - It can be extended to support additional data loading patterns and destinations as needed.
# 
# ### Class Definition
# - The class does not require specific initialization parameters for now but is structured to easily accommodate future enhancements.
# - The `load_data` method handles the actual data loading process based on the provided object metadata.
# 
# ### Method: load_data
# - This method takes a DataFrame (`data_df`) and a row of metadata (`object_row`) as arguments.
# - It checks the `fabric_lakehouse_load` flag in `object_row` to determine if the data should be loaded into the Delta table.
# - If `fabric_lakehouse_load` is set to 1, it loads the data into a Delta table formatted as `"bronze.{object_schema}_{object_name}"`.
# 
# ### Usage
# - Create an instance of `DataLoader` and pass it to the data extraction process, such as in an instance of `SQLDataExtractor`.
# - During data extraction, the `DataLoader` instance is used to load the data based on the specified loading pattern.
# 
# ### Example Initialization and Usage
# ```python
# data_loader = DataLoader()
# 
# # ... Data extraction logic ...
# 
# # Use DataLoader instance to load data
# if object_row['fabric_lakehouse_load'] == 1:
#     data_loader.load_data(extracted_data_df, object_row)

# CELL ********************

class DataLoader:
    def __init__(self):
        """
        Initializes the DataLoader base class.
        """
        pass

    def load_data(self, data_df, object_row):
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def get_data_loader(object_row):
        """
        Factory method to get the appropriate data loader based on the object_row.

        Args:
            object_row (dict): A dictionary containing object metadata.

        Returns:
            DataLoader: An instance of a subclass of DataLoader.
        """
        # Printing the object_row for diagnostic purposes
        # print("Object Row:", object_row)

        # Check if the key 'fabric_lakehouse_load' exists and its value
        fabric_lakehouse_load = object_row.get('fabric_lakehouse_load')
        print("Fabric Lakehouse Load:", fabric_lakehouse_load)
        
        print(f"Selecting DataLoader for object_row with ID: {object_row.get('object_id', 'N/A')}")
        
        # Example criterion for selecting the data loader
        if object_row['fabric_lakehouse_load'] == 1:
            return DeltaDataLoader()
        else:
            # Add more conditions for different data loaders if needed
            raise ValueError("Unsupported data loading type")

# MARKDOWN ********************

# ## DeltaDataLoader Subclass
# 
# ### Purpose
# The `DeltaDataLoader` subclass extends the `DataLoader` class, specifically focusing on loading data into Delta tables. It is tailored for scenarios where data is stored in a Delta Lake format, providing efficient and reliable data loading.
# 
# ### Functionality
# - This subclass overrides the `load_data` method to specifically handle the loading of data into Delta tables.
# - It constructs the Delta table name using schema and object name from the `object_row` and loads the data accordingly.
# 
# ### Subclass Definition
# - Inherits from the `DataLoader` class and leverages its factory pattern.
# - The overridden `load_data` method includes additional logic specific to Delta tables.
# 
# ### Method: load_data
# - This method takes a DataFrame (`data_df`) and a row of metadata (`object_row`) as arguments.
# - Constructs the target Delta table name using `"bronze.{object_schema}_{object_name}"` format.
# - Writes the data from the DataFrame to the Delta table, using overwrite mode for simplicity. 
# - Appropriate for scenarios where data refreshes or updates are frequent.
# 
# ### Usage
# - An instance of `DeltaDataLoader` is obtained through the `DataLoader` factory method, depending on the `fabric_lakehouse_load` flag.
# - It seamlessly integrates with existing data pipelines that utilize the `DataLoader` class.
# 
# ### Example Initialization and Usage
# ```python
# # Assuming object_row indicates loading into a Delta table
# delta_data_loader = DataLoader.get_data_loader(object_row)
# 
# # Data extraction logic...
# 
# # Use DeltaDataLoader instance to load data into a Delta table
# delta_data_loader.load_data(extracted_data_df, object_row)

# CELL ********************

class DeltaDataLoader(DataLoader):
    def load_data(self, data_df, object_row):
        """
        Loads data into a Delta table based on the object_row information.

        Args:
            data_df (DataFrame): The DataFrame containing the data to be loaded.
            object_row (dict): A dictionary containing object metadata.
        """
        # Assign target delta table location
        delta_table_name = f"bronze.{object_row['object_schema']}_{object_row['object_name']}"
        
        # Print the start of the data loading process
        print(f"Loading data into Delta table: {delta_table_name}")
        
        # Write the source data to the target delta table
        data_df.write.format("delta").mode("overwrite").saveAsTable(delta_table_name)
        
        # Print the end of the data loading process
        print(f"Data loaded into Delta table: {delta_table_name}")

# MARKDOWN ********************

# ## ExtractionLogger Class
# 
# ### Purpose
# The `ExtractionLogger` class is responsible for logging details of the data extraction process. It manages the creation and writing of log entries to a structured log table.
# 
# ### Class Definition
# - Provides methods to log extraction details, including start and end times, success status, errors, and other relevant metrics.
# - Utilizes a predefined schema (`log_schema`) for the log table.
# 
# ### Method: log_extraction_details
# - Logs details of each extraction process.
# - Takes a tuple of log data and writes it to a Delta table formatted according to `log_schema`.
# 
# ### Usage
# - Instantiate `ExtractionLogger` with the log schema.
# - Call `log_extraction_details` after each extraction process to record its details and outcomes.
# 
# ### Example Usage
# ```python
# # Initializing the logger with a schema
# logger = ExtractionLogger(log_schema)
# 
# # Logging extraction details
# log_data = (extraction_id, object_id, source_id, execution_id, start_time, end_time, rows_read, duration, success, error, old_watermark, new_watermark, sink_location, status)
# logger.log_extraction_details(log_data)

# CELL ********************

import logging
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BooleanType, LongType

class ExtractionLogger:
    # Define the schema for the logging DataFrame
    log_schema = StructType([
        StructField("extraction_id", StringType(), True),
        StructField("object_id", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("execution_id", StringType(), True),
        StructField("execution_start_time", TimestampType(), True),
        StructField("execution_end_time", TimestampType(), True),
        StructField("rows_read", LongType(), True),
        StructField("copy_duration", FloatType(), True),
        StructField("extraction_success", BooleanType(), True),
        StructField("errors", StringType(), True),
        StructField("old_watermark_date", TimestampType(), True),
        StructField("watermark_date", TimestampType(), True),
        StructField("sink_location", StringType(), True),
        StructField("extraction_status", StringType(), True)
    ])

    def log_extraction_details(self, log_row_data):
        # Unpack log_row_data
        (extraction_id, object_id, source_id, execution_id, 
         execution_start_time, execution_end_time, 
         rows_read, copy_duration, extraction_success, 
         error_message, old_watermark_date, new_watermark_date, 
         sink_location, extraction_status) = log_row_data

        # Convert string timestamps to datetime objects
        print("Converting appropriate fields to timestamps")
        execution_start_time = self.convert_to_datetime(execution_start_time)
        execution_end_time = self.convert_to_datetime(execution_end_time)
        old_watermark_date = self.convert_to_datetime(old_watermark_date)
        new_watermark_date = self.convert_to_datetime(new_watermark_date)

        # Create a new log row with the converted timestamps
        log_row = Row(extraction_id, object_id, source_id, execution_id, 
                      execution_start_time, execution_end_time, 
                      rows_read, copy_duration, extraction_success, 
                      error_message, old_watermark_date, new_watermark_date, 
                      sink_location, extraction_status)

        # Create a DataFrame from the log row with the specified schema
        log_df = spark.createDataFrame([log_row], schema=self.log_schema)
        # Write the log data to the specified table
        log_df.write.format("delta").mode("append").saveAsTable('control.extraction_log')

    @staticmethod
    def convert_to_datetime(timestamp_str):
        """
        Convert a timestamp string to a datetime object.

        Args:
            timestamp_str (str): Timestamp string.

        Returns:
            datetime: Converted datetime object or the original input if it's not a string.
        """
        if isinstance(timestamp_str, str):
            try:
                return dt.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print(f"Timestamp conversion error for string: {timestamp_str}")
        return timestamp_str

# MARKDOWN ********************

# ## ExtractionManager Class
# 
# ### Purpose
# The `ExtractionManager` class orchestrates the data extraction process, managing multiple extraction tasks simultaneously using threading.
# 
# ### Class Definition
# - Manages a queue of extraction tasks and distributes these tasks across multiple worker threads.
# - Accepts an instance of a `DataExtractor` subclass and a list of objects to be processed.
# 
# ### Methods
# - `enqueue_tasks`: Adds tasks to the queue based on the object list.
# - `run_tasks`: Defines the worker function for processing tasks from the queue.
# - `start_extraction`: Initiates the extraction process using a specified number of worker threads.
# 
# ### Usage
# - Initialize `ExtractionManager` with a specific `DataExtractor` instance and a list of objects to process.
# - Use `start_extraction` to begin the extraction process, utilizing multi-threading for efficiency.
# 
# ### Example Usage
# ```python
# # Initializing the extraction manager
# manager = ExtractionManager(sql_extractor, object_list, log_schema)
# 
# # Starting the extraction process
# manager.start_extraction(worker_count=5)

# CELL ********************

from threading import Thread
from queue import Queue
import logging

class ExtractionManager:
    def __init__(self, extractor, object_list, logger, data_loader):
        """
        Initializes the ExtractionManager with a specific DataExtractor, a list of objects, and a logger.

        Args:
            extractor (DataExtractor): An instance of a DataExtractor subclass.
            object_list (list): A list of object metadata rows.
            logger (ExtractionLogger): An instance of the ExtractionLogger class.
        """
        # Assigning the provided extractor, object_list, and logger to instance variables
        self.extractor = extractor
        self.object_list = object_list
        self.logger = logger
        self.data_loader = data_loader

        
        # Initializing a queue to hold tasks
        self.queue = Queue()
        
        # Logging the initialization of the ExtractionManager
        logging.info("ExtractionManager initialized.")

    def enqueue_tasks(self):
        """
        Enqueues tasks based on the object list.
        """
        # Log the start of task enqueueing
        logging.info("Enqueuing tasks...")
        
        # Iterate through each object in the object list
        for object_row in self.object_list:
            
            # Add each object_row to the queue
            self.queue.put(object_row)
        
        # Log that all tasks have been enqueued
        logging.info("All tasks enqueued.")

        # Display the objects queued for processing
        # display(Queue)

    def run_tasks(self):
        """
        Worker method for processing tasks. Selects the appropriate DataLoader for each object_row.
        """
        # Continuously process tasks until the queue is empty
        while not self.queue.empty():
            try:
                # Retrieve the next object_row from the queue
                object_row = self.queue.get()
                
                # Display object row being processed
                # display(object_row)
                
                # Log the processing of the current object_row
                logging.info(f"Processing object_row with ID: {object_row['object_id']}")
                print(f"Processing object_row with ID: {object_row['object_id']}")
                
                # Determine the appropriate DataLoader for the current object_row
                #print("Identifying required data loader class")
                #data_loader = DataLoader.get_data_loader(object_row)
                
                # Log that the DataLoader has been selected
                #logging.info("DataLoader selected.")
                #print("DataLoader selected.")
                
                # Call the extract_data method of the extractor with the current object_row, logger, and data_loader
                logging.info("Calling selected DataExtractor...")
                self.extractor.extract_data(object_row, self.logger, self.data_loader)
                
                # Log the completion of data extraction for the current object_row
                logging.info(f"Data extraction completed for object_row ID: {object_row['object_id']}")
                
                # Mark the current task as done in the queue
                self.queue.task_done()
            
            except Exception as e:
                # Log any error that occurs during the extraction process
                logging.error(f"Error in thread during extraction: {e}")
                
                # Mark the task as done even if an error occurs
                self.queue.task_done()

    def start_extraction(self, worker_count=10):
        """
        Starts the extraction process using multiple worker threads.

        Args:
            worker_count (int): Number of worker threads to use.
        """
        # Log the start of the extraction process
        logging.info("Starting extraction process...")
        
        # Enqueue all tasks from the object_list
        self.enqueue_tasks()
        
        # Start worker threads based on the specified worker count
        for _ in range(worker_count):
            
            # Create a thread that runs the run_tasks method
            t = Thread(target=self.run_tasks)
            
            # Set the thread as a daemon thread
            t.daemon = True
            
            # Start the thread
            t.start()
        
        # Log the start of the worker threads
        logging.info(f"{worker_count} worker threads started.")
        
        # Wait for all tasks in the queue to be completed
        self.queue.join()
        
        # Log the completion of the extraction process
        logging.info("Extraction process completed.")
