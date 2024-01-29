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

# ## Importing Utility Functions and Classes
# 
# This cell is responsible for integrating the custom utility functions and classes that are essential for the data processing tasks:
# 
# - The line `%run Utils` is a Jupyter Notebook command that executes the `Utils.ipynb` notebook. This command imports all the functions, classes, and variables defined in `Utils.ipynb` into the current notebook's environment.
# 
# The `Utils.ipynb` notebook contains various utility classes and functions that have been specifically designed to streamline the data processing tasks. These utilities include classes for data extraction, transformation, loading, and other common data engineering operations.
# 
# By executing this cell, the current notebook is enhanced with a suite of tailored tools and constructs, ensuring a more efficient, maintainable, and modular data processing workflow.

# CELL ********************

%run Utils

# MARKDOWN ********************

# ## Importing Standard Libraries and Initializing Logger
# 
# This cell is crucial for setting up the basic environment for the notebook:
# 
# 1. **Importing Standard Libraries:**
#    - `logging`: Used for logging events and messages during the execution of the notebook. Essential for debugging and tracking the progress of data processes.
# 
# 2. **Initializing the Logger:**
#    - A logger specific to this notebook is initialized using `logging.getLogger("DataProcessing")`. This sets up a logging instance that can be used throughout the notebook to log messages, which is vital for monitoring the notebook's execution and troubleshooting any issues that arise.
#    
# By executing this cell, the notebook is equipped with fundamental tools for logging, time management, and unique identifier generation, paving the way for a structured and traceable data processing workflow.

# CELL ********************

# Import standard libraries
import logging

# Initialize the logger for this notebook
logger = logging.getLogger("DataProcessing")

# MARKDOWN ********************

# ## Setting Parameters for Source System
# 
# This section focuses on assigning specific parameters to variables, which are essential for identifying and processing the source system data:
# 
# 1. **Assigning Source Name:**
#    - The variable `source_name` is assigned a value. This identifier is used to specify the exact source system from which data will be extracted. It is a key parameter that ensures the data processing aligns with the intended source of information.
# 
# 2. **Setting Load Group:**
#    - The variable `load_group` is assigned a value. This value represents a specific grouping or categorization within the source system. The load group can dictate various aspects of data processing, such as the subset of data to be extracted, processing priority, or specific processing rules applicable to that group.
# 
# These parameters are foundational for the subsequent steps in the data processing workflow. They ensure that the processing logic is tailored to the specific characteristics of the data source, thereby enhancing the accuracy and relevance of the data extraction and processing activities.


# PARAMETERS CELL ********************

# Assigning the parameters to variables
source_name = 'Lucid_Sample_Data'
load_group = 1

# MARKDOWN ********************

# ## Retrieving Source System Details and Configuration
# 
# This section of the notebook is dedicated to retrieving the configuration details of the source system. It involves the following steps:
# 
# 1. **Set Source System Parameters:** 
#    - `source_name` and `load_group` variables are defined to specify the particular source system and load group we are interested in. These parameters are crucial in identifying the right configuration for data extraction.
# 
# 2. **Instantiate `SourceSystemConfig` Class:**
#    - An instance of the `SourceSystemConfig` class is created. This class is designed to handle the retrieval and management of configuration details related to various source systems. It acts as a centralized place to manage these configurations, thereby promoting reusability and modularity.
# 
# 3. **Get Source Details:**
#    - The `get_source_details` method of the `SourceSystemConfig` instance is called, passing the `source_name` and `load_group` as arguments. This method fetches the relevant configuration details for the specified source system and load group, encapsulating them in a DataFrame.
# 
# 4. **Display DataFrame for Verification:**
#    - The resulting DataFrame, `source_details_df`, is displayed (with a limit of 1 row for brevity). This step is crucial for verifying that the correct configuration details have been retrieved. It ensures that the data extraction process will be based on accurate and relevant source system information.
# 
# By following this structured approach, the notebook ensures that the data extraction and loading processes are aligned with the specific configurations of the desired source system, leading to more accurate and efficient data processing.

# CELL ********************

# Create an instance of the SourceSystemConfig class
source_system_config = SourceSystemConfig()
source_details_df = source_system_config.get_source_details(source_name)

# Display the DataFrame for verification
source_details_df.show(1)

# CELL ********************

# Create an instance of the SourceSystemConfig class
source_system_config = SourceSystemConfig()
processing_list = source_system_config.get_processing_list(source_name, load_group)

# Display the DataFrame for verification
display(processing_list)

# MARKDOWN ********************

# ## Retrieving Key Vault Connection Details
# 
# This cell is crucial for establishing a secure connection to the Key Vault, which is essential for accessing sensitive data securely:
# 
# 1. **Extracting First Row from DataFrame:**
#    - The first step involves retrieving the first row of the DataFrame `source_details_df` using the `.first()` method. This row contains key configuration details required for the subsequent steps.
#    - Two specific pieces of information, `keyvault_name` and `secure_connection`, are extracted from this row. These are critical for establishing a connection to the Key Vault.
# 
# 2. **Creating KeyVaultConnector Instance:**
#    - An instance of the `KeyVaultConnector` class is created. This class is designed to manage interactions with Azure Key Vault, providing a secure and efficient way to access secrets and keys.
# 
# 3. **Retrieving Connection String:**
#    - The `get_connection_details` method of the `KeyVaultConnector` instance is used to fetch the connection string. It uses `keyvault_name` and `secure_connection` as parameters to identify and retrieve the correct connection details.
#    - The retrieved `connection_string` is essential for connecting to databases or other services requiring authenticated access.
# 
# By completing these steps, the notebook securely acquires the necessary connection string from the Key Vault, ensuring that sensitive information is handled appropriately. This process is vital for maintaining security and privacy standards in data processing activities.

# CELL ********************

# Retrieve the first row of the DataFrame
first_row = source_details_df.first()

# Assign keyvault variables
keyvault_name = first_row['keyvault_name']
secure_connection = first_row['secure_connection']

# Create an instance of the KeyVaultConnector class
keyvault_connector = KeyVaultConnector()
connection_string = keyvault_connector.get_connection_details(keyvault_name, secure_connection)

# MARKDOWN ********************

# ## Data Extraction Process Overview
# 
# This section outlines the steps involved in initializing and executing the data extraction process. It involves setting up various components like `DataExtractor`, `DataLoader`, and `ExtractionManager` to handle the extraction of data from a source system.
# 
# ### Steps
# 
# 1. **Assign Source Type**: 
#    - `source_type = first_row["source_type"]`: Determine the type of the data source from the first row of a DataFrame.
# 
# 2. **Prepare Object List**:
#    - `object_list = [row.asDict() for row in processing_list.collect()]`: Convert the rows in `processing_list` to dictionaries and store them in `object_list`.
# 
# 3. **Isolate First Object for Loading Pattern**:
#    - `first_object_row = object_list[0] if object_list else None`: Identify the loading pattern by isolating the first object row.
# 
# 4. **Initialize DataExtractor**:
#    - Create an instance of `DataExtractor` and use the factory method `get_data_extractor` to obtain an extractor specific to the source type.
# 
# 5. **Initialize DataLoader**:
#    - Create an instance of `DataLoader` and use the factory method `get_data_loader` to obtain a data loader based on the first object row.
# 
# 6. **Initialize ExtractionLogger**:
#    - `logger = ExtractionLogger()`: Instantiate the `ExtractionLogger` for logging extraction details.
# 
# 7. **Initialize and Start ExtractionManager**:
#    - `extraction_manager = ExtractionManager(data_extractor, object_list, logger, data_loader)`: Create an instance of `ExtractionManager` with the data extractor, object list, logger, and data loader.
#    - `extraction_manager.start_extraction(worker_count)`: Start the extraction process using a specified number of worker threads.
# 
# ### Parallel Extraction Process
# The `ExtractionManager` class handles the parallel execution of data extraction tasks. It distributes the workload across multiple threads for efficient processing, ensuring each object in the object list is processed concurrently. This approach enhances performance, especially when dealing with large volumes of data or numerous extraction tasks.
# 
# ### Example Usage
# ```python
# # Assume first_row, processing_list, and connection_string are defined
# source_type = first_row["source_type"]
# object_list = [row.asDict() for row in processing_list.collect()]
# first_object_row = object_list[0] if object_list else None
# 
# data_extractor = DataExtractor().get_data_extractor(source_type, connection_string)
# data_loader = DataLoader().get_data_loader(first_object_row)
# logger = ExtractionLogger()
# 
# extraction_manager = ExtractionManager(data_extractor, object_list, logger, data_loader)
# extraction_manager.start_extraction(worker_count=10)

# CELL ********************

# Assign source_type variable
source_type = first_row["source_type"]

# Assign object list for processing to dictionary
object_list = [row.asDict() for row in processing_list.collect()]

# Isolate the first row in the object list to identify loading pattern
first_object_row = object_list[0] if object_list else None

# Create an instance of the DataExtractor
data_extractor = DataExtractor()
data_extractor = data_extractor.get_data_extractor(source_type, connection_string)

# Create an instance of the DataLoader
data_loader = DataLoader()
data_loader = data_loader.get_data_loader(first_object_row)

# Create an instance of the ExtractionLogger
logger = ExtractionLogger()

# Create an instance of the ExtractionManager class
extraction_manager = ExtractionManager(data_extractor, object_list, logger, data_loader)

# Assign worker count for parallel processing
worker_count = 25

# Start the extraction process
extraction_manager.start_extraction(worker_count)
