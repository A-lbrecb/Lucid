# Overview

Lucid is an open-source project developed to accelerate analytics processes in data-driven organizations. It establishes a metadata-driven ETL/ELT framework, leveraging Apache Spark.

Features:
- **Metadata-Driven Framework**: Simplifies data transformation, aiming to reduce time-to-insight.
- **Apache Spark Integration**: Facilitates efficient large-scale data processing.
- **Multi-Platform Compatibility**: Supports various platforms, including Microsoft Fabric and Databricks, catering to diverse cloud environments.

The Fabric Control Framework component includes notebooks and configurations essential for the orchestration of ETL/ELT processes, designed to optimize the performance and scalability of the data pipeline.

# Pre-requisites for using the Lucid framework with Microsoft Fabric

To use the Lucid framework within a Microsoft Fabric environment, ensure the following pre-requisites are met:

- Azure DevOps
- Azure KeyVault
- Microsoft Fabric Workspace

# Current Supported Source Types

As of the current release, Lucid supports the following source types:

- SQL Server

# Importing Lucid for Fabric

Follow these steps to import the Lucid framework into your Microsoft Fabric environment:

1. Create a project in Azure DevOps.
2. Import the Lucid repository into your project from GitHub:
   `https://github.com/Lucid-Will/Lucid`
3. Create a new Microsoft Fabric workspace.
4. Connect your Fabric Workspace to your Azure DevOps repository, including "Fabric Control Framework" as the Git folder.

# Configuring Lucid for Fabric

To configure Lucid for use with Microsoft Fabric, perform the following steps:

1. Connect the following notebooks to the Control Lakehouse:
   - `Build_Control_Layer`
   - `Load_Source_System`
   - `Metadata_Acquisition`
   - `Data_Processing`
2. Connect the `Data_Processing` notebook to the Bronze Lakehouse.
3. Run the `Build_Control_Layer` notebook to create the control tables for metadata processing.
4. Open, update parameters, and run the `Load_Source_System` notebook with the following parameters:
   - `source_name = "{your source system name}"`
   - `source_type = "{source system type}"`
   - `keyvault_name = "{your keyvault name}"`
   - `connection_details = "{name of secret in keyvault containing connection string}"`
5. Open, update parameters, and run the `Metadata_Acquisition` notebook:
   - `source_name = "{your source system name from source_system table}"`
6. Update the `object_metadata` table and set all but one object to `active = false` for initial testing.
7. Run the `Data_Processing` notebook.


# Contribute

Lucid is a community-driven project, and contributions are welcome from all individuals. Whether you are helping to fix bugs, proposing new features, improving documentation, or working on the designs and tests, your input is valuable.

## How to Contribute

If you're interested in contributing, follow these steps:

1. **Fork the Repository**: Make a copy of the project to your GitHub account.
2. **Create a Feature Branch**: From your fork, create a new branch to work on your contribution.
3. **Make Your Changes**: Implement your feature, fix, or documentation update.
4. **Document Your Changes**: Clearly describe the issue your branch addresses and how your changes resolve it.
5. **Submit a Pull Request**: Once you're ready, submit a pull request to the main repository for review.

We ask that all participants adhere to the basic principle of cooperation: do not be disruptive to the community.

## Recognition

Contributors who provide valuable additions to the project will be acknowledged in the project documentation. We believe in recognizing the hard work and dedication of our community members.

## Communication

While we currently do not have a dedicated communication channel, contributors can use GitHub issues and discussions to communicate. We encourage you to engage with the project maintainers and other contributors through these means.

## Issue Tracking and Labels

Please use the issue labels in the GitHub repository to find areas where you can contribute. If you're new to the project, look for issues labeled as 'good first issue' or 'help wanted' as they will provide a good starting point for your contributions.

Remember, contributions are not limited to code. We highly value and encourage documentation improvements, design work, and testing. Whatever your skill set, there is a place for you in the Lucid project.
