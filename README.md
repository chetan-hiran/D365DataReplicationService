# ODS - Operational Data Store.

Replicate Dataverse data to Azure SQL Database. 
ADF Based Data Export Services, which can be the replacement for the retired D365 DES Service.

## Contributors

- Chetan Hiran
- Sathishkumar Ramamurthy
- Amrita Ahirrao
- Dakota Giltner

## Version

1.0.0

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Architecture](#architecture)
  - [Best Practices when using ODS framework.](#best-practices-when-using-ods-framework)
- [Deployment Steps](#deployment-steps)
  - [Create Data Factory](#create-data-factory)
  - [Import the Pipelines through ARM Template](#import-the-pipelines-through-arm-template)
  - [Post deployment validation](#post-deployment-validation)
- [Pipeline Details and Execution Sequence:](#pipeline-details-and-execution-sequence)
  - [01 - Master Data Load](#01---master-data-load) 
  - [02 - Entity Schema Sync Master](#02---entity-schema-sync-master)
  - [03 - Data Sync Master:](#03---data-sync-master)
- [Known Issues](#known-issues)

## Overview

ODS is a framework developed to replicate data from Microsoft Dataverse
to an Azure SQL Database in a user owned Azure Subscription. This
framework will replicate data similar to [Data Export
Services](https://learn.microsoft.com/en-us/power-platform/admin/replicate-data-microsoft-azure-sql-database).
[Data Export
Services](https://learn.microsoft.com/en-us/power-platform/admin/replicate-data-microsoft-azure-sql-database)
is retired by the Product Team and is replaced with [Azure Synapse Link for
Dataverse](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/export-to-data-lake).
Currently there is no support to export the Dataverse data directly to
Azure SQL Server close to real time. Azure Synapse link has the
latency of \~15 mins or more. The ODS framework is fast, cost effective and eliminates the dependency on Azure Synapse.

ODS supports Azure SQL Database, but the framework can be easily extended to support SQL Server on Azure virtual machines or on-Premises
SQL server. The ODS intelligently synchronizes the full inital data load and thereafter synchronizes only the delta records that are modified in the system.

### How does ODS sync the Data from Dataverse

ODS uses the change tracking flag in Dataverse to synchronize data with
external systems, refer to the link [Use change tracking to synchronize
data with external systems (Microsoft Dataverse) - Power Apps \|
Microsoft
Learn](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/use-change-tracking-synchronize-data-external-systems)
to learn about the change tracking and delta token.

## Prerequisites

1.  Azure SQL database:

Azure SQL database and an user/account with the below permissions.

| Permission Type | Permission Name  |
| --------------- | ---------------- |
| CRTB            | CREATE TABLE     |
| CRTY            | CREATE TYPE      |
| CRVW            | CREATE VIEW      |
| CRPR            | CREATE PROCEDURE |

Schema permissions required:

---

| Permission type code | Permission name |
| -------------------- | --------------- |
| AL                   | ALTER           |
| IN                   | INSERT          |
| DL                   | DELETE          |
| SL                   | SELECT          |
| UP                   | UPDATE          |
| EX                   | EXECUTE         |
| RF                   | REFERENCES      |

2.  **Azure Data Factory:**

Follow [these steps](https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory) to create the data factory.

3.  **Azure Key Vault service**

Create an Azure Key Vault and provide Secrets User Role for Data
Factory.

4.  **Azure Blob Storage**

Create Azure Blob Storage and provide Data Contributor role for Data
factory. The ADF pipeline execution logs can be found in this storage.

5.  Microsoft Dataverse

In D365 provision an Application user with System Administrator security role. The framework connects to the environment with this user to fetch the data. 

## Architecture

<img src="media/image1.png" width="500"/>

6.  Pipeline Design:

    a. Schema Sync Pipeline

<img src="media/image2.png" width="500"/>

b. Data Sync Pipeline

<img src="media/image3.png" width="500"/>

### Best Practices when using ODS framework.

- To avoid synchronization errors due to resource throttling, we
  recommend that you have an Azure SQL Database Premium P1 or better
  plan

- Enable Change tracking on Dataverse Entities. More Information:
  [Enable Change Tracking in Power
  Platform.](https://learn.microsoft.com/en-us/power-platform/admin/enable-change-tracking-control-data-synchronization)

- Periodically update database statistics on tables and indexed views
  in the SQL database. More information: [Update
  Statistics](https://learn.microsoft.com/en-us/sql/relational-databases/statistics/update-statistics?view=sql-server-ver16)

- Monitor the SQL database\'s utilization. More information: [Perf
  monitoring](https://learn.microsoft.com/en-us/azure/azure-sql/database/monitor-tune-overview?view=azuresql)

- Monitor Data Factory Pipeline execution. More Information: [Monitor
  using Azure
  Monitor](https://learn.microsoft.com/en-us/azure/data-factory/monitor-using-azure-monitor)

## Deployment Steps

### Run the SQL Scripts

Run the SQL Script located [here](https://github.com/chetan-hiran/D365DataReplicationService/blob/main/Code/SqlScripts/01%20-SQL%20Scripts.sql)

### Create Data Factory

Follow the steps to create the data factory.

<https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory>

### Import the Pipelines through ARM Template

1.  Navigate to Manage -\> ARM Template -\> Import ARM Template.

![](media/image4.png)

2.  Select "Build your own template in the editor."

![](media/image5.png)

3.  Browse to "ARMTemplateForFactory.json" file.

![](media/image6.png)

4.  Click Save

5.  Enter the configuration details.

![](media/image7.png)

6.  Click Review + Create.

This will deploy the pipeline to the Data factory.

### Post deployment validation

1.  Open Data Factory Studio

2.  Navigate to Manage -> Linked Services and Test all the connections.

![](media/image8.png)

3.  Test all the connections.

![](media/image9.png)

## Pipeline Details and Execution Sequence:

### 01 - Master Data Load
<p>This pipeline is used to onboard all the D365 entities in SQL database. This pipeline will create a new record for each D365 entity in
\[ODS\].\[EntitySync\] table.</p>

The table \[ODS\].\[EntitySync\] has three important columns.

1. **"SyncReady:"** <p>The data is synced only for the Entities that are flagged for SyncReady.  </p>

How to set the SyncReady to 1.

```sql
UPDATE ODS.EntitySync
SET SyncReady = 1
WHERE EntityName = 'account'
```

2.  **"SyncFrequency":** <p>This column is used for grouping entities in different buckets for sync.<p>

3.  **DeltaToken:** <p>When DeltaToken is null, the data sync pipeline will fetch all the records from the D365 instance for sync. 
When DetaToken has value only the detla records will be fetched for sync. The delta token is automatically updated after a successful execution of the pipeline.  </p> <p>If a full data load for the entity is needed, then set the value as NULL for entity in the table.</p> 

How to set the DeltaToken to NULL.

```sql
UPDATE ODS.EntitySync
SET DeltaToken = NULL
WHERE EntityName = 'account'
```

**Note:**

- Execute this pipeline manually once you complete the setup of the pipelines. 
- Execute this pipeline any time when there is new entity in D365 that needs to be enabled for data sync


**PARAMETERS**: This pipeline has no parameters.

#### How to Execute the Pipeline

1.  Navigate to Author -\> 01 Master Data Load -\> Add Trigger -\>
    Trigger Now

2.  The pipeline will load data on ODS.EntitySync table.

3.  Update SyncReady Flag in ODS.EntitySync table using the below sample
    script. Update this flag for only the entities that required for data sync.

```sql
UPDATE ODS.EntitySync
SET SyncReady = 1
where EntityName = 'account'
```

**Note:** Only the entities flagged as SyncReady = 1 will be set for data sync.

![](media/image10.png)

## 02 - Entity Schema Sync Master

This pipeline creates the actual physical table in the database for the
entities that are marked as SyncReady = 1 in the table
\[ODS\].\[EntitySync\] and inserts entity attribute details into
\[ODS\].\[EntityAttributeSchema\] table. There will be two tables
created for each entity, one with the ODS and other with STAGING schema.
Execute this pipeline manually after each solution deployment to
environment. This pipeline helps to sync the schema between D365 and the
SQL database.

**Note**

- Execute this pipeline manually once you complete the execution of
  the pipeline "01 - Master Data Load" setup of the pipelines.

- Execute this pipeline manually after each solution deployment to
  environment to have the entity schema be in sync between D365 and
  SQL Database. Basically if you are making any schema changes in the
  existing entity or adding any new entity in D365 and if you want
  those schema changes/new entity to get reflected in the Database
  then you should execute this pipeline.

**PARAMETERS**:

| Parameter  | Data Type | Default Value    | How to Use                                                                                                                                                                                                                                                                                 |
| ---------- | --------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| EntityName | String    | `{"key": "all"}` | If you want to run the pipeline for a specific entity, replace the word all with the table name in the comma separated format. For example: {“key” : “account, contact”}. If set to “all” it will pick up all entities for which SyncReady = 1 is set in the table \[ODS\].\[EntitySync\]. |

Navigate to 02 -- Entity Schema Sync Master -\> Add Trigger -\>
Trigger Now and mention the parameter value.

![](media/image11.png)

## 03 - Data Sync Master:

This pipeline will sync the data from D365 to Azure SQL db. It is
recommended to create triggers to sync the data at periodic intervals.
This way your data will be synced at regular intervals.

You can also run this pipeline only for selected entities by passing the
entity name as parameter.

**PARAMETERS**:

| Parameter     | Data Type | Default Value    | How to Use                                                                                                                                                                                                                                                   |
| ------------- | --------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| SyncFrequency | String    | Daily            | Corresponds to the value in the SyncFrequency column in the ODS.EntitySync table. This must match any entity that you want to sync with this pipeline.                                                                                                       |
| EntityName    | String    | `{"key": "all"}` | Specifies the entity to sync. If you want to run the pipeline for a specific entity, replace the word all with the table name. For example: {“key”: “account, contact”} If set to “all” it will pick up all entities that match the SyncFrequency parameter. |

![](media/image12.png)

04 - Data Sync -- OptionSets

This pipeline creates the actual physical table in the database for
OptionSetMetadata and GlobalOptionSetMetadata and sync the data from
D365 to SQL to the respective tables.

![](media/image13.png)

# Known Issues

There are no known bugs when using the framework at this point.
