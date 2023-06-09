/****** Object:  Schema [History]    Script Date: 4/13/2023 9:38:47 AM ******/
CREATE SCHEMA [History]
GO
/****** Object:  Schema [ODS]    Script Date: 4/13/2023 9:38:47 AM ******/
CREATE SCHEMA [ODS]
GO
/****** Object:  Schema [Staging]    Script Date: 4/13/2023 9:38:47 AM ******/
CREATE SCHEMA [Staging]
GO
/****** Object:  UserDefinedTableType [ODS].[EntityAttributeSchemaType]    Script Date: 4/13/2023 9:38:47 AM ******/
CREATE TYPE [ODS].[EntityAttributeSchemaType] AS TABLE(
	[AttributeOf] [varchar](500) NULL,
	[AttributeType] [varchar](500) NULL,
	[EntityLogicalName] [varchar](500) NULL,
	[IsPrimaryId] [bit] NULL,
	[IsValidODataAttribute] [bit] NULL,
	[IsPrimaryName] [bit] NULL,
	[AttributeLogicalName] [varchar](500) NULL,
	[AttributeSchemaName] [varchar](500) NULL,
	[MaxLength] [int] NULL,
	[DatabaseLength] [int] NULL,
	[Precision] [int] NULL,
	[IsRetrievable] [bit] NULL,
	[AttributeTypeName] [nvarchar](500) NULL
)
GO
/****** Object:  UserDefinedTableType [ODS].[EntitySyncType]    Script Date: 4/13/2023 9:38:47 AM ******/
CREATE TYPE [ODS].[EntitySyncType] AS TABLE(
	[DisplayName] [nvarchar](500) NULL,
	[EntityName] [varchar](500) NOT NULL,
	[RelativeURL] [varchar](500) NOT NULL,
	[ChangeTrackingEnabled] [bit] NULL,
	[IsNNRelationShip] [bit] NULL,
	[PrimaryIdAttribute] [nvarchar](500) NULL,
	[IsBPFEntity] [bit] NULL,
	[IsActivity] [bit] NULL
)
GO
/****** Object:  Table [ODS].[EntityAttributeSchema]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[EntityAttributeSchema](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[AttributeOf] [varchar](500) NULL,
	[AttributeType] [varchar](500) NULL,
	[EntityLogicalName] [varchar](500) NULL,
	[IsPrimaryId] [bit] NULL,
	[IsValidODataAttribute] [bit] NULL,
	[IsPrimaryName] [bit] NULL,
	[AttributeLogicalName] [varchar](500) NULL,
	[AttributeSchemaName] [varchar](500) NULL,
	[MaxLength] [int] NULL,
	[DatabaseLength] [int] NULL,
	[Precision] [int] NULL,
	[IsRetrievable] [bit] NULL,
	[IsValid] [bit] NULL,
	[AttributeTypeName] [nvarchar](500) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [ODS].[EntityAttributeSchemaMapping]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[EntityAttributeSchemaMapping](
	[SourceSchema] [varchar](500) NULL,
	[DestinationSchema] [varchar](500) NULL,
	[PrimaryColumnMapping] [varchar](max) NULL,
	[SecondaryColumnMapping] [varchar](max) NULL,
	[AttributeTypeName] [nvarchar](500) NULL,
	[SecondaryColumnMappingJson] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [ODS].[EntityDataSyncLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[EntityDataSyncLog](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[EntityName] [nvarchar](500) NOT NULL,
	[ExecutionId] [uniqueidentifier] NOT NULL,
	[PipelineRunID] [uniqueidentifier] NOT NULL,
	[InsertCount] [int] NULL,
	[UpdateCount] [int] NULL,
	[DeleteCount] [int] NULL,
	[SyncDate] [datetime] NOT NULL,
	[SkippedRows] [int] NOT NULL,
	[LogFilePath] [nvarchar](500) NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [ODS].[EntityDeltaTokenLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[EntityDeltaTokenLog](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[ExecutionId] [uniqueidentifier] NOT NULL,
	[PipelineRunId] [uniqueidentifier] NOT NULL,
	[EntityName] [nvarchar](500) NOT NULL,
	[DeltaToken] [varchar](500) NOT NULL,
	[LogDate] [datetime] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [ODS].[EntitySchemaSyncLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[EntitySchemaSyncLog](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[ExecutionId] [uniqueidentifier] NULL,
	[SchemaChangeQuery] [nvarchar](max) NULL,
	[SyncDate] [datetime] NULL,
	[TableName] [nvarchar](500) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [ODS].[EntitySync]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[EntitySync](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DisplayName] [nvarchar](500) NOT NULL,
	[EntityName] [varchar](500) NOT NULL,
	[RelativeURL] [varchar](500) NOT NULL,
	[DeltaToken] [varchar](500) NULL,
	[LastSyncTimeStamp] [datetime] NULL,
	[AttributeSchemaURL] [varchar](500) NULL,
	[Mapping] [varchar](max) NULL,
	[FirstInsertMapping] [varchar](max) NULL,
	[SyncReady] [bit] NULL,
	[ChangeTrackingEnabled] [bit] NULL,
	[IsNNRelationShip] [bit] NULL,
	[SyncFrequency] [varchar](100) NULL,
	[PrimaryIdAttribute] [nvarchar](500) NULL,
	[IsBPFEntity] [bit] NULL,
	[IsActivity] [bit] NULL,
	[IsSystemVersioningEnabled] [bit] NOT NULL,
 CONSTRAINT [PK_EntitySync] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [ODS].[ExecutionLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[ExecutionLog](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ExecutionId] [uniqueidentifier] NULL,
	[PackageName] [varchar](500) NULL,
	[RunStatus] [varchar](100) NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[Message] [nvarchar](500) NULL,
 CONSTRAINT [PK_ExecutionLog] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [ODS].[ExecutionLogDetail]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ODS].[ExecutionLogDetail](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[ExecutionId] [uniqueidentifier] NULL,
	[PipelineRunID] [uniqueidentifier] NULL,
	[StepName] [varchar](500) NULL,
	[StartDate] [datetime] NULL,
	[Message] [nvarchar](500) NULL,
	[LogType] [varchar](100) NULL,
	[EntityName] [varchar](500) NULL,
 CONSTRAINT [PK_ExecutionLogDetail] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [ODS].[EntityAttributeSchema] ADD  CONSTRAINT [DF__EntityAtt__IsVal__6DB809C1]  DEFAULT ((1)) FOR [IsValid]
GO
ALTER TABLE [ODS].[EntityDataSyncLog] ADD  DEFAULT (getutcdate()) FOR [SyncDate]
GO
ALTER TABLE [ODS].[EntityDeltaTokenLog] ADD  DEFAULT (getutcdate()) FOR [LogDate]
GO
ALTER TABLE [ODS].[EntitySchemaSyncLog] ADD  DEFAULT (getutcdate()) FOR [SyncDate]
GO
ALTER TABLE [ODS].[EntitySync] ADD  CONSTRAINT [DF__EntitySyn__SyncR__59662CFA]  DEFAULT ((0)) FOR [SyncReady]
GO
ALTER TABLE [ODS].[EntitySync] ADD  DEFAULT ((0)) FOR [IsSystemVersioningEnabled]
GO
ALTER TABLE [ODS].[ExecutionLog] ADD  DEFAULT (getdate()) FOR [StartDate]
GO
ALTER TABLE [ODS].[ExecutionLogDetail] ADD  CONSTRAINT [DF__Execution__Start__25B17ECA]  DEFAULT (getdate()) FOR [StartDate]
GO
/****** Object:  StoredProcedure [ODS].[usp_CreateStagingTable]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      Microsoft
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_CreateStagingTable] 
(
	@sourceEntityName    NVARCHAR(500),
	@PrimaryIdAttribute NVARCHAR(500) 
) 
AS
BEGIN
	IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME   = @sourceEntityName AND Table_Schema = 'Staging')
	BEGIN
		--- MODIFIED
		--declare @dropStatement nvarchar(500) = 'drop table Staging.' + @sourceEntityName
		--exec sp_executeSQL @dropStatement ;
		--END
		-- Create Staging Table for comparision in Staging Schema
		DECLARE @tableCreateScriptStaging NVARCHAR(MAX) = 'Create table Staging.' + @sourceEntityName + ' ({Columns})'
		DECLARE @columnsStaging NVARCHAR(MAX) = ''
        SELECT
                @columnsStaging = @columnsStaging + AttributeLogicalName + ' ' + destinationSchema + CASE WHEN destinationSchema = 'nvarchar' THEN '(' + CAST(ISNULL( CASE WHEN DatabaseLength = 0 THEN MaxLength ELSE DatabaseLength / 2 END , 4000) AS NVARCHAR) + ')' ELSE '' END + CASE WHEN destinationSchema = 'decimal' THEN '(38,' + CAST(ISNULL(PRECISION,2) AS NVARCHAR) + ')' ELSE '' END + ','
        FROM
                ODS.EntityAttributeSchema AS s
        JOIN
                ODS.EntityAttributeSchemaMapping map
        ON
                map.SourceSchema      = s.AttributeType
        AND     map.AttributeTypeName = s.AttributeTypeName
        WHERE
                s.EntityLogicalName = @sourceEntityName
        AND     s.IsValid           = 1
        SELECT
                @columnsStaging = SUBSTRING(@columnsStaging,0, LEN(@columnsStaging)) SET @tableCreateScriptStaging = REPLACE(@tableCreateScriptStaging, '{Columns}', @columnsStaging);
                                                
        EXEC sp_executeSQL @tableCreateScriptStaging;
        -- Add Delta Token Column
        DECLARE @delataTokenSql NVARCHAR(500) = 'Alter table Staging.' + @sourceEntityName + ' Add ODS_DeltaToken nvarchar(500), id uniqueidentifier, reason varchar(100), Action varchar(3)' EXEC sp_executeSQL @delataTokenSql
        DECLARE @indexPrimaryColumn NVARCHAR(MAX) = 'CREATE INDEX idx_{0}_{1} ON Staging.{0} ({1})';
        SET @indexPrimaryColumn           = REPLACE(@indexPrimaryColumn,'{1}',@PrimaryIdAttribute);
        SET @indexPrimaryColumn           = REPLACE(@indexPrimaryColumn,'{0}',@sourceEntityName);
        EXEC sp_executeSQL @indexPrimaryColumn
        DECLARE @actionIndex NVARCHAR(MAX) = REPLACE('CREATE INDEX idx_{0}_action ON Staging.{0} (action)','{0}',@sourceEntityName);
        EXEC sp_executeSql @actionIndex
        DECLARE @deleteIdIndex NVARCHAR(MAX) = REPLACE('CREATE INDEX idx_{0}_DeleteId ON Staging.{0} (id, reason)','{0}',@sourceEntityName);
        EXEC sp_executeSql @deleteIdIndex

	END
    ELSE
    BEGIN
		DECLARE @truncateSQL NVARCHAR(MAX) = 'TRUNCATE TABLE Staging.' + @sourceEntityName EXEC sp_executeSql @truncateSQL
    END
END
GO
/****** Object:  StoredProcedure [ODS].[usp_ExecutionLogDetail]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_ExecutionLogDetail] 
(
	@ExecutionId UNIQUEIDENTIFIER,
	@PipelineRunID UNIQUEIDENTIFIER,
	@StepName   VARCHAR(500),
	@Message    NVARCHAR(500),
	@LogType    VARCHAR(100) = 'Information',
	@entityName VARCHAR(500) = NULL 
) AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO ODS.ExecutionLogDetail
	(
			ExecutionId  ,
			StepName     ,
			StartDate    ,
			MESSAGE      ,
			PipelineRunID,
			LogType      ,
			EntityName
	)
	VALUES
	(
			@ExecutionId  ,
			@StepName     ,
			GETUTCDATE()  ,
			@Message      ,
			@PipelineRunID,
			@LogType      ,
			@entityName
	)
END
GO
/****** Object:  StoredProcedure [ODS].[usp_InsertEntityDataSyncLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_InsertEntityDataSyncLog] 
(
	@EntityName NVARCHAR(500),
	@ExecutionId UNIQUEIDENTIFIER,
	@PipelineRunID UNIQUEIDENTIFIER,
	@InsertCount INT,
	@UpdateCount INT,
	@DeleteCount INT,
	@SkippedRows INT,
	@LogFilePath NVARCHAR(500) 
) AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO ODS.EntityDataSyncLog
	(
			EntityName   ,
			ExecutionId  ,
			PipelineRunID,
			InsertCount  ,
			UpdateCount  ,
			DeleteCount  ,
			SkippedRows  ,
			LogFilePath
	)
	VALUES
	(
			@EntityName   ,
			@ExecutionId  ,
			@PipelineRunID,
			@InsertCount  ,
			@UpdateCount  ,
			@DeleteCount  ,
			@SkippedRows  ,
			@LogFilePath
	)
END
GO
/****** Object:  StoredProcedure [ODS].[usp_InsertEntityDeltaTokenLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_InsertEntityDeltaTokenLog] 
(
	@ExecutionId UNIQUEIDENTIFIER,
	@DeltaToken VARCHAR(500),
	@PipelineRunId UNIQUEIDENTIFIER,
	@EntityName NVARCHAR(500) 
) AS
BEGIN
	INSERT INTO ODS.EntityDeltaTokenLog
	(
			ExecutionId  ,
			DeltaToken   ,
			PipelineRunId,
			EntityName
	)
	SELECT @ExecutionId, @DeltaToken, @PipelineRunId, @EntityName 
END
GO
/****** Object:  StoredProcedure [ODS].[usp_InsertEntitySchemaSyncLog]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_InsertEntitySchemaSyncLog] 
(	
	@ExecutionId UNIQUEIDENTIFIER,
	@SchemaChangeQuery NVARCHAR(MAX),
	@tableName         NVARCHAR(500)
) AS        
BEGIN

	SET NOCOUNT ON
	-- Insert statements for procedure here
	INSERT INTO ODS.EntitySchemaSyncLog
	(
			ExecutionId      ,
			SchemaChangeQuery,
			TableName
	)
	SELECT @ExecutionId, @SchemaChangeQuery, @tableName 
END
GO
/****** Object:  StoredProcedure [ODS].[usp_InsertOrUpdateEntityTable]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:   Microsoft   
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_InsertOrUpdateEntityTable] 
(
	@sourceTableName NVARCHAR(500),
	@primaryColumn   NVARCHAR(500) ,
	@ExecutionId     NVARCHAR(50) 
)
AS
BEGIN
    DECLARE @count INT
    DECLARE @countInt INT
    DECLARE @sql NVARCHAR(MAX) = 'select @countInt = count(1)  from Staging.' + @sourceTableName 
	EXEC sp_executeSQL @sql, N'@countInt int output',
    @countInt  = @countInt OUT

    IF (@countInt = 0)
		RETURN

    DECLARE @updateColumns NVARCHAR(MAX) = '', 
			@insertColumns NVARCHAR(MAX) = '',
            @insertsColumn NVARCHAR(MAX) = '', 
			@insertColumnSource NVARCHAR(MAX)
    SELECT
            @updateColumns = @updateColumns + 
				CASE WHEN COLUMN_Name <> @primaryColumn 
				THEN COLUMN_Name + '=s.' + COLUMN_Name +',' 
				ELSE '' END,
            @insertColumns = @insertColumns + COLUMN_Name+',' ,
            @insertsColumn = @insertsColumn + 's.'+ COLUMN_Name +','
    FROM
            INFORMATION_SCHEMA.COLUMNS
    WHERE
            TABLE_NAME   = ''+@sourceTableName+''
    AND     Table_Schema = 'dbo'
    AND     COLUMN_NAME <> 'ODS_IsDeleted'
    AND     COLUMN_NAME <> 'ODS_DeletedOn'
    AND     COLUMN_NAME <> 'ExecutionId'
    AND     COLUMN_NAME <> 'SinkModifiedOn'
    AND     COLUMN_NAME <> 'SinkCreatedOn'
    AND     COLUMN_NAME <> 'ValidFrom'
    AND     COLUMN_NAME <> 'ValidTo'
    AND     COLUMN_NAME <> 'Period'
    AND     COLUMN_NAME <> 'ID'
    AND     COLUMN_NAME <> @primaryColumn SET @updateColumns = SUBSTRING(@updateColumns, 0, LEN(@updateColumns));
                                                
    SET @updateColumns      = @updateColumns + ',ExecutionId=''' + @ExecutionId + '''';
    SET @updateColumns      = @updateColumns + ',SinkModifiedOn=getutcdate()';
    SET @insertColumns      = SUBSTRING(@insertColumns, 0, LEN(@insertColumns));
    SET @insertColumnSource = @insertColumns;
    SET @insertColumns      = @insertColumns + ',ExecutionId,ID'
    DECLARE @updateActionSql NVARCHAR(MAX) = 'declare @isNN bit; select @isNN = IsNNRelationShip from ODS.EntitySync where EntityName = '''+@sourceTableName+''';

	if @isNN = 1
	BEGIN
		Update Staging.'+@sourceTableName+'
		Set Action = ''I''
		from Staging.'+@sourceTableName+' as s
			LEFT JOIN dbo.'+@sourceTableName+' as d
				on s.'+@primaryColumn+' = d.ID
			where d.ID is null
	
		Insert into Staging.'+@sourceTableName+' (id, Action)	
		select d.'+@primaryColumn+', ''D'' from dbo.'+@sourceTableName+' as d
			LEFT JOIN Staging.'+@sourceTableName+' as s
				on s.'+@primaryColumn+' = d.ID
			where s.'+@primaryColumn+' is null and d.ODS_IsDeleted is null

		Update Staging.'+@sourceTableName+'
		set Action = ''D''
		where '+@primaryColumn+' is null and reason = ''deleted'';

		END
		ELSE BEGIN
		Update Staging.'+@sourceTableName+'
		set Action = ''D''
		where '+@primaryColumn+' is null and reason = ''deleted'';

		Update Staging.'+@sourceTableName+'
		set Action = ''U''
		from Staging.'+@sourceTableName+' as s
			JOIN dbo.'+@sourceTableName+' as d
			on d.ID = s.'+@primaryColumn+'

		update Staging.'+@sourceTableName+'
		set Action = ''I''
		where Action is null
	END' 
	EXEC sp_executeSQL @updateActionSql
	DECLARE @insertStatement NVARCHAR(MAX) = 'Insert into dbo.'+@sourceTableName+' ('+@insertColumns+')
	select '+@insertColumnSource + ',''' + @ExecutionId +''',' +@primaryColumn+' from staging.'+@sourceTableName+' where Action = ''I''' 
	EXEC sp_executeSQL @insertStatement
	DECLARE @updateStatement NVARCHAR(MAX) = 'Update dbo.'+@sourceTableName+' set '+@updateColumns+'
	from dbo.'+@sourceTableName+' as d join Staging.'+@sourceTableName+' as s on s.'+@primaryColumn+' = d.ID' 
	EXEC sp_executeSQL @updateStatement
		--declare @deleteQuery nvarchar(max) =
		--'Update dbo.'+@sourceTableName+'
		--set ODS_IsDeleted = 1, ODS_DeletedOn = getutcdate(), ODS_ModifiedOn=getutcdate()
		--, ExecutionId = '''+@ExecutionId+'''
		--from dbo.'+@sourceTableName+' as a
		--JOIN Staging.'+@sourceTableName+' as d
		-- on d.id = a.'+@primaryColumn +'
		-- where d.action = ''D'''
		--exec sp_executeSQL @deleteQuery
	DECLARE @deleteQuery NVARCHAR(MAX) = 'delete a
	from dbo.'+@sourceTableName+' as a
	JOIN Staging.'+@sourceTableName+' as d
		on d.id = a.ID
		where d.action = ''D''' 
	EXEC sp_executeSQL @deleteQuery
	--exec [ODS].[usp_UpdateDeltaToken] @sourceSchema = 'Staging', @sourceTableName = @sourceTableName
	--exec [ODS].[usp_TruncateStagingTable] @sourceEntityName=@sourceTableName,  @databaseSchema = 'Staging'
	DECLARE @updateTimeStampQuery NVARCHAR(MAX) = 'Update ODS.EntitySync set LastSyncTimeStamp = getdate() where EntityName = '''+@sourceTableName+ '''' 
	EXEC sp_executeSQL @updateTimeStampQuery
END
GO
/****** Object:  StoredProcedure [ODS].[usp_LoadEntityAttributeSchema]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_LoadEntityAttributeSchema] 
(
	@passing ODS.EntityAttributeSchemaType READONLY,
	@sourceTableName NVARCHAR(100) 
) 
AS
BEGIN
        MERGE ODS.EntityAttributeSchema AS d
        USING   @passing                AS s
        ON
                s.EntityLogicalName    = d.EntityLogicalName
        AND     s.AttributeLogicalName = d.AttributeLogicalName
        AND     (
                        d.AttributeType            <> s.AttributeType
                OR      d.IsValidODataAttribute    <> s.IsValidODataAttribute
                OR      ISNULL(d.MaxLength,0)      <> ISNULL(s.MaxLength,0)
                OR      ISNULL(d.DatabaseLength,0) <> ISNULL(s.DatabaseLength,0)
                OR      ISNULL(d.Precision,0)      <> ISNULL(s.Precision,0)
                OR      d.AttributeTypeName        <> s.AttributeTypeName )
        WHEN MATCHED THEN
        UPDATE
        SET     d.EntityLogicalName     = s.EntityLogicalName     ,
                d.AttributeLogicalName  = s.AttributeLogicalName  ,
                d.AttributeOf           = s.AttributeOf           ,
                d.AttributeType         = s.AttributeType         ,
                d.IsPrimaryId           = s.IsPrimaryId           ,
                d.IsValidODataAttribute = s.IsValidODataAttribute ,
                d.IsPrimaryName         = s.IsPrimaryName         ,
                d.AttributeSchemaName   = s.AttributeSchemaName   ,
                d.MaxLength             = s.MaxLength             ,
                d.DatabaseLength        = s.DatabaseLength        ,
                d.Precision             = s.Precision             ,
                d.IsRetrievable         = s.IsRetrievable         ,
                d.AttributeTypeName     = s.AttributeTypeName;
                
        MERGE ODS.EntityAttributeSchema AS d
        USING   @passing                AS s
        ON
                s.EntityLogicalName    = d.EntityLogicalName
        AND     s.AttributeLogicalName = d.AttributeLogicalName
        WHEN NOT MATCHED BY TARGET THEN
        INSERT
                (
                        AttributeOf          ,
                        AttributeType        ,
                        EntityLogicalName    ,
                        IsPrimaryId          ,
                        IsValidODataAttribute,
                        IsPrimaryName        ,
                        AttributeLogicalName ,
                        AttributeSchemaName  ,
                        MaxLength            ,
                        DatabaseLength       ,
                        PRECISION            ,
                        IsRetrievable        ,
                        AttributeTypeName
                )
                VALUES
                (
                        s.AttributeOf          ,
                        s.AttributeType        ,
                        s.EntityLogicalName    ,
                        s.IsPrimaryId          ,
                        s.IsValidODataAttribute,
                        s.IsPrimaryName        ,
                        s.AttributeLogicalName ,
                        s.AttributeSchemaName  ,
                        s.MaxLength            ,
                        s.DatabaseLength       ,
                        s.Precision            ,
                        s.IsRetrievable        ,
                        s.AttributeTypeName
                );
                
        DELETE
                s
        FROM
                ODS.EntityAttributeSchema AS s
        WHERE
                id IN
                (
                        SELECT
                                s.id
                        FROM
                                ODS.EntityAttributeSchema AS s
                        LEFT JOIN
                                @passing AS t
                        ON
                                t.EntityLogicalName    = s.EntityLogicalName
                        AND     t.AttributeLogicalName = s.AttributeLogicalName
                        WHERE
                                t.AttributeLogicalName IS NULL
                        AND     s.EntityLogicalName          = @sourceTableName)
        INSERT INTO [ODS].[EntityAttributeSchema]
                (
                        [AttributeOf]           ,
                        [AttributeType]         ,
                        [EntityLogicalName]     ,
                        [IsPrimaryId]           ,
                        [IsValidODataAttribute] ,
                        [IsPrimaryName]         ,
                        [AttributeLogicalName]  ,
                        [AttributeSchemaName]   ,
                        [IsRetrievable]         ,
                        [IsValid]               ,
                        [AttributeTypeName]
                )
        SELECT
                AttributeLogicalName,
                'EntityName'        ,
                EntityLogicalName   ,
                0,0,0,
                AttributeLogicalName +'_entitytype',
                AttributeLogicalName +'_entitytype',
                0                                  ,
                1                                  ,
                'EntityNameType'
        FROM
                [ODS].[EntityAttributeSchema]
        WHERE
                EntityLogicalName = @sourceTableName
        AND     AttributeType     ='Lookup'
END
GO
/****** Object:  StoredProcedure [ODS].[usp_LoadEntityAttributeSchemaMapping]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft  
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_LoadEntityAttributeSchemaMapping]
AS 
BEGIN
 TRUNCATE TABLE [ODS].[EntityAttributeSchemaMapping]

INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'BigInt', N'bigint', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Int64"}}', NULL, N'BigIntType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Boolean', N'bit', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Boolean"}}', N'{"source": { "path": "[''{AttributeLogicalName}@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'BooleanType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''{AttributeOf}@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'DateTime', N'datetime', N'{"source":{"path":"[''{AttributeLogicalName}'']"},"sink":{"name": "{AttributeLogicalName}","type": "Datetime"}}', NULL, N'DateTimeType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Decimal', N'decimal', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Decimal"}}', NULL, N'DecimalType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Double', N'float', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "float"}}', NULL, N'DoubleType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'EntityName', N'nvarchar', NULL, NULL, N'EntityNameType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Integer', N'int', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Int32"}}', NULL, N'IntegerType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Lookup', N'uniqueidentifier', N'{"source": { "path": "[''_{AttributeLogicalName}_value'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', N'{"source": { "path": "[''_{AttributeLogicalName}_value@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'LookupType', N'[{"secondaryType":"StringType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Memo', N'nvarchar(MAX)', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', NULL, N'MemoType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Money', N'money', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Decimal"}}', NULL, N'MoneyType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Owner', N'uniqueidentifier', N'{"source": { "path": "[''_{AttributeLogicalName}_value'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', N'{ "source": {"path": "[''_{AttributeLogicalName}_value@OData.Community.Display.V1.FormattedValue'']" }, "sink": {"name": "{AttributeLogicalName}name","type": "String" }},{ "source": {"path": "[''_{AttributeLogicalName}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']" }, "sink": {"name": "{AttributeLogicalName}type","type": "String"}}', N'OwnerType', N'[{"secondaryType":"StringType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}},{"secondaryType":"EntityNameType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Picklist', N'int', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Int32"}}', N'{"source": { "path": "[''{AttributeLogicalName}@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'PicklistType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''{AttributeOf}@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'State', N'int', N'{"source": { "path": "[''statecode'']" },"sink": {"name": "statecode","type": "Int32"}}', N'{"source": { "path": "[''statecode@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "statecodename","type": "String"}}', N'StateType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''statecode@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"statecodename","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Status', N'int', N'{"source": { "path": "[''statuscode'']" },"sink": {"name": "statuscode","type": "Int32"}}', N'{"source": { "path": "[''statuscode@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "statuscodename","type": "String"}}', N'StatusType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''statuscode@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"statuscodename","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'String', N'nvarchar', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', NULL, N'StringType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Uniqueidentifier', N'uniqueidentifier', N'{"source":{"path":"[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', NULL, N'UniqueidentifierType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Virtual', N'nvarchar', NULL, NULL, N'VirtualType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Customer', N'uniqueidentifier', N'{"source": { "path": "[''_{AttributeLogicalName}_value'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', NULL, N'CustomerType', N'[{"secondaryType":"StringType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}},{"secondaryType":"EntityNameType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Virtual', N'nvarchar(MAX)', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', N'{"source": { "path": "[''{AttributeLogicalName}@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'MultiSelectPicklistType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''{AttributeOf}@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')

END
GO
/****** Object:  StoredProcedure [ODS].[usp_LoadEntitySyncData]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_LoadEntitySyncData]
(
	@Sourcetbl [ODS].[EntitySyncType] READONLY
)
AS
BEGIN

-- Inserting new Entity Data
	MERGE ODS.EntitySync AS trgt
	USING @Sourcetbl AS src
	ON (src.EntityName=trgt.EntityName)
WHEN NOT MATCHED THEN
    	INSERT (
	[DisplayName],
	[EntityName],
	[RelativeURL],
	[AttributeSchemaURL],
	[SyncReady],
	[ChangeTrackingEnabled],
	[IsNNRelationShip],
	[SyncFrequency],
	[PrimaryIdAttribute],
	[IsBPFEntity],
	[IsActivity]
)
	VALUES (
	ISNULL(src.[DisplayName],''),
	src.[EntityName],
	'/api/data/v9.1/' + src.[RelativeURL],
	'/api/data/v9.1/EntityDefinitions(LogicalName='''+ src.EntityName + ''')?$select=LogicalName&$expand=Attributes',
	--src.[AttributeSchemaURL],
	0,
	src.[ChangeTrackingEnabled],
	src.[IsNNRelationShip],
	'Daily',
	src.[PrimaryIdAttribute],
	src.[IsBPFEntity],
	src.[IsActivity]
	);

-- Updating ChangeTracking flag if changed
	MERGE ODS.EntitySync AS trgt
	USING @Sourcetbl AS src
	ON (src.EntityName=trgt.EntityName
	AND src.ChangeTrackingEnabled <> trgt.ChangeTrackingEnabled
	)
	WHEN MATCHED THEN
	UPDATE 
	SET trgt.ChangeTrackingEnabled = src.ChangeTrackingEnabled;	

END
GO
/****** Object:  StoredProcedure [ODS].[usp_LogEndExecution]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_LogEndExecution]
(
@ExecutionId UNIQUEIDENTIFIER,
@RunStatus VARCHAR(100),
@Message NVARCHAR(500)
)
AS 
BEGIN
	UPDATE ODS.ExecutionLog
	Set RunStatus = @RunStatus,
	EndDate = GETUTCDATE(),
	Message = @Message
	Where @ExecutionId = ExecutionId 
	and RunStatus <> 'Error'
END
GO
/****** Object:  StoredProcedure [ODS].[usp_LogStartExecution]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_LogStartExecution]
(
	@ExecutionId UNIQUEIDENTIFIER,
	@PackageName VARCHAR(500),
	@RunStatus VARCHAR(100)
)
As 
BEGIN
	INSERT INTO ODS.ExecutionLog
	(ExecutionId,PackageName,RunStatus)
	VALUES
	(@ExecutionId,@PackageName,@RunStatus)

	SELECT @@IDENTITY AS LogId 
END
GO
/****** Object:  StoredProcedure [ODS].[usp_SyncEntitySchema]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_SyncEntitySchema] (
	@sourceEntityName NVARCHAR(500)
	,@PrimaryIdAttribute NVARCHAR(500)
	,@ExecutionId UNIQUEIDENTIFIER
	,@StagingDBSchema VARCHAR(100) = 'Staging'
	)
AS
BEGIN
	SET NOCOUNT ON

	IF EXISTS (
			SELECT *
			FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_NAME = @sourceEntityName
				AND Table_Schema = 'Staging'
			)
	BEGIN
		DECLARE @dropStatement NVARCHAR(500) = 'drop table Staging.' + @sourceEntityName

		EXEC sp_executeSQL @dropStatement;
	END

	BEGIN TRAN

	BEGIN TRY
		--------------------------------- STAGING TABLE ---------------------------------
		EXEC ODS.usp_CreateStagingTable @sourceEntityName
			,@PrimaryIdAttribute

		--------------------------------- STAGING TABLE END ---------------------------------
		---------------------------------NEW DATA TABLE ---------------------------------
		IF NOT EXISTS (
				SELECT *
				FROM INFORMATION_SCHEMA.TABLES
				WHERE TABLE_NAME = @sourceEntityName
					AND Table_Schema = 'dbo'
				)
		BEGIN
			-- Create New Destination Table
			DECLARE @tableCreateScript NVARCHAR(MAX) = 'Create table dbo.' + @sourceEntityName + ' (ID uniqueidentifier NOT NULL PRIMARY KEY, {Columns})'
			DECLARE @columns NVARCHAR(MAX) = ''

			SELECT @columns = @columns + AttributeLogicalName + ' ' + CASE 
					WHEN AttributeLogicalName = @PrimaryIdAttribute
						THEN ' as ID, '
					ELSE destinationSchema + CASE 
							WHEN destinationSchema = 'nvarchar'
								THEN '(' + CAST(ISNULL(CASE 
												WHEN DatabaseLength = 0
													THEN MaxLength
												ELSE DatabaseLength / 2
												END, 4000) AS NVARCHAR) + ')'
							ELSE ''
							END + CASE 
							WHEN destinationSchema = 'decimal'
								THEN '(38,' + CAST(ISNULL(PRECISION, 2) AS NVARCHAR) + ')'
							ELSE ''
							END +
						--case when AttributeLogicalName = @PrimaryIdAttribute then ' NOT NULL PRIMARY KEY' else '' end+
						','
					END
			FROM ODS.EntityAttributeSchema AS s
			JOIN ODS.EntityAttributeSchemaMapping map ON map.SourceSchema = s.AttributeType
				AND map.AttributeTypeName = s.AttributeTypeName
			WHERE s.EntityLogicalName = @sourceEntityName
				AND s.IsValid = 1
			ORDER BY AttributeLogicalName

			SELECT @columns = SUBSTRING(@columns, 0, LEN(@columns))

			SET @tableCreateScript = REPLACE(@tableCreateScript, '{Columns}', @columns);

			EXEC ODS.[usp_InsertEntitySchemaSyncLog] @ExecutionId
				,@tableCreateScript
				,@sourceEntityName

			EXEC sp_executeSQL @tableCreateScript;

			DECLARE @deleteColumns NVARCHAR(500) = 'Alter table dbo.' + @sourceEntityName + ' Add SinkCreatedOn datetime NULL default(getutcdate()),SinkModifiedOn datetime NULL default(getutcdate()), ODS_IsDeleted bit NOT NULL default(0),
		ODS_DeletedOn datetime,ODS_DeltaToken varchar(500), ExecutionId uniqueidentifier'

			EXEC sp_executeSQL @deleteColumns

			EXEC ODS.[usp_InsertEntitySchemaSyncLog] @ExecutionId
				,@deleteColumns
				,@sourceEntityName
				--declare @indexPrimaryColumn nvarchar(max) = 'CREATE INDEX idx_{0}_{1} ON dbo.{0} ({1})';
				--declare @modifiedOnIndex nvarchar(1000) = replace(replace(@indexPrimaryColumn,'{0}', @sourceEntityName),'{1}','ODS_ModifiedOn')
				--declare @deletedOnIndex nvarchar(1000) = replace(replace(@indexPrimaryColumn,'{0}', @sourceEntityName),'{1}','ODS_IsDeleted')
				--exec sp_executeSQL @modifiedOnIndex
				--exec sp_executeSQL @deletedOnIndex
				--declare @idQuery nvarchar(max) = 'alter table '+@sourceEntityName+' add ID uniqueidentifier NOT NULL PRIMARY KEY'
				--exec sp_executeSQL @idQuery
				--declare @idQueryIndex nvarchar(1000) = replace(replace(@indexPrimaryColumn,'{0}', @sourceEntityName),'{1}','ID')
				--exec sp_executeSQL @idQueryIndex
		END
		ELSE
		BEGIN
			---------------------------------NEW DATA TABLE END ---------------------------------
			---------------------------------COMPARE STAGING / DATA TABLE ---------------------------------
			-- Compare Staging table with destination table and generate dynamic statements
				;

			WITH T
			AS (
				SELECT Table_name
					,COLUMN_NAME
					,DATA_TYPE
					,CHARACTER_MAXIMUM_LENGTH
					,NUMERIC_PRECISION
					,d.NUMERIC_SCALE
				FROM INFORMATION_SCHEMA.COLUMNS AS d
				WHERE TABLE_NAME = @sourceEntityName
					AND TABLE_SCHEMA = 'Staging'
					AND COLUMN_NAME <> 'ODS_DeltaToken'
					AND COLUMN_NAME <> 'id'
					AND COLUMN_NAME <> 'reason'
					AND COLUMN_NAME <> 'Action'
					AND COLUMN_NAME <> 'ValidFrom'
					AND COLUMN_NAME <> 'ValidTo'
					AND COLUMN_NAME <> 'PERIOD'
				)
			SELECT CASE 
					---New Attribute Added in D365
					WHEN d.COLUMN_NAME IS NULL 
						THEN 'Alter table ' + s.TABLE_NAME + ' add ' + s.COLUMN_NAME + ' ' + s.DATA_TYPE + ' ' + CASE 
								WHEN s.DATA_TYPE = 'nvarchar'
									AND s.CHARACTER_MAXIMUM_LENGTH = - 1
									THEN '(MAX)'
								WHEN s.DATA_TYPE = 'nvarchar'
									AND s.CHARACTER_MAXIMUM_LENGTH <> - 1
									THEN '(' + CAST(s.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR) + ')'
								WHEN s.DATA_TYPE = 'decimal'
									THEN '(38,' + CAST(s.NUMERIC_SCALE AS NVARCHAR) + ')'
								ELSE ''
								END

					-- Attribute is deleted from D365
					WHEN s.COLUMN_NAME IS NULL
						AND d.COLUMN_NAME IS NOT NULL
						THEN 'Alter table ' + d.TABLE_NAME + ' drop column ' + d.COLUMN_NAME
					
					--Data Type changed from BIGINT to INT
					WHEN s.COLUMN_NAME = d.COLUMN_NAME
						AND s.DATA_TYPE = 'bigint'
						AND d.DATA_TYPE = 'int'
						THEN 'Alter table ' + d.TABLE_NAME + ' alter column ' + d.COLUMN_NAME + ' bigint;'

					-- Attribute created with the same schema name but different data type
					WHEN s.COLUMN_NAME = d.COLUMN_NAME
						AND s.DATA_TYPE <> d.DATA_TYPE
						THEN 'Alter table ' + d.TABLE_NAME + ' drop column ' + d.COLUMN_NAME + ';' + 'Alter table ' + s.TABLE_NAME + ' add ' + s.COLUMN_NAME + ' ' + s.DATA_TYPE + ' ' + CASE 
								WHEN (s.DATA_TYPE = 'nvarchar' or s.DATA_TYPE = 'varchar') -- NVARCHAR(MAX)
									AND s.CHARACTER_MAXIMUM_LENGTH = - 1
									THEN '(MAX)'
								WHEN (s.DATA_TYPE = 'nvarchar' or s.DATA_TYPE = 'varchar')
									AND s.CHARACTER_MAXIMUM_LENGTH <> - 1
									THEN '(' + CAST(s.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR) + ')'
								WHEN s.DATA_TYPE = 'decimal'
									THEN '(38,' + CAST(s.NUMERIC_SCALE AS NVARCHAR) + ')'
								ELSE ''
								END
					
					-- Attribute updated for change in lenght. 
					WHEN s.COLUMN_NAME = d.COLUMN_NAME
						AND (
							s.CHARACTER_MAXIMUM_LENGTH <> d.CHARACTER_MAXIMUM_LENGTH
							OR s.NUMERIC_PRECISION <> d.NUMERIC_PRECISION
							OR s.NUMERIC_SCALE <> d.NUMERIC_SCALE
							)
						THEN 'alter table ' + s.TABLE_NAME + ' alter column ' + s.COLUMN_NAME + ' ' + s.DATA_TYPE + ' ' + CASE 
								WHEN (s.DATA_TYPE = 'nvarchar' or s.DATA_TYPE = 'varchar') -- NVARCHAR(MAX)
									AND s.CHARACTER_MAXIMUM_LENGTH = - 1
									THEN '(MAX)'
								WHEN (s.DATA_TYPE = 'nvarchar' or s.DATA_TYPE = 'varchar') -- NVARCHAR(MAX)
									AND s.CHARACTER_MAXIMUM_LENGTH <> - 1
									THEN '(' + CAST(s.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR) + ')'
								WHEN s.DATA_TYPE = 'decimal'
									THEN '(38,' + CAST(s.NUMERIC_SCALE AS NVARCHAR) + ')'
								ELSE ''
								END
					ELSE NULL
					END AS Query
			INTO #temp
			FROM T AS s
			FULL JOIN (
				SELECT Table_name
					,COLUMN_NAME
					,DATA_TYPE
					,CHARACTER_MAXIMUM_LENGTH
					,NUMERIC_PRECISION
					,NUMERIC_SCALE
				FROM INFORMATION_SCHEMA.COLUMNS AS d
				WHERE TABLE_NAME = @sourceEntityName
					AND TABLE_SCHEMA = 'dbo'
					AND COLUMN_NAME <> 'ODS_IsDeleted'
					AND COLUMN_NAME <> 'ODS_DeletedOn'
					AND COLUMN_NAME <> 'ExecutionId'
					AND COLUMN_NAME <> 'ODS_DeltaToken'
					AND COLUMN_NAME <> 'SinkModifiedOn'
					AND COLUMN_NAME <> 'SinkCreatedOn'
					AND COLUMN_NAME <> 'ID'
				) AS d ON d.Table_Name = s.TABLE_NAME
				AND s.COLUMN_NAME = d.COLUMN_NAME
			--and s.AttributeType = d.DATA_TYPE
			WHERE d.TABLE_NAME IS NULL
				OR s.TABLE_NAME IS NULL
				OR s.DATA_TYPE <> d.DATA_TYPE
				OR s.CHARACTER_MAXIMUM_LENGTH <> d.CHARACTER_MAXIMUM_LENGTH
				OR s.NUMERIC_PRECISION <> d.NUMERIC_PRECISION
				OR s.NUMERIC_SCALE <> d.NUMERIC_SCALE

			-- Cursor to execute the dynamic statements
			DECLARE @query NVARCHAR(MAX) = ''

			DECLARE cur CURSOR
			FOR
			SELECT Query
			FROM #temp

			OPEN cur

			FETCH NEXT
			FROM cur
			INTO @query

			WHILE @@FETCH_STATUS = 0
			BEGIN
				PRINT @query;

				EXEC ODS.[usp_InsertEntitySchemaSyncLog] @ExecutionId
					,@query
					,@sourceEntityName

				EXEC sp_executeSQL @query;

				FETCH NEXT
				FROM cur
				INTO @query
			END

			CLOSE cur

			DEALLOCATE cur
		END

		---------------------------------COMPARE STAGING / DATA TABLE END ---------------------------------
		--------------------------------- REMOVE STAGING TABLE ---------------------------------
		EXEC ODS.usp_TruncateStagingTable @sourceEntityName
			,@StagingDBSchema

		--------------------------------- REMOVE STAGING TABLE END---------------------------------
		DECLARE @IsSystemVersioningEnabled BIT;

		SELECT @IsSystemVersioningEnabled = IsSystemVersioningEnabled
		FROM ODS.EntitySync
		WHERE EntityName = @sourceEntityName;

		IF @IsSystemVersioningEnabled = 1
			AND OBJECTPROPERTY(OBJECT_ID(@sourceEntityName), 'TableTemporalType') <> 2
		BEGIN
			DECLARE @sql NVARCHAR(MAX) = 'alter table dbo.' + @sourceEntityName + '
		add
		ValidFrom  datetime2 generated always as row start not null default getutcdate(),
		ValidTo    datetime2 generated always as row end   not null default convert(datetime2, ''9999-12-31 23:59:59.9999999''),
		Period for system_time (ValidFrom, ValidTo);

		alter table dbo.' + @sourceEntityName + '
		set (system_versioning = on (HISTORY_TABLE = History.[' + @sourceEntityName + ']));
						
		CREATE NONCLUSTERED INDEX IX_' + @sourceEntityName + 'History_ID_PERIOD_COLUMNS
		ON History.' + @sourceEntityName + ' (ValidTo, ValidFrom, ' + @PrimaryIdAttribute + ');
'

			-- TO DO Include COLUMNSTORE in PROD
			--CREATE CLUSTERED COLUMNSTORE INDEX IX_'+@sourceEntityName+'History
			-- ON History.'+@sourceEntityName+';
			--print @sql 	
			EXEC sp_executeSQL @sql;
		END

		COMMIT TRAN
	END TRY

	BEGIN CATCH
		ROLLBACK TRAN THROW;
	END CATCH
END
GO
/****** Object:  StoredProcedure [ODS].[usp_TruncateStagingTable]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_TruncateStagingTable]
(
	@sourceEntityName VARCHAR(500), 
	@databaseSchema VARCHAR(100)
)
AS 
BEGIN

DECLARE @query NVARCHAR(1000) =
'IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''{TB}'' and Table_Schema = ''{SCH}'') 
BEGIN TRUNCATE TABLE {SCH}.{TB}; END'

SET @query = REPLACE(Replace(@query, '{SCH}',@databaseSchema),'{TB}', @sourceEntityName)

EXEC sp_executeSQL @query
END
GO
/****** Object:  StoredProcedure [ODS].[usp_UpdateAttributeMapping_WithJson]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:  Microsoft    
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_UpdateAttributeMapping_WithJson]
(
	@sourceTableName NVARCHAR(500) , 
	@isActivity BIT = 0, 
	@primaryColumnName NVARCHAR(100) = 'accountid'
)
AS
BEGIN

DECLARE @primary VARCHAR(MAX) = '', @firstInsertMapping VARCHAR(MAX)

DECLARE @baseMapping VARCHAR(MAX)= 
'{"type": "TabularTranslator","mappings": [{"source": {"path": "$[''@odata.deltaLink'']"},"sink": {"name": "ODS_DeltaToken","type": "String"}},{MappingColumns}],"collectionReference": "$[''value'']","mapComplexValuesToString": false}';

DECLARE @baseMappingFirstInsert VARCHAR(MAX)= 
'{"type": "TabularTranslator","mappings": [{"source":{"path":"$[''ExecutionId'']"},"sink":{"name":"ExecutionId","type":"Guid"}},{"source": {"path": "$[''@odata.deltaLink'']"},"sink": {"name": "ODS_DeltaToken","type": "String"}},{MappingColumns}],"collectionReference": "$[''value'']","mapComplexValuesToString": false}';

--declare @baseMappingFirstInsert varchar(max)= 
--'{"type": "TabularTranslator","mappings": [{MappingColumns}],"collectionReference": "$[''value'']","mapComplexValuesToString": false}';

WITH T AS (
SELECT s.AttributeLogicalName,
REPLACE(PrimaryColumnMapping, '{AttributeLogicalName}', s.AttributeLogicalName) AS PrimaryColumnMapping

FROM ODS.EntityAttributeSchema AS s
JOIN ODS.EntityAttributeSchemaMapping m
	ON s.AttributeType = m.SourceSchema
	AND s.AttributeTypeName = m.AttributeTypeName

WHERE s.EntityLogicalName = @sourceTableName
AND PrimaryColumnMapping IS NOT NULL
AND (
		(@isActivity = 0 AND s.IsValidODataAttribute = 1)
		OR
		(@isActivity = 1 AND AttributeLogicalName NOT LIKE '%name')
	)
AND s.IsValid = 1
)

SELECT @primary = @primary + PrimaryColumnMapping + ',' 

FROM T

;WITH M AS
(
	SELECT SecondaryType, Mapping, SourceSchema, DestinationSchema, AttributeTypeName FROM ods.EntityAttributeSchemaMapping 
	CROSS APPLY OPENJSON ( SecondaryColumnMappingJson )  
WITH (   
              SecondaryType   NVARCHAR(200)   '$.secondaryType', 
			   [Mapping]  NVARCHAR(MAX)  AS JSON 
 )
)
SELECT @primary = @primary + 
REPLACE(REPLACE(m.Mapping,'{AttributeOf}',s.AttributeOf), '{AttributeLogicalName}',s.AttributeLogicalName) 
+','

FROM ODS.EntityAttributeSchema	AS s
	JOIN Ods.EntityAttributeSchema AS p
		ON p.AttributeLogicalName = s.AttributeOf
			AND p.EntityLogicalName = s.EntityLogicalName
	JOIN M	
		ON M.SecondaryType = s.AttributeTypeName
		AND M.SourceSchema = p.AttributeType

WHERE p.EntityLogicalName = @sourceTableName
AND (
		(@isActivity = 0 AND p.IsValidODataAttribute = 1)
		OR
		(@isActivity = 1 AND p.AttributeLogicalName NOT LIKE '%name')
	)
AND p.IsValid = 1
AND s.IsValid = 1



-- Special mapping for string map table
SELECT @primary = @primary + 
'{"source": { "path": "[''objecttypecode'']" },"sink": {"name": "objecttypecode","type": "String"}},
{"source": { "path": "[''objecttypecode@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "objecttypecodename","type": "String"}}'
+','
FROM Ods.EntityAttributeSchema
WHERE EntityLogicalName = @sourceTableName
AND AttributeOf IS NULL AND AttributeLogicalName = 'objecttypecode'


SET @firstInsertMapping = @primary;
SET @firstInsertMapping = REPLACE(@firstInsertMapping
,'"sink": {"name": "'+@primaryColumnName+'","type": "Guid"}','"sink": {"name": "ID","type": "Guid"}')
--"sink": {"name": "{AttributeLogicalName}","type": "Guid"}

SET @primary = @primary + '{"source":{"path":"[''reason'']"},"sink":{"name":"reason","type":"String"}},{"source":{"path":"[''id'']"},"sink":{"name":"id","type":"Guid"}}'
--set @primary = SUBSTRING(@primary, 0, len(@primary))


UPDATE ODS.EntitySync
SET Mapping = REPLACE(@baseMapping, '{MappingColumns}', @primary),
FirstInsertMapping = REPLACE(@baseMappingFirstInsert, '{MappingColumns}', @firstInsertMapping) 

WHERE EntityName = @sourceTableName

END
GO
/****** Object:  StoredProcedure [ODS].[usp_UpdateDeltaToken]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_UpdateDeltaToken]
(
	@sourceTableName NVARCHAR(500), 
	@sourceSchema NVARCHAR(100),
	@pipelineRunId NVARCHAR(100)
)
AS 
BEGIN


DECLARE @count INT
DECLARE @countInt INT
DECLARE @sqlCount NVARCHAR(MAX) = 'select @countInt = count(1)  from '+@sourceSchema+'.' + @sourceTableName
exec sp_executeSQL @sqlCount, N'@countInt int output',
@countInt = @countInt out

If (@countInt = 0)
	RETURN;
	DECLARE @sql NVARCHAR(MAX) = 

	'DECLARE @deltaToken NVARCHAR(500) = null

	SELECT TOP(1) @deltaToken =
	SUBSTRING(ODS_DeltaToken, 
	CHARINDEX(''/api'', ODS_DeltaToken),
	LEN(ODS_DeltaToken) - CHARINDEX(''/api'', ODS_DeltaToken) + 1)

	FROM '+@sourceSchema+'.'+@sourceTableName+' where ODS_DeltaToken is not null or ODS_DeltaToken <>'''';
	
	UPDATE ODS.ENTITYSYNC
	SET DeltaToken = @deltaToken
	WHERE @deltaToken IS NOT NULL AND EntityName = '''+@sourceTableName +''';
	
	INSERT INTO [ODS].[EntityDeltaTokenLog]
	(ExecutionId,PipelineRunId,EntityName,DeltaToken)
	SELECT '''+@pipelineRunId+''','''+@pipelineRunId+''',''' + @sourceTableName + ''',@deltaToken WHERE @deltaToken IS NOT NULL;'	

	EXEC sp_executeSql @sql

	UPDATE ODS.EntitySync
		SET LastSyncTimeStamp = GETUTCDATE()
	WHERE EntityName = @sourceTableName;

	EXEC [ODS].[usp_TruncateStagingTable] @sourceEntityName=@sourceTableName,  @databaseSchema = 'Staging'

END


GO
/****** Object:  StoredProcedure [ODS].[usp_UpsertOptionSetMetadata]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_UpsertOptionSetMetadata]
AS
BEGIN

SELECT DISTINCT EntityName, OptionSetName, GlobalOptionSetName,OptionValue as [Option],OptionValueText as LocalizedLabel,LanguageCode as LocalizedLabelLanguageCode, 0 as IsUserLocalizedLabel
INTO #tempOptionSet 
FROM Staging.OptionSetMetadata
CROSS APPLY OPENJSON (OptionJson) with(
  OptionValue int '$.Value', OptionValueText nvarchar(255) '$.Label.UserLocalizedLabel.Label', LanguageCode int '$.Label.UserLocalizedLabel.LanguageCode'
)
WHERE IsGlobal =0

SELECT DISTINCT EntityName, OptionSetName, GlobalOptionSetName,OptionValue as [Option],OptionValueText as LocalizedLabel,LanguageCode as LocalizedLabelLanguageCode, 0 as IsUserLocalizedLabel
INTO #tempGlobalOptionSet 
FROM Staging.OptionSetMetadata
CROSS APPLY OPENJSON (OptionJson) with(
  OptionValue int '$.Value', OptionValueText nvarchar(255) '$.Label.UserLocalizedLabel.Label', LanguageCode int '$.Label.UserLocalizedLabel.LanguageCode'
)
WHERE IsGlobal =1

-- Local Option Set
MERGE OptionSetMetadata as tg
USING #tempOptionSet as sr
ON	sr.[EntityName] = tg.[EntityName] AND
	sr.[OptionSetName] = tg.[OptionSetName] AND
	sr.[Option] = tg.[Option] AND 
	sr.[IsUserLocalizedLabel] = tg.[IsUserLocalizedLabel] AND
	sr.[LocalizedLabelLanguageCode] = tg.[LocalizedLabelLanguageCode]
	
WHEN NOT MATCHED BY TARGET THEN
INSERT  ([EntityName]
        ,[OptionSetName]
        ,[Option]
        ,[IsUserLocalizedLabel]
        ,[LocalizedLabelLanguageCode]
        ,[LocalizedLabel])
     VALUES
     (
		sr.[EntityName]
        ,sr.[OptionSetName]
        ,sr.[Option]
        ,sr.[IsUserLocalizedLabel]
        ,sr.[LocalizedLabelLanguageCode]
        ,sr.[LocalizedLabel]
	)
 WHEN MATCHED THEN UPDATE SET
	tg.[LocalizedLabel] = sr.[LocalizedLabel];


--- Global Option Set
MERGE GlobalOptionSetMetadata as tg
USING #tempGlobalOptionSet as sr
ON	sr.[GlobalOptionSetName] = tg.[GlobalOptionSetName] AND
	sr.[OptionSetName] = tg.[OptionSetName] AND
	sr.[Option] = tg.[Option] AND 
	sr.[IsUserLocalizedLabel] = tg.[IsUserLocalizedLabel] AND
	sr.[LocalizedLabelLanguageCode] = tg.[LocalizedLabelLanguageCode] AND
	sr.[EntityName] = tg.[EntityName] 	
WHEN NOT MATCHED BY TARGET THEN
INSERT  ([EntityName]
        ,[OptionSetName]
        ,[Option]
        ,[IsUserLocalizedLabel]
        ,[LocalizedLabelLanguageCode]
        ,[LocalizedLabel]
		,[GlobalOptionSetName])
     VALUES
     (
		sr.[EntityName]
        ,sr.[OptionSetName]
        ,sr.[Option]
        ,sr.[IsUserLocalizedLabel]
        ,sr.[LocalizedLabelLanguageCode]
        ,sr.[LocalizedLabel]
		,sr.[GlobalOptionSetName]
	)
 WHEN MATCHED THEN UPDATE SET
	tg.[LocalizedLabel] = sr.[LocalizedLabel];

DROP TABLE #tempGlobalOptionSet
DROP TABLE #tempOptionSet

END
GO
/****** Object:  StoredProcedure [ODS].[usp_ValidateAttributes]    Script Date: 4/13/2023 9:38:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author: Microsoft     
-- Create Date: 
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [ODS].[usp_ValidateAttributes]
(
	@sourceTableName VARCHAR(500)
)
AS
BEGIN
	UPDATE ODS.EntityAttributeSchema
	SET IsValid = 0
	WHERE EntityLogicalName = @sourceTableName
	AND AttributeLogicalName like '%yominame'

	UPDATE ODS.EntityAttributeSchema
	SET IsValid = 0
	WHERE EntityLogicalName = @sourceTableName
	AND AttributeLogicalName like '%typecode' and AttributeType = 'EntityName'
	

	-- This update will ignore the OptionSet Name being an extra column in the tables. But will aloow only for StrigMap table
	UPDATE ODS.EntityAttributeSchema
	SET IsValid = 0
	WHERE EntityLogicalName = @sourceTableName
	AND attributetypename = 'VirtualType'
	AND EntityLogicalName <> 'stringmap'  -- Special condition for string map
END
GO


INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'BigInt', N'bigint', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Int64"}}', NULL, N'BigIntType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Boolean', N'bit', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Boolean"}}', N'{"source": { "path": "[''{AttributeLogicalName}@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'BooleanType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''{AttributeOf}@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'DateTime', N'datetime', N'{"source":{"path":"[''{AttributeLogicalName}'']"},"sink":{"name": "{AttributeLogicalName}","type": "Datetime"}}', NULL, N'DateTimeType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Decimal', N'decimal', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Decimal"}}', NULL, N'DecimalType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Double', N'decimal', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Decimal"}}', NULL, N'DoubleType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'EntityName', N'nvarchar', NULL, NULL, N'EntityNameType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Integer', N'int', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Int32"}}', NULL, N'IntegerType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Lookup', N'uniqueidentifier', N'{"source": { "path": "[''_{AttributeLogicalName}_value'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', N'{"source": { "path": "[''_{AttributeLogicalName}_value@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'LookupType', N'[{"secondaryType":"StringType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}},{"secondaryType":"EntityNameType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Memo', N'nvarchar(MAX)', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', NULL, N'MemoType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Money', N'decimal', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Decimal"}}', NULL, N'MoneyType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Owner', N'uniqueidentifier', N'{"source": { "path": "[''_{AttributeLogicalName}_value'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', N'{ "source": {"path": "[''_{AttributeLogicalName}_value@OData.Community.Display.V1.FormattedValue'']" }, "sink": {"name": "{AttributeLogicalName}name","type": "String" }},{ "source": {"path": "[''_{AttributeLogicalName}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']" }, "sink": {"name": "{AttributeLogicalName}type","type": "String"}}', N'OwnerType', N'[{"secondaryType":"StringType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}},{"secondaryType":"EntityNameType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Picklist', N'int', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Int32"}}', N'{"source": { "path": "[''{AttributeLogicalName}@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'PicklistType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''{AttributeOf}@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'State', N'int', N'{"source": { "path": "[''statecode'']" },"sink": {"name": "statecode","type": "Int32"}}', N'{"source": { "path": "[''statecode@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "statecodename","type": "String"}}', N'StateType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''statecode@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"statecodename","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Status', N'int', N'{"source": { "path": "[''statuscode'']" },"sink": {"name": "statuscode","type": "Int32"}}', N'{"source": { "path": "[''statuscode@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "statuscodename","type": "String"}}', N'StatusType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''statuscode@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"statuscodename","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'String', N'nvarchar', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', NULL, N'StringType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Uniqueidentifier', N'uniqueidentifier', N'{"source":{"path":"[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', NULL, N'UniqueidentifierType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Virtual', N'nvarchar', NULL, NULL, N'VirtualType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Customer', N'uniqueidentifier', N'{"source": { "path": "[''_{AttributeLogicalName}_value'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', NULL, N'CustomerType', N'[{"secondaryType":"StringType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}},{"secondaryType":"EntityNameType","Mapping":{"source":{"path":"[''_{AttributeOf}_value@Microsoft.Dynamics.CRM.lookuplogicalname'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Virtual', N'nvarchar(MAX)', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', N'{"source": { "path": "[''{AttributeLogicalName}@OData.Community.Display.V1.FormattedValue'']" },"sink": {"name": "{AttributeLogicalName}name","type": "String"}}', N'MultiSelectPicklistType', N'[{"secondaryType":"VirtualType","Mapping":{"source":{"path":"[''{AttributeOf}@OData.Community.Display.V1.FormattedValue'']"},"sink":{"name":"{AttributeLogicalName}","type":"String"}}}]')
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Virtual', N'varchar(MAX)', N'{"source": { "path": "[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "String"}}', NULL, N'ImageType', NULL)
INSERT [ODS].[EntityAttributeSchemaMapping] ([SourceSchema], [DestinationSchema], [PrimaryColumnMapping], [SecondaryColumnMapping], [AttributeTypeName], [SecondaryColumnMappingJson]) VALUES (N'Virtual', N'uniqueidentifier', N'{"source":{"path":"[''{AttributeLogicalName}'']" },"sink": {"name": "{AttributeLogicalName}","type": "Guid"}}', NULL, N'FileType', NULL)
GO

