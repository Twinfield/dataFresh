IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangedTableDataRefresh]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
	  DROP PROCEDURE [dbo].[df_ChangedTableDataRefresh]
GO 

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerCreate]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
	  DROP PROCEDURE [dbo].[df_ChangeTrackingTriggerCreate]
GO

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerRemove]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
	  DROP PROCEDURE [dbo].[df_ChangeTrackingTriggerRemove]
GO

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_TableDataExtract]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[df_TableDataExtract]
GO 

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_TableDataImport]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
	  DROP PROCEDURE [dbo].[df_TableDataImport]
GO 

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[df_ChangeTracking]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
	DROP TABLE [dbo].[df_ChangeTracking]
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[df_DeleteDataInChunks]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
	DROP TABLE [dbo].[df_DeleteDataInChunks]
GO


CREATE TABLE [dbo].[df_ChangeTracking]
	(
		[TABLESCHEMA] sysname,
		[TABLENAME] sysname
	)
GO

CREATE PROCEDURE dbo.[df_DeleteDataInChunks]
	@TableSchema VARCHAR(255),
	@TableName VARCHAR(255)
AS
	DECLARE @sql NVARCHAR(4000)

	SET @sql = N'
		DECLARE @DeleteChunk INT = 500
		DECLARE @rowcount INT = 1

		WHILE @rowcount > 0
		BEGIN
			DELETE TOP (@DeleteChunk) FROM [' + @TableSchema + '].[' + @TableName +'] WITH(ROWLOCK)
			SET @rowcount = @@RowCount
		END'
	
	EXEC sp_executesql @sql
GO

	
CREATE PROCEDURE dbo.[df_ChangedTableDataRefresh]
AS
	DECLARE @sql NVARCHAR(4000)
	DECLARE @columnNameList NVARCHAR(4000)
	DECLARE @TableSchema VARCHAR(255)
	DECLARE @TableName VARCHAR(255)

	SELECT DISTINCT TableSchema, TableName INTO #ChangedTables FROM df_ChangeTracking

	TRUNCATE TABLE df_ChangeTracking

	DECLARE Table_Cursor INSENSITIVE SCROLL CURSOR FOR
		SELECT [tableschema], [tablename] from #ChangedTables
		UNION
		SELECT DISTINCT
				OBJECT_SCHEMA_NAME(fkeyid) AS Referenced_Table_Schema,
				OBJECT_NAME(fkeyid) AS Referenced_Table_Name
		FROM 
			sysreferences sr
			INNER JOIN #ChangedTables ct ON sr.rkeyid = OBJECT_ID(ct.tablename)

	OPEN Table_Cursor

	-- Deactivate Constrains for tables referencing changed tables
	FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName

	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @sql = N'Alter Table [' + @TableSchema + '].[' + @TableName + '] NOCHECK CONSTRAINT ALL'
			EXEC sp_executesql @sql

			FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName
	END

	-- Delete All data from Changed Tables and Refill
	DECLARE ChangedTable_Cursor CURSOR FOR
		SELECT [tableschema], [tablename] FROM #ChangedTables
		WHERE tablename NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
			AND tablename NOT LIKE '%__backup'

	OPEN ChangedTable_Cursor
	FETCH NEXT FROM ChangedTable_Cursor INTO @TableSchema, @TableName
	WHILE (@@Fetch_Status = 0)
	BEGIN
			EXEC [dbo].[df_DeleteDataInChunks] @TableSchema, @TableName

			SET @sql = N'DELETE FROM df_ChangeTracking WHERE TableName=''' + @TableName + ''' and TableSchema=''' + @TableSchema + ''''
			EXEC sp_executesql @sql
			
			SET @sql = N'IF(SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ''' + @TableSchema + ''' AND table_name = ''' + @TableName + ''' AND IDENT_SEED(TABLE_NAME) IS NOT NULL) > 0
			BEGIN
				DBCC CHECKIDENT ([' + @TableSchema + '.' + @TableName + '], RESEED, 0)
			END'

			EXEC sp_executesql @sql
	
			SET @columnNameList = STUFF((select ',[' + a.name + ']'
				from sys.all_columns a
				join sys.tables t on a.object_id = t.object_id 
				where t.object_id = object_id('[' + @TableSchema + '].[' + @TableName + ']')
					and EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = t.object_id)
				for xml path ('')
				),1,1,'');

			IF (@columnNameList IS NULL) 
				SET @sql = N'INSERT INTO [' + @TableSchema + '].[' + @TableName + ']
					SELECT * FROM [' + @TableSchema + '].[' + @TableName + '__backup];'
			ELSE
				SET @sql = N'SET IDENTITY_INSERT [' + @TableSchema + '].[' + @TableName + '] ON;
				INSERT INTO [' + @TableSchema + '].[' + @TableName + '](' + @columnNameList + ')
					SELECT * FROM [' + @TableSchema + '].[' + @TableName + '__backup];
				SET IDENTITY_INSERT [' + @TableSchema + '].[' + @TableName + '] OFF;'
			EXEC sp_executesql @sql

			FETCH NEXT FROM ChangedTable_Cursor INTO @TableSchema, @TableName
	END
	CLOSE ChangedTable_Cursor
	DEALLOCATE ChangedTable_Cursor

	-- ReEnable Constrants for All Tables
	FETCH FIRST FROM Table_Cursor INTO @TableSchema, @TableName
	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @sql = N'Alter Table [' + @TableSchema + '].[' + @TableName + '] CHECK CONSTRAINT ALL'
			EXEC sp_executesql @sql

			FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName
	END
	CLOSE Table_Cursor
	DEALLOCATE Table_Cursor
GO

CREATE PROCEDURE dbo.[df_ChangeTrackingTriggerCreate]
AS

	IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[df_ChangeTracking]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
	CREATE TABLE [df_ChangeTracking]
	(
		[TABLESCHEMA] sysname,
		[TABLENAME] sysname
	)

	DECLARE @sql NVARCHAR(4000)
	DECLARE @TableSchema VARCHAR(255)
	DECLARE @TableName VARCHAR(255)

	DECLARE Table_Cursor CURSOR FOR
		SELECT [table_schema], [table_name]
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
			and [table_name] NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
			and [table_name] NOT LIKE '%__backup'

	OPEN Table_Cursor
	FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName

	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @sql = N'IF EXISTS (SELECT * FROM dbo.SYSOBJECTS WHERE ID = Object_ID(N''[' + @TableSchema + '].[trig_df_ChangeTracking_' + @TableName + ']'') AND OBJECTPROPERTY(ID, N''IsTrigger'') = 1) 
				DROP TRIGGER [' + @TableSchema + '].[trig_df_ChangeTracking_' + @TableName + ']'
			EXEC sp_executesql @sql

			SET @sql = N'CREATE TRIGGER [' + @TableSchema + '].[trig_df_ChangeTracking_' + @TableName + '] on [' + @TableSchema + '].[' + @TableName + '] for insert, update, delete
			as
			SET NOCOUNT ON
			INSERT INTO df_ChangeTracking (tableschema, tablename) VALUES (''' + @TableSchema + ''', ''' + @TableName + ''')
			SET NOCOUNT OFF' 
			
			EXEC sp_executesql @sql

			FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName

	END
	CLOSE Table_Cursor
	DEALLOCATE Table_Cursor

GO

CREATE PROCEDURE dbo.[df_TableDataExtract]
AS
	DECLARE @CMD NVARCHAR(4000)
	
	DECLARE Table_Cursor CURSOR FOR
		SELECT N'IF OBJECT_ID(''' + Table_Schema + '.' + Table_Name + '__backup'', ''U'') IS NOT NULL
			DROP TABLE [' + Table_Schema + '].[' + Table_Name + '__backup];
			SELECT * INTO [' + Table_Schema + '].[' + Table_Name + '__backup]
				FROM [' + Table_Schema + '].[' + Table_Name + ']'
		FROM Information_Schema.tables
		WHERE table_type = 'BASE TABLE'
			AND [table_name] NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
			AND [table_name] NOT LIKE '%__backup'

	OPEN Table_Cursor
	FETCH NEXT FROM Table_Cursor INTO @CMD

	WHILE (@@Fetch_Status = 0)
	BEGIN
		EXEC sp_executesql @CMD
		FETCH NEXT FROM Table_Cursor INTO @CMD
	END

	CLOSE Table_Cursor
	Deallocate Table_Cursor
	
GO

CREATE PROCEDURE dbo.[df_TableDataImport]
AS

	DECLARE @sql NVARCHAR(4000)
	DECLARE @columnNameList nvarchar(MAX)
	DECLARE @TableSchema VARCHAR(255)
	DECLARE @TableName VARCHAR(255)

	SELECT Table_Schema as TableSchema, Table_Name as TableName INTO #UserTables
		FROM Information_Schema.tables
		WHERE table_type = 'BASE TABLE'
			AND [table_name] NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
			AND [table_name] NOT LIKE '%__backup'

	DECLARE Table_Cursor INSENSITIVE SCROLL CURSOR FOR
		SELECT [tableschema], [tablename] FROM #UserTables

	OPEN Table_Cursor

	-- Deactivate Constrains for tables referencing changed tables
	FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName

	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @sql = N'Alter Table [' + @TableSchema + '].[' + @TableName + '] NOCHECK CONSTRAINT ALL'
			EXEC sp_executesql @sql

			FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName
	END

	-- Delete All data from Changed Tables and Refill
	DECLARE UserTable_Cursor CURSOR FOR
		SELECT [tableschema], [tablename]
		FROM #UserTables
		WHERE tablename NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
			AND tablename NOT LIKE '%__backup'

	OPEN UserTable_Cursor

	FETCH NEXT FROM UserTable_Cursor INTO @TableSchema, @TableName
	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @columnNameList = STUFF((select ',[' + a.name + ']'
					from sys.all_columns a
					join sys.tables t on a.object_id = t.object_id 
					where t.object_id = object_id('[' + @TableSchema + '].[' + @TableName + ']')
						and EXISTS (SELECT 1 FROM sys.identity_columns WHERE object_id = t.object_id)
					for xml path ('')
					),1,1,'');
			PRINT @columnNameList

			EXEC [dbo].[df_DeleteDataInChunks] @TableSchema, @TableName

			IF (@columnNameList IS NULL) 
				SET @sql = 
					N'INSERT INTO [' + @TableSchema + '].[' + @TableName + ']
						SELECT * FROM [' + @TableSchema + '].[' + @TableName + '__backup];'
			ELSE
				SET @sql = 
					N'SET IDENTITY_INSERT [' + @TableSchema + '].[' + @TableName + '] ON;
					INSERT INTO [' + @TableSchema + '].[' + @TableName + '](' + @columnNameList + ')
						SELECT * FROM [' + @TableSchema + '].[' + @TableName + '__backup];
					SET IDENTITY_INSERT [' + @TableSchema + '].[' + @TableName + '] OFF;'
			EXEC sp_executesql @sql

			FETCH NEXT FROM UserTable_Cursor INTO @TableSchema, @TableName

	END
	CLOSE UserTable_Cursor
	DEALLOCATE UserTable_Cursor

	-- ReEnable Constrants for All Tables
	FETCH FIRST FROM Table_Cursor INTO @TableSchema, @TableName
	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @sql = N'Alter Table [' + @TableSchema + '].[' + @TableName + '] CHECK CONSTRAINT ALL'
			EXEC sp_executesql @sql
			
			FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName
	END

	CLOSE Table_Cursor
	DEALLOCATE Table_Cursor

GO

CREATE PROCEDURE dbo.[df_ChangeTrackingTriggerRemove]
AS
	DECLARE @sql NVARCHAR(4000)
	DECLARE @TableSchema VARCHAR(255)
	DECLARE @TableName VARCHAR(255)

	DECLARE Table_Cursor CURSOR FOR
		SELECT [table_schema], [table_name]
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
			AND [table_name] NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
			AND [table_name] NOT LIKE '%__backup'

	OPEN Table_Cursor
	FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName

	WHILE (@@Fetch_Status = 0)
	BEGIN
			SET @sql = N'IF EXISTS (SELECT * FROM DBO.SYSOBJECTS WHERE ID = Object_ID(N''[' + @TableSchema + '].[trig_df_ChangeTracking_' + @TableName + ']'') AND OBJECTPROPERTY(ID, N''IsTrigger'') = 1) 
				DROP TRIGGER [' + @TableSchema + '].[trig_df_ChangeTracking_' + @TableName + ']' 
			
			EXEC sp_executesql @sql

			FETCH NEXT FROM Table_Cursor INTO @TableSchema, @TableName

	END
	CLOSE Table_Cursor
	DEALLOCATE Table_Cursor
	
	IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[df_ChangeTracking]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
		DROP TABLE [dbo].[df_ChangeTracking]

GO