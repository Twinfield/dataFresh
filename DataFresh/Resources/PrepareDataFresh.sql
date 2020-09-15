IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerCreate]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
     DROP PROCEDURE [dbo].[df_ChangeTrackingTriggerCreate]
GO

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerRemove]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
     DROP PROCEDURE [dbo].[df_ChangeTrackingTriggerRemove]
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[df_ChangeTracking]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
	DROP TABLE [dbo].[df_ChangeTracking]
GO

CREATE TABLE [dbo].[df_ChangeTracking]
	(
		[TABLESCHEMA] sysname,
		[TABLENAME] sysname
	)
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
		SELECT [table_schema], [table_name] FROM information_schema.tables WHERE table_type = 'BASE TABLE' 

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

CREATE PROCEDURE dbo.[df_ChangeTrackingTriggerRemove]
AS
	DECLARE @sql NVARCHAR(4000)
	DECLARE @TableSchema VARCHAR(255)
	DECLARE @TableName VARCHAR(255)

	DECLARE Table_Cursor CURSOR FOR
		SELECT [table_schema], [table_name] FROM information_schema.tables WHERE table_type = 'BASE TABLE'

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