IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerRemove]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
     EXEC df_ChangeTrackingTriggerRemove
GO

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerCreate]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
     DROP PROCEDURE [dbo].[df_ChangeTrackingTriggerCreate]
GO

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTrackingTriggerRemove]') AND OBJECTPROPERTY(ID, N'IsProcedure') = 1)
     DROP PROCEDURE [dbo].[df_ChangeTrackingTriggerRemove]
GO

IF EXISTS (SELECT * FROM [DBO].SYSOBJECTS WHERE ID = Object_ID(N'[DBO].[df_ChangeTracking]') AND OBJECTPROPERTY(ID, N'IsTable') = 1)
     DROP TABLE [dbo].[df_ChangeTracking]
GO 