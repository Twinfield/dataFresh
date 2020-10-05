// EntropyZero dataFresh Copyright (C) 2007 EntropyZero Consulting, LLC.
// Please visit us on the web: http://blogs.ent0.com/
//
// This library is free software; you can redistribute it and/or modify 
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation; either version 2.1 of the 
// License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful, but 
// WITHOUT ANY WARRANTY; without even the implied warranty of 
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU 
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public 
// License along with this library; if not, write to:
// Free Software Foundation, Inc., 
// 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 

using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace DataFresh
{
	/// <summary>
	/// dataFresh by Entropy Zero Consulting is a library that will enable
	/// the test driven developer to build a test harness that will refresh
	/// the database to a known state between tests
	/// </summary>
	public class SqlDataFresh : IDataFresh
	{
		readonly string connectionString;
		public const string ChangeTrackingTableName = "df_ChangeTracking";
		const string SnapshotTableSuffix = "__backup";

		sealed class TableMetadata
		{
			public string Schema { get; set; }
			public string Name { get; set; }
		}

		readonly bool verbose;

		public SqlDataFresh(string connectionString)
		{
			this.connectionString = connectionString;
		}

		public SqlDataFresh(string connectionString, bool verbose)
		{
			this.connectionString = connectionString;
			this.verbose = verbose;
		}

		public void PrepareDatabaseForDataFresh()
		{
			PrepareDatabaseForDataFresh(true);
		}

		public void PrepareDatabaseForDataFresh(bool createSnapshot)
		{
			var before = DateTime.Now;
			var cb = new SqlConnectionStringBuilder(connectionString);
			var mode = createSnapshot ? "(with snapshot creation)" : string.Empty;
			ConsoleWrite($"PrepareDatabaseForDataFresh for {cb.InitialCatalog} started {mode}");

			PrepareDataFresh();

			if (createSnapshot)
				CreateSnapshot();

			ConsoleWrite($"PrepareDatabaseForDataFresh for {cb.InitialCatalog} complete: " + (DateTime.Now - before));
		}

		public void CreateSnapshot()
		{
			var before = DateTime.Now;
			var cb = new SqlConnectionStringBuilder(connectionString);

			ConsoleWrite($"CreateSnapshot for {cb.InitialCatalog} started");
			GuardDatabaseIsPrepared();

			var tables = GetAllUserTables();
			PerformBulkBackup(tables, cb);

			ConsoleWrite($"CreateSnapshot for {cb.InitialCatalog} complete: {DateTime.Now - before}");
		}

		void PrepareDataFresh()
		{
			var tables = GetAllUserTables();

			ExecuteNonQuery(@"
				IF OBJECT_ID (N'[dbo].[df_ChangeTracking]', N'U') IS NOT NULL
					DROP TABLE [dbo].[df_ChangeTracking]

				CREATE TABLE [dbo].[df_ChangeTracking]
				(
					[TableSchema] SYSNAME,
					[TableName] SYSNAME
				)");

			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				foreach (var t in tables)
				{
					ExecuteNonQuery($@"
						IF (OBJECT_ID(N'[{t.Schema}].[trig_df_ChangeTracking_{t.Name}]') IS NOT NULL)
						BEGIN
							DROP TRIGGER [{t.Schema}].[trig_df_ChangeTracking_{t.Name}]
						END", connection);

					ExecuteNonQuery($@"
						CREATE TRIGGER [{t.Schema}].[trig_df_ChangeTracking_{t.Name}] on [{t.Schema}].[{t.Name}]
							FOR INSERT, UPDATE, DELETE
						AS
							SET NOCOUNT ON
							INSERT INTO df_ChangeTracking(TableSchema, TableName)
								VALUES('{t.Schema}', '{t.Name}')
							SET NOCOUNT OFF
						", connection);
				}
			}
		}

		void RemoveDataFresh()
		{
			var tables = GetAllUserTables();

			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();

				foreach (var t in tables)
				{
					var sql = $@"
						IF (OBJECT_ID(N'[{t.Schema}].[trig_df_ChangeTracking_{t.Name}]') IS NOT NULL)
						BEGIN
							DROP TRIGGER [{t.Schema}].[trig_df_ChangeTracking_{t.Name}]
						END

						IF OBJECT_ID (N'[{t.Schema}].[{t.Name}{SnapshotTableSuffix}]', N'U') IS NOT NULL
						BEGIN
							DROP TABLE [{t.Schema}].[{t.Name}{SnapshotTableSuffix}];
						END
					";

					ExecuteNonQuery(sql, connection);
				}

				ExecuteNonQuery(@"
					IF OBJECT_ID (N'[dbo].[df_ChangeTracking]', N'U') IS NOT NULL
						DROP TABLE [dbo].[df_ChangeTracking]
				", connection);
			}
		}

		public void RemoveDataFreshFromDatabase()
		{
			var before = DateTime.Now;
			var cb = new SqlConnectionStringBuilder(connectionString);
			ConsoleWrite($"RemoveDataFreshFromDatabase for {cb.InitialCatalog} started");
			RemoveDataFresh();
			ConsoleWrite($"RemoveDataFreshFromDatabase for {cb.InitialCatalog} complete: " + (DateTime.Now - before));
		}

		public void RefreshTheDatabase()
		{
			var before = DateTime.Now;
			var cb = new SqlConnectionStringBuilder(connectionString);
			ConsoleWrite($"RefreshTheDatabase for {cb.InitialCatalog} started");
			GuardDatabaseIsPrepared();

			var changedAndReferencedTables = GetChangedAndReferencedUserTables();
			var changedTables = GetChangedUserTables();

			using (var conn = new SqlConnection(connectionString))
			{
				conn.Open();

				ExecuteNonQuery("TRUNCATE TABLE df_ChangeTracking", conn);

				foreach (var table in changedAndReferencedTables)
					ExecuteNonQuery($"ALTER TABLE [{table.Schema}].[{table.Name}] NOCHECK CONSTRAINT ALL", conn);

				foreach (var t in changedTables)
				{
					var sql = $@"
						DELETE [{t.Schema}].[{t.Name}];
						DELETE FROM df_ChangeTracking WHERE TableName='{t.Name}' and TableSchema='{t.Schema}';

						IF (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
							WHERE table_schema = '{t.Schema}'
								AND table_name = '{t.Name}' 
								AND IDENT_SEED(TABLE_NAME) IS NOT NULL) > 0 
						BEGIN
								DBCC CHECKIDENT([{t.Schema}.{t.Name}], RESEED, 0)
						END";
					ExecuteNonQuery(sql, conn);
				}

				PerformBulkRestore(changedTables, cb);

				foreach (var table in changedAndReferencedTables)
					ExecuteNonQuery($"ALTER TABLE [{table.Schema}].[{table.Name}] CHECK CONSTRAINT ALL", conn);
			}

			ConsoleWrite($"RefreshTheDatabase for {cb.InitialCatalog} complete: " + (DateTime.Now - before));
		}

		public void RefreshTheEntireDatabase()
		{
			var cb = new SqlConnectionStringBuilder(connectionString);
			var before = DateTime.Now;
			ConsoleWrite($"RefreshTheEntireDatabase for ({cb.InitialCatalog}) Started");
			GuardDatabaseIsPrepared();

			var allTables = GetAllUserTables();
			var changedTables = GetChangedUserTables();

			using (var conn = new SqlConnection(connectionString))
			{
				conn.Open();

				foreach (var table in allTables)
					ExecuteNonQuery($"ALTER TABLE [{table.Schema}].[{table.Name}] NOCHECK CONSTRAINT ALL", conn);

				foreach (var table in changedTables)
					ExecuteNonQuery($"DELETE [{table.Schema}].[{table.Name}]", conn);

				PerformBulkRestore(changedTables, cb);

				foreach (var table in allTables)
					ExecuteNonQuery($"ALTER TABLE [{table.Schema}].[{table.Name}] CHECK CONSTRAINT ALL", conn);
			}

			ConsoleWrite($"RefreshTheEntireDatabase for ({cb.InitialCatalog}) complete: " + (DateTime.Now - before));
		}

		public bool HasDatabaseBeenModified()
		{
			GuardDatabaseIsPrepared();
			var sql =
				$@"SELECT COUNT(*) FROM {ChangeTrackingTableName}
				WHERE TableName <> 'ChangeTrackingTableName' 
				AND TableName NOT LIKE '%{SnapshotTableSuffix}'";
			var ret = Convert.ToInt32(ExecuteScalar(sql));
			return ret > 0;
		}

		void PerformBulkBackup(TableMetadata[] tables, SqlConnectionStringBuilder cb)
		{
			PerformBulkOperation(tables, cb, backup: true);
		}

		void PerformBulkRestore(TableMetadata[] tables, SqlConnectionStringBuilder cb)
		{
			PerformBulkOperation(tables, cb, backup: false);
		}

		void PerformBulkOperation(TableMetadata[] tables, SqlConnectionStringBuilder cb, bool backup)
		{
			var operation = backup ? "restore" : "backup";
			ConsoleWrite($"Bulk {operation} started for {cb.InitialCatalog}");

			var before = DateTime.Now;

			var destinationSuffix = backup ? SnapshotTableSuffix : string.Empty;
			var sourceSuffix = backup ? string.Empty : SnapshotTableSuffix;

			if (backup)
			{
				foreach (var table in tables)
				{
					var sourceTable = $"[{table.Schema}].[{table.Name}{sourceSuffix}]";
					var destinationTable = $"[{table.Schema}].[{table.Name}{destinationSuffix}]";

					var sql =
						$"IF OBJECT_ID (N'{destinationTable}', N'U') IS NULL " +
						$"BEGIN SELECT * INTO {destinationTable} FROM {sourceTable} WHERE 1=2 END " +
						"ELSE " +
						$"BEGIN TRUNCATE TABLE {destinationTable} END";
					ExecuteNonQuery(sql);
				}
			}

			foreach (var table in tables)
			{
				var sourceTable = $"[{table.Schema}].[{table.Name}{sourceSuffix}]";
				var destinationTable = $"[{table.Schema}].[{table.Name}{destinationSuffix}]";

				CopyTable(cb.ConnectionString, sourceTable, destinationTable);
			}

			ConsoleWrite($"Bulk {operation} complete for {cb.InitialCatalog}: {DateTime.Now - before}");
		}

		static void CopyTable(string dbConnectionString, string sourceTable, string destinationTable)
		{
			using (var sourceConnection = new SqlConnection(dbConnectionString))
			{
				sourceConnection.Open();
				var commandSourceData = new SqlCommand($"SELECT * FROM {sourceTable};", sourceConnection);
				var reader = commandSourceData.ExecuteReader();

				using (var destinationConnection = new SqlConnection(dbConnectionString))
				{
					destinationConnection.Open();

					using (var bulkCopy = new SqlBulkCopy(destinationConnection))
					{
						bulkCopy.DestinationTableName = destinationTable;
						try
						{
							bulkCopy.WriteToServer(reader);
						}
						catch (Exception ex)
						{
							Console.WriteLine(ex.Message);
						}
						finally
						{
							reader.Close();
						}
					}
				}
			}
		}

		object ExecuteScalar(string sql)
		{
			using (var conn = new SqlConnection(connectionString))
			{
				sql += " --dataProfilerIgnore";
				var cmd = new SqlCommand(sql, conn) { CommandTimeout = 1200 };
				conn.Open();
				return cmd.ExecuteScalar();
			}
		}

		void ExecuteNonQuery(string sql)
		{
			using (var conn = new SqlConnection(connectionString))
			{
				conn.Open();
				ExecuteNonQuery(sql, conn);
			}
		}

		static void ExecuteNonQuery(string sql, SqlConnection conn)
		{
			sql += " --dataProfilerIgnore";
			using (var cmd = new SqlCommand(sql, conn) { CommandTimeout = 1200 })
				cmd.ExecuteNonQuery();
		}

		void ConsoleWrite(string message)
		{
			if (verbose)
				Console.Out.WriteLine(message);
		}

		static TableMetadata[] SelectTables(string sql, string connectionString)
		{
			var tables = new List<TableMetadata>();

			using (var conn = new SqlConnection(connectionString))
			{
				conn.Open();
				using (var cmd = new SqlCommand(sql, conn))
				using (var reader = cmd.ExecuteReader())
				{
					while (reader.Read())
					{
						tables.Add(new TableMetadata
						{
							Schema = reader.GetString(0),
							Name = reader.GetString(1)
						});
					}
				}
			}
			return tables.ToArray();
		}

		void GuardDatabaseIsPrepared()
		{
			if (!TableExists(ChangeTrackingTableName))
				throw new SqlDataFreshException(
					$"DataFresh table ({ChangeTrackingTableName}) not found. Please prepare the database.");
		}

		TableMetadata[] GetAllUserTables()
		{
			var sql =
				$@"SELECT table_schema, table_name 
				FROM Information_Schema.tables 
				WHERE table_type = 'BASE TABLE' 
					AND table_name NOT LIKE '%{SnapshotTableSuffix}'";

			var tables = SelectTables(sql, connectionString);
			return tables;
		}

		TableMetadata[] GetChangedUserTables()
		{
			var sql =
				$@"SELECT DISTINCT TableSchema, TableName 
				FROM df_ChangeTracking 
				WHERE TableName NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
				AND TableName NOT LIKE '%{SnapshotTableSuffix}'";

			var tables = SelectTables(sql, connectionString);
			return tables;
		}

		TableMetadata[] GetChangedAndReferencedUserTables()
		{
			var sql = $@"
				SELECT DISTINCT x.TableSchema, x.TableName
				FROM (
					SELECT DISTINCT TableSchema, TableName 
					FROM df_ChangeTracking 
					UNION
					SELECT DISTINCT
						OBJECT_SCHEMA_NAME(fkeyid) AS TableSchema,
						OBJECT_NAME(fkeyid) AS TableName
					FROM sysreferences sr 
					INNER JOIN df_ChangeTracking ct ON sr.rkeyid = OBJECT_ID(ct.TableName)
				) x
				WHERE x.TableName NOT IN ('df_ChangeTracking', 'dr_DeltaVersion')
					AND x.TableName NOT LIKE '%{SnapshotTableSuffix}'
			";

			var tables = SelectTables(sql, connectionString);
			return tables;
		}

		public bool TableExists(string tableName)
		{
			var tableCount = int.Parse(ExecuteScalar(
				$"SELECT COUNT(TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tableName}'").ToString());
			return (tableCount > 0);
		}
	}
}