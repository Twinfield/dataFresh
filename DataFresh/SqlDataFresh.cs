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
using System.IO;
using System.Linq;
using System.Text;

namespace DataFresh
{
	/// <summary>
	/// dataFresh by Entropy Zero Consulting is a library that will enable
	/// the test driven developer to build a test harness that will refresh
	/// the database to a known state between tests
	/// </summary>
	public class SqlDataFresh : IDataFresh
	{
		#region Member Variables

		readonly string connectionString;
		string snapshotRootPath;

		const string PrepareScriptResourceName = "DataFresh.Resources.PrepareDataFresh.sql";
		const string RemoveScriptResourceName = "DataFresh.Resources.RemoveDataFresh.sql";

		public string PrepareProcedureName = "df_ChangeTrackingTriggerCreate";
		public string ChangeTrackingTableName = "df_ChangeTracking";

		readonly bool verbose;

		#endregion

		#region Public Methods

		public SqlDataFresh(string connectionString)
		{
			this.connectionString = connectionString;
		}

		public SqlDataFresh(string connectionString, bool verbose)
		{
			this.connectionString = connectionString;
			this.verbose = verbose;
		}

		public bool TableExists(string tableName)
		{
			var tableCount = int.Parse(ExecuteScalar(
				$"SELECT COUNT(TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tableName}'").ToString());
			return (tableCount > 0);
		}

		public bool ProcedureExists(string procedureName)
		{
			var procedureCount = int.Parse(ExecuteScalar(
				$"SELECT COUNT(ROUTINE_NAME) FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME='{procedureName}'").ToString());
			return (procedureCount > 0);
		}

		#endregion

		#region IDataFresh Members

		/// <summary>
		/// prepare the database to use the dataFresh library
		/// </summary>
		public void PrepareDatabaseforDataFresh()
		{
			PrepareDatabaseforDataFresh(true);
		}

		public void PrepareDatabaseforDataFresh(bool createSnapshot)
		{
			var before = DateTime.Now;
			ConsoleWrite("PrepareDatabaseforDataFresh Started");
			RunSqlScript(ResourceManagement.GetDecryptedResourceStream(PrepareScriptResourceName));

			ExecuteNonQuery("exec " + PrepareProcedureName);

			if (createSnapshot)
				CreateSnapshot();

			ConsoleWrite("PrepareDatabaseforDataFresh Complete : " + (DateTime.Now - before));
		}

		/// <summary>
		/// remove the dataFresh objects from a database
		/// </summary>
		public void RemoveDataFreshFromDatabase()
		{
			var before = DateTime.Now;
			ConsoleWrite("RemoveDataFreshFromDatabase Started");
			RunSqlScript(ResourceManagement.GetDecryptedResourceStream(RemoveScriptResourceName));
			ConsoleWrite("RemoveDataFreshFromDatabase Complete : " + (DateTime.Now - before));
		}

		/// <summary>
		/// refresh the database to a known state
		/// </summary>
		public void RefreshTheDatabase()
		{
			var before = DateTime.Now;
			ConsoleWrite("RefreshTheDatabase Started");
			if (!ProcedureExists(PrepareProcedureName))
				throw new SqlDataFreshException($"DataFresh procedure ({PrepareProcedureName}) not found. Please prepare the database.");

			const string changedAndReferencedTablesSql = @"
SELECT DB_NAME(), [tableschema], [tablename] from df_ChangeTracking
UNION
SELECT DISTINCT
		DB_NAME(),
      OBJECT_SCHEMA_NAME(fkeyid) AS Referenced_Table_Schema,
      OBJECT_NAME(fkeyid) AS Referenced_Table_Name
FROM 
   sysreferences sr
   INNER JOIN df_ChangeTracking ct ON sr.rkeyid = OBJECT_ID(ct.tablename)
";
			const string changedTablesSql = "SELECT DB_NAME(), [tableschema], [tablename] FROM df_ChangeTracking WHERE tablename not in('df_ChangeTracking', 'dr_DeltaVersion')";
			var commandBuilder = new StringBuilder();
			var snapshotPath = GetSnapshotPath();
			var connectionBuilder = new SqlConnectionStringBuilder(connectionString);
			using (var conn = new SqlConnection(connectionString))
			{
				conn.Open();
				var changedAndReferencedTables = SelectTables(changedAndReferencedTablesSql, conn);
				var changedTables = SelectTables(changedTablesSql, conn);
				ExecuteNonQuery("TRUNCATE TABLE df_ChangeTracking", conn);

				foreach (var table in changedAndReferencedTables)
					ExecuteNonQuery($"Alter Table [{table.Schema}].[{table.Name}] NOCHECK CONSTRAINT ALL", conn);

				const int batchSize = 20;
				for (var i = 0; i < changedTables.Count; i += batchSize)
				{
					var batch = changedTables.Skip(i).Take(batchSize);
					foreach (var t in batch)
					{
						commandBuilder.Append($"bcp \"{t.Database}.[{t.Schema}].[{t.Name}]\"" +
							$" in \"{snapshotPath}{t.Schema}.{t.Name}.df\"" +
							$" -n -k -E -C 1252 -S {connectionBuilder.DataSource} -T &&");

						var sql = $"DELETE [{t.Schema}].[{t.Name}]; DELETE FROM df_ChangeTracking WHERE TableName='{t.Name}' and TableSchema='{t.Schema}'";
						ExecuteNonQuery(sql, conn);

						sql = $"IF (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{t.Schema}' AND table_name = '{t.Name}' AND IDENT_SEED(TABLE_NAME) IS NOT NULL) > 0 " +
							$"BEGIN DBCC CHECKIDENT([{t.Schema}.{t.Name}], RESEED, 0) END";
						ExecuteNonQuery(sql, conn);
					}

					commandBuilder.Append("REM");
					ExecuteCmd(commandBuilder.ToString());
					commandBuilder.Clear();
				}

				foreach (var table in changedAndReferencedTables)
					ExecuteNonQuery($"Alter Table [{table.Schema}].[{table.Name}] CHECK CONSTRAINT ALL", conn);
			}

			ConsoleWrite("RefreshTheDatabase Complete : " + (DateTime.Now - before));
		}

		static List<TableMetadata> SelectTables(string sql, SqlConnection conn)
		{
			var tables = new List<TableMetadata>();
			using (var cmd = new SqlCommand(sql, conn))
			using (var reader = cmd.ExecuteReader())
			{
				while (reader.Read())
				{
					tables.Add(new TableMetadata
					{
						Database = reader.GetString(0),
						Schema = reader.GetString(1),
						Name = reader.GetString(2)
					});
				}
			}
			return tables;
		}

		/// <summary>
		/// refresh the database ignoring the dataFresh change tracking table.
		/// </summary>
		public void RefreshTheEntireDatabase()
		{
			var before = DateTime.Now;
			ConsoleWrite("RefreshTheEntireDatabase Started");
			if (!ProcedureExists(PrepareProcedureName))
				throw new SqlDataFreshException($"DataFresh procedure ({PrepareProcedureName}) not found. Please prepare the database.");

			const string allTablesSql = @"SELECT DB_NAME(), Table_Schema as TableSchema, Table_Name as TableName FROM Information_Schema.tables WHERE table_type = 'BASE TABLE'";
			const string changedTablesSql = "SELECT DB_NAME(), [tableschema], [tablename] FROM df_ChangeTracking WHERE tablename not in('df_ChangeTracking', 'dr_DeltaVersion')";
			var commandBuilder = new StringBuilder();
			var snapshotPath = GetSnapshotPath();
			var connectionBuilder = new SqlConnectionStringBuilder(connectionString);
			using (var conn = new SqlConnection(connectionString))
			{
				conn.Open();
				var allTables = SelectTables(allTablesSql, conn);
				var changedTables = SelectTables(changedTablesSql, conn);

				foreach (var table in allTables)
					ExecuteNonQuery($"Alter Table [{table.Schema}].[{table.Name}] NOCHECK CONSTRAINT ALL", conn);

				const int batchSize = 20;
				for (var i = 0; i < changedTables.Count; i += batchSize)
				{
					var batch = changedTables.Skip(i).Take(batchSize);
					foreach (var t in batch)
					{
						commandBuilder.Append($"bcp \"{t.Database}.[{t.Schema}].[{t.Name}]\"" +
							$" in \"{snapshotPath}{t.Schema}.{t.Name}.df\"" +
							$" -n -k -E -C 1252 -S {connectionBuilder.DataSource} -T &&");
						ExecuteNonQuery($"DELETE [{t.Schema}].[{t.Name}]", conn);
					}

					commandBuilder.Append("REM");
					ExecuteCmd(commandBuilder.ToString());
					commandBuilder.Clear();
				}

				foreach (var table in allTables)
					ExecuteNonQuery($"Alter Table [{table.Schema}].[{table.Name}] CHECK CONSTRAINT ALL", conn);
			}

			ConsoleWrite("RefreshTheEntireDatabase Complete : " + (DateTime.Now - before));
		}

		static void ExecuteCmd(string command)
		{
			var process = new System.Diagnostics.Process();
			var startInfo = new System.Diagnostics.ProcessStartInfo
			{
				WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden,
				FileName = "cmd.exe",
				Arguments = "/C " + command
			};
			process.StartInfo = startInfo;
			process.Start();
			process.WaitForExit();
		}

		sealed class TableMetadata
		{
			public string Database { get; set; }
			public string Schema { get; set; }
			public string Name { get; set; }
		}

		/// <summary>
		/// create snapshot of database
		/// </summary>
		public void CreateSnapshot()
		{
			var before = DateTime.Now;
			ConsoleWrite("CreateSnapshot Started");
			if (!ProcedureExists(PrepareProcedureName))
				throw new SqlDataFreshException($"DataFresh procedure ({PrepareProcedureName}) not found. Please prepare the database.");

			var sql = "SELECT DB_NAME(), TABLE_SCHEMA, TABLE_NAME FROM Information_Schema.tables WHERE table_type = 'BASE TABLE'";
			var tables = new List<TableMetadata>();
			using (var conn = new SqlConnection(connectionString))
			{
				sql += " --dataProfilerIgnore";
				var cmd = new SqlCommand(sql, conn) { CommandTimeout = 1200 };
				conn.Open();
				using (var reader = cmd.ExecuteReader())
				{
					while (reader.Read())
					{
						tables.Add(new TableMetadata
						{
							Database = reader.GetString(0),
							Schema = reader.GetString(1),
							Name = reader.GetString(2)
						});
					}
				}
			}

			var snapshotPath = GetSnapshotPath();
			Directory.CreateDirectory(snapshotPath);
			var connectionBuilder = new SqlConnectionStringBuilder(connectionString);
			var commandBuilder = new StringBuilder();

			const int batchSize = 20;
			for (var i = 0; i < tables.Count; i += batchSize)
			{
				var batch = tables.Skip(i).Take(batchSize);
				foreach (var t in batch)
				{
					commandBuilder.Append($"bcp \"{t.Database}.[{t.Schema}].[{t.Name}]\"" +
						$" out \"{snapshotPath}{t.Schema}.{t.Name}.df\"" +
						$" -n -k -E -C 1252 -S {connectionBuilder.DataSource} -T &&");
				}
				commandBuilder.Append("REM");
				ExecuteCmd(commandBuilder.ToString());
				commandBuilder.Clear();
			}

			ConsoleWrite("CreateSnapshot Complete : " + (DateTime.Now - before));
		}

		/// <summary>
		/// determine if the database has been modified
		/// </summary>
		/// <returns>true if modified.</returns>
		public bool HasDatabaseBeenModified()
		{
			if (!TableExists(ChangeTrackingTableName))
			{
				throw new SqlDataFreshException($"DataFresh table ({ChangeTrackingTableName}) not found. Please prepare the database.");
			}

			var sql = string.Format(@"SELECT COUNT(*) FROM {0} WHERE TableName <> '{0}'", ChangeTrackingTableName);
			var ret = Convert.ToInt32(ExecuteScalar(sql));
			return ret > 0;
		}

		/// <summary>
		/// location on the server where the snapshot files are located
		/// </summary>
		public string SnapshotRootPath
		{
			get => snapshotRootPath;
			set => snapshotRootPath = CheckForIllegalCharsAndAppendTrailingSlash(value);
		}

		#endregion

		#region Private Methods

		static string CheckForIllegalCharsAndAppendTrailingSlash(string value)
		{
			if (value == null)
				return null;
			var path = value;
			path = path.Replace("\"", "");
			if (!path.EndsWith(@"\"))
				path += @"\";
			return path;
		}

		object ExecuteScalar(string sql)
		{
			using (var conn = new SqlConnection(connectionString))
			{
				sql += " --dataProfilerIgnore";
				var cmd = new SqlCommand(sql, conn) {CommandTimeout = 1200};
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

		string GetCurrentDatabaseName()
		{
			return (string)ExecuteScalar("SELECT DB_Name()");
		}

		string GetSnapshotPath()
		{
			if (string.IsNullOrEmpty(snapshotRootPath))
				throw new InvalidOperationException("Snapshot root path is not defined.");

			var dbName = GetCurrentDatabaseName();
			return Path.Combine(snapshotRootPath, $"Snapshot_{dbName}\\");
		}

		void RunSqlScript(TextReader reader)
		{
			string line;
			var cmd = new StringBuilder();
			while ((line = reader.ReadLine()) != null)
			{
				if (line.Trim().ToLower().Equals("go"))
				{
					ExecuteNonQuery(cmd.ToString());
					cmd.Length = 0;
				}
				else
				{
					cmd.Append(line);
					cmd.Append(Environment.NewLine);
				}
			}

			if (cmd.ToString().Trim().Length > 0)
				ExecuteNonQuery(cmd.ToString());
		}

		void ConsoleWrite(string message)
		{
			if (this.verbose)
				Console.Out.WriteLine(message);
		}

		#endregion
	}
}