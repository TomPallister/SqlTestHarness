using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.Text;
using Fourth.Sql.Test.Harness.Configuration;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;

namespace Fourth.Sql.Test.Harness.DatabaseSetup
{
    public class TempDb
    {
        private readonly Database _sourceDatabase;
        private readonly Server _sourceServer;

        private readonly Database _targetDatabase;
        private readonly Server _targetServer;
        private readonly DbElement _targetSettings;

        public TempDb()
        {
            Logger.WriteLine("Getting configuration details");
            var settings = (TempDbSettingsSection) ConfigurationManager.GetSection("TempDbSettings");

            Logger.WriteLine(" - Getting source settings");
            DbElement sourceSettings = settings.SourceDb;

            Logger.WriteLine(" - Getting target settings");
            _targetSettings = settings.TargetDb;

            Logger.WriteLine("Creating source connection");
            var sourceConnection = new ServerConnection(sourceSettings.ServerInstance, sourceSettings.UserName,
                sourceSettings.Password);
            Logger.WriteLine(" - {0}", sourceConnection.SqlConnectionObject.ConnectionString);

            Logger.WriteLine("Creating source server");
            _sourceServer = new Server(sourceConnection);
            _sourceDatabase = _sourceServer.Databases[sourceSettings.DatabaseName];

            Logger.WriteLine("Creating target connection");
            var targetConnection = new ServerConnection(_targetSettings.ServerInstance, _targetSettings.UserName,
                _targetSettings.Password);
            Logger.WriteLine(" - {0}", targetConnection.SqlConnectionObject.ConnectionString);

            Logger.WriteLine("Creating target server");
            _targetServer = new Server(targetConnection);
            _targetDatabase = new Database(_targetServer, _targetSettings.DatabaseName);

            Tables = new List<string>();
        }

        private IList<string> Tables { get; set; }

        public void Delete()
        {
            if (_targetServer.Databases.Contains(_targetSettings.DatabaseName))
            {
                string sql = string.Format(
                    CultureInfo.InvariantCulture,
                    "ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                    _targetSettings.DatabaseName);

                Logger.WriteLine("Placing {0} into single user mode", _targetSettings.DatabaseName);
                _targetDatabase.ExecuteNonQuery(sql);

                Logger.WriteLine("Killing Database {0}", _targetSettings.DatabaseName);
                //_targetServer.DetachDatabase(_targetSettings.DatabaseName, true);
                _targetServer.KillDatabase(_targetSettings.DatabaseName);
            }
        }

        public void Create()
        {
            Delete();
            _targetDatabase.Collation = "Latin1_General_CI_AI";
            var fileGroup = new FileGroup(_targetDatabase, "PRIMARY");
            _targetDatabase.FileGroups.Add(fileGroup);


            var dataFile = new DataFile(fileGroup, _targetSettings.DatabaseName, _targetSettings.DataFilePath)
            {
                Growth = 10,
                GrowthType = FileGrowthType.Percent
            };

            fileGroup.Files.Add(dataFile);

            var logFile = new LogFile(_targetDatabase, string.Format("{0}_log", _targetSettings.DatabaseName),
                _targetSettings.LogFilePath)
            {
                Growth = 10,
                GrowthType = FileGrowthType.Percent
            };

            _targetDatabase.LogFiles.Add(logFile);
            _targetDatabase.Collation = _sourceDatabase.Collation;

            Logger.WriteLine("Creating target database {0}", _targetSettings.DatabaseName);
            _targetDatabase.DatabaseOptions.Trustworthy = true;
            _targetDatabase.Create();
        }

        public void CopyStoredProcedure(string storedProcName)
        {
            Logger.WriteLine("Copying stored procedure {0}", storedProcName);

            SqlSmoObject sourceProcedure = _sourceDatabase.StoredProcedures[storedProcName];
            CopySqlSmoObject(sourceProcedure);
        }

        public void CopyView(string viewName)
        {
            Logger.WriteLine("Copying view {0}", viewName);

            SqlSmoObject sourceView = _sourceDatabase.Views[viewName];
            CopySqlSmoObject(sourceView);
        }

        public void CopyTable(string tableName)
        {
            Logger.WriteLine("Copying table {0}", tableName);

            SqlSmoObject table = _sourceDatabase.Tables[tableName];
            CopySqlSmoObject(table);
        }

        public void CopyFunction(string functionName)
        {
            Logger.WriteLine("Copying function {0}", functionName);

            SqlSmoObject function = _sourceDatabase.UserDefinedFunctions[functionName];
            CopySqlSmoObject(function);
        }

        public void DeleteStoredProcedure(string storedProcName)
        {
            Logger.WriteLine("Deleting stored procedure {0}", storedProcName);

            SqlSmoObject sourceProcedure = _sourceDatabase.StoredProcedures[storedProcName];
            DeleteSqlSmoObject(sourceProcedure);
        }

        public void DeleteView(string viewName)
        {
            Logger.WriteLine("Deleting view {0}", viewName);

            SqlSmoObject sourceView = _sourceDatabase.Views[viewName];
            DeleteSqlSmoObject(sourceView);
        }

        public void DeleteTable(string tableName)
        {
            Logger.WriteLine("Deleting table {0}", tableName);

            SqlSmoObject sourceTable = _sourceDatabase.Tables[tableName];
            DeleteSqlSmoObject(sourceTable);
        }

        public void DeleteFunction(string functionName)
        {
            Logger.WriteLine("Deleting function {0}", functionName);

            SqlSmoObject function = _sourceDatabase.UserDefinedFunctions[functionName];
            DeleteSqlSmoObject(function);
        }

        private void CopySqlSmoObject(SqlSmoObject smoObject)
        {
            try
            {
                IEnumerable<DependencyCollectionNode> dependencies = GetDependencies(smoObject, Location.Source);

                ScriptDependencies(dependencies);
            }
            catch
            {
                Logger.WriteLine("Failed to copy {0}", smoObject.Urn);
            }
        }

        private void DeleteSqlSmoObject(SqlSmoObject smoObject)
        {
            try
            {
                IEnumerable<DependencyCollectionNode> dependencies = GetDependencies(smoObject, Location.Target);
                DeleteDependencies(dependencies);
            }
            catch
            {
                Logger.WriteLine("Failed to delete {0}", smoObject.Urn);
            }
        }

        private IEnumerable<DependencyCollectionNode> GetDependencies(SqlSmoObject smoObject, Location location)
        {
            Logger.WriteLine("Gathering dependencies for {0}", smoObject.Urn);

            try
            {
                var dependencyWalker = new DependencyWalker(location == Location.Source ? _sourceServer : _targetServer);

                DependencyTree parents = dependencyWalker.DiscoverDependencies(new[] {smoObject},
                    DependencyType.Parents);
                DependencyCollection dependencies = dependencyWalker.WalkDependencies(parents);
                return dependencies;
            }
            catch
            {
                Logger.WriteLine("Failed to get dependencies for {0}", smoObject.Urn);
                throw;
            }
        }

        private void ScriptDependencies(IEnumerable<DependencyCollectionNode> dependencies)
        {
            Logger.WriteLine("Scripting dependencies:");

            var tableOptions = new ScriptingOptions
            {
                WithDependencies = false,
                ScriptSchema = true,
                Indexes = true,
                IncludeIfNotExists = true,
                NoAssemblies = true
            };

            var procFuncOptions = new ScriptingOptions
            {
                WithDependencies = true,
                ScriptSchema = true,
                IncludeIfNotExists = true,
                NoAssemblies = true
            };

            var assemblyOptions = new ScriptingOptions
            {
                WithDependencies = true,
                ScriptSchema = true,
                IncludeIfNotExists = true,
                NoAssemblies = false
            };

            var script = new Scripter(_sourceServer);

            foreach (DependencyCollectionNode node in dependencies)
            {
                Urn urn = node.Urn;

                if (node.Urn.Type == "UnresolvedEntity")
                {
                    continue;
                }

                Logger.WriteLine(" - Generating scripts from {0}", urn);

                switch (urn.Type)
                {
                    case "Table":
                        script.Options = tableOptions;
                        break;
                    case "SqlAssembly":
                        script.Options = assemblyOptions;
                        break;
                    default:
                        script.Options = procFuncOptions;
                        break;
                }

                StringCollection scripts = script.Script(new[] {urn});

                foreach (string scr in scripts)
                {
                    Logger.WriteLine(" - Executing script on {0}: {1}", _targetDatabase.Urn, scr);
                    _targetDatabase.ExecuteNonQuery(scr);
                }

                if (SqlSmoObject.GetTypeFromUrnSkeleton(urn) == typeof (Table))
                {
                    if (Tables.Contains(urn.ToString()))
                    {
                        Logger.WriteLine(" - Table already exists so skipping data copy");
                    }
                    else
                    {
                        CopyDependentData((Table) _sourceServer.GetSmoObject(urn));
                    }
                }
            }
        }

        private void DeleteDependencies(IEnumerable<DependencyCollectionNode> dependencies)
        {
            Logger.WriteLine("Deleting dependencies:");

            var script = new Scripter(_sourceServer)
            {
                Options = new ScriptingOptions
                {
                    WithDependencies = true,
                    ScriptData = false,
                    ScriptDrops = true,
                    ScriptSchema = true,
                    IncludeIfNotExists = true
                }
            };

            foreach (DependencyCollectionNode node in dependencies)
            {
                Urn urn = node.Urn;

                Logger.WriteLine(" - Generating scripts from {0}", urn);
                StringCollection scripts = script.Script(new[] {urn});

                foreach (string scr in scripts)
                {
                    Logger.WriteLine(" - Executing script on {0}: {1}", _targetDatabase.Urn, scr);
                    _targetDatabase.ExecuteNonQuery(scr);
                }
            }
        }

        private void ClearTable(Table table)
        {
            Logger.WriteLine("Clearing table {0}", table.Urn);

            var script = new Scripter(_sourceServer)
            {
                Options = new ScriptingOptions
                {
                    ScriptData = true,
                    ScriptDrops = true,
                    ScriptSchema = false
                }
            };

            Logger.WriteLine(" - Generating scripts");
            IEnumerable<string> scripts = script.EnumScript(new SqlSmoObject[] {table});

            foreach (string scr in scripts)
            {
                Logger.WriteLine(" - Executing script on {0}: {1}", _targetDatabase.Urn, scr);
                _targetDatabase.ExecuteNonQuery(scr);
            }
        }

        private void CopyDependentData(Table table)
        {
            ClearTable(table);
            int timeout = 900;
            using (SqlConnection sourceConnection = _sourceServer.ConnectionContext.SqlConnectionObject)
            {
                sourceConnection.Open();

                string sourceTableName = string.Format("{0}.{1}.{2}", _sourceDatabase.Name, table.Schema, table.Name);
                string targetTableName = string.Format("{0}.{1}.{2}", _targetDatabase.Name, table.Schema, table.Name);

                Logger.WriteLine("Copying data from {0} to {1}", sourceTableName, targetTableName);

                string sqlSelect = string.Format("SELECT top 1000 * FROM {0}", sourceTableName);
                var cmdSelect = new SqlCommand(sqlSelect, sourceConnection);
                cmdSelect.CommandTimeout = timeout;

                Logger.WriteLine(" - Reading data from {0}", sourceTableName);

                SqlDataReader reader = cmdSelect.ExecuteReader();

                using (SqlConnection targetConnection = _targetServer.ConnectionContext.SqlConnectionObject)
                {
                    targetConnection.Open();

                    using (
                        var bulkCopy = new SqlBulkCopy(targetConnection,
                            SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock, null))
                    {
                        bulkCopy.DestinationTableName = targetTableName;
                        bulkCopy.BulkCopyTimeout = timeout;
                        bulkCopy.EnableStreaming = true;

                        GetColumnMappings(targetConnection, bulkCopy, _targetDatabase.Name, table.Schema, table.Name);

                        try
                        {
                            Logger.WriteLine(" - Writing data to {0}", targetTableName);
                            bulkCopy.WriteToServer(reader);

                            Tables.Add(table.Urn.ToString());
                        }
                        finally
                        {
                            reader.Close();
                        }
                    }
                }
            }
        }

        private void GetColumnMappings(SqlConnection connection, SqlBulkCopy bulkCopy, string databaseName,
            string schema, string tableName)
        {
            Logger.WriteLine("Obtaining column mappings for {0}.{1}.{2}", databaseName, schema, tableName);

            var query = new StringBuilder();
            query.AppendFormat("USE {0} ", databaseName);
            query.Append("SELECT COLUMN_NAME, ");
            query.AppendFormat("COLUMNPROPERTY(OBJECT_ID('{0}.{1}'), COLUMN_NAME, 'IsComputed') AS IsComputed ", schema,
                tableName);
            query.Append("FROM INFORMATION_SCHEMA.COLUMNS ");
            query.AppendFormat("WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'", schema, tableName);

            Logger.WriteLine(" - Reading columns");

            var cmd = new SqlCommand(query.ToString(), connection);
            SqlDataReader drcolumns = cmd.ExecuteReader();

            while (drcolumns.Read())
            {
                if (drcolumns.GetInt32(1) != 1)
                {
                    string columnName = drcolumns.GetString(0);

                    Logger.WriteLine(" - Adding column mapping for {0}", columnName);

                    bulkCopy.ColumnMappings.Add(columnName, columnName);
                }
            }

            drcolumns.Close();
        }

        public void EnableClr(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                string sql =
                    @"exec sp_configure 'show advanced options', 1; RECONFIGURE; exec sp_configure 'clr enabled', 1; RECONFIGURE; ";

                using (var command = new SqlCommand(sql, connection))
                {
                    connection.Open();

                    command.ExecuteNonQuery();
                }
            }
        }

        private enum Location
        {
            Source,
            Target
        };
    }
}