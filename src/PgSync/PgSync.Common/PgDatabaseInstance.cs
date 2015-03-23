using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

using Npgsql;

namespace PgSync.Common
{
    public class PgDatabaseInstance : BaseDatabaseInstance
    {
        private readonly DatabaseDefinition _databaseDefinition;

        /// <summary>
        /// The data-cache instance
        /// </summary>
        private readonly IDataCache _dataCache;

        /// <summary>
        /// Gets or sets a logger
        /// </summary>
        public ILogger Logger { get; set; } 

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="databaseDefinition"></param>

        public PgDatabaseInstance(DatabaseDefinition databaseDefinition, IDataCache dataCache = null)
        {
            _databaseDefinition = databaseDefinition;
            _dataCache = dataCache ?? new SimpleFileCache();
        }

        private LocalDataStoreSlot _isolatedDataStoreSlot;

        /// <summary>
        /// Provides improved connection flow by providing thread isolated connections for the
        /// lifetime of the disposable.
        /// </summary>
        /// <returns></returns>
        public override IDisposable IsolatedConnections()
        {
            var connection = CreateConnection();

            _isolatedDataStoreSlot = Thread.AllocateDataSlot();
            Thread.SetData(_isolatedDataStoreSlot, connection);

            return new TrackedDisposable(() =>
            {
                connection.Close();
                connection.Dispose();

                _isolatedDataStoreSlot = null;
            });
        }

        /// <summary>
        /// Creates a connection to a database.
        /// </summary>
        /// <param name="databaseDefinition"></param>
        /// <returns></returns>
        public IDbConnection CreateConnection()
        {
            if (_isolatedDataStoreSlot != null)
            {
                var isoconnection = (NpgsqlConnection)Thread.GetData(_isolatedDataStoreSlot);
                if (isoconnection != null)
                {
                    return new ConnectionWrapper(isoconnection);
                }
            }

            var databaseDefinition = _databaseDefinition;
            var builder = new NpgsqlConnectionStringBuilder();

            if (!string.IsNullOrWhiteSpace(databaseDefinition.DatabaseHost))
            {
                builder.Host = databaseDefinition.DatabaseHost;
            }

            if (!string.IsNullOrWhiteSpace(databaseDefinition.DatabaseName))
            {
                builder.Database = databaseDefinition.DatabaseName;
            }

            if (string.IsNullOrWhiteSpace(databaseDefinition.DatabaseUser) || string.IsNullOrWhiteSpace(databaseDefinition.DatabasePassword))
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.UserName = databaseDefinition.DatabaseUser;
                builder.Password = databaseDefinition.DatabasePassword;
            }

            //builder.Pooling = true;
            //builder.MaxPoolSize = databaseDefinition.MaxPoolSize;

            if (databaseDefinition.Timeout != 0)
            {
                builder.Timeout = databaseDefinition.Timeout;
                builder.CommandTimeout = databaseDefinition.Timeout;
            }

            var connection = new NpgsqlConnection(builder.ConnectionString);
            connection.Open();
            return connection;
        }

        /// <summary>
        /// Returns a new connection for this database instance.
        /// </summary>
        /// <returns></returns>
        public override IDbConnection GetConnection()
        {
            return CreateConnection();
        }

        /// <summary>
        /// Returns a dictionary of all tables in this database.  If schemaBound is provided
        /// then only those tables within the schemas identified by schemaBound will be
        /// included.
        /// </summary>
        /// <param name="schemaBound"></param>
        /// <returns></returns>
        public override IDictionary<string, TableDefinition> GetTables(bool isCacheable = false)
        {
            if (isCacheable)
            {
                return _dataCache.GetItem(
                    string.Format("{0}.{1}.tables", _databaseDefinition.DatabaseHost, _databaseDefinition.DatabaseName),
                    () => GetTables(false));
            }

            var tableMap = new SortedDictionary<string, TableDefinition>();

            using (var connection = CreateConnection())
            {
                do
                {
                    var query = (
                           "SELECT " +
                           " c.TABLE_SCHEMA," +
                           " c.TABLE_NAME," +
                           " c.COLUMN_NAME," +
                           " c.ORDINAL_POSITION," +
                           " c.COLUMN_DEFAULT," +
                           " c.IS_NULLABLE," +
                           " c.DATA_TYPE," +
                           " c.CHARACTER_MAXIMUM_LENGTH," +
                           " c.NUMERIC_PRECISION," +
                           " c.NUMERIC_PRECISION_RADIX," +
                           " c.NUMERIC_SCALE," +
                           " c.DATETIME_PRECISION" +
                           " FROM INFORMATION_SCHEMA.COLUMNS c" +
                           " JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA" +
                           " WHERE t.TABLE_TYPE = 'BASE TABLE' " +
                           " ORDER BY c.ORDINAL_POSITION"
                           );

                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = query;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var schemaName = reader.GetString(0).ToLowerInvariant();
                            var tableName = reader.GetString(1).ToLowerInvariant();
                            var qualifiedTableName = schemaName + "." + tableName;
                            var table = tableMap.GetOrAdd(qualifiedTableName, t => new TableDefinition(schemaName, tableName));
                            table.Columns.Add(ReadColumnDefinition(reader));
                        }
                    }

                } while (false);

                // initialize the primary key constraint data for each table
                do
                {
                    var query = (
                        "SELECT TC.TABLE_SCHEMA, TC.TABLE_NAME, CCU.COLUMN_NAME" +
                        "  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC" +
                        "  LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME" +
                        " WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY TABLE_SCHEMA, TABLE_NAME"
                        );

                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = query;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var schemaName = reader.GetString(0).ToLowerInvariant();
                            var tableName = reader.GetString(1).ToLowerInvariant();
                            var qualifiedTableName = schemaName + "." + tableName;
                            var table = tableMap.Find(qualifiedTableName);
                            if (table == null)
                                continue;

                            table.MarkAsPrimaryKey(reader.GetString(2).ToLowerInvariant());
                        }
                    }
                } while (false);
            }

            return tableMap;
        }

        /// <summary>
        /// Reads a column from a data reader
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private static ColumnDefinition ReadColumnDefinition(IDataReader reader)
        {
            var column = new ColumnDefinition()
            {
                Name = reader.GetString(2).ToLowerInvariant(),
                OrdinalPosition = Convert.ToInt32(reader.GetValue(3)),
                Default = reader.GetStringOrNull(4),
                IsNullable = String.Equals("YES", reader.GetStringOrNull(5), StringComparison.OrdinalIgnoreCase),
                DataType = reader.GetStringOrNull(6)
            };

            if (!reader.IsDBNull(7))
                column.CharMaximumLength = Convert.ToInt32(reader.GetValue(7));
            if (!reader.IsDBNull(8))
                column.NumericPrecision = Convert.ToInt32(reader.GetValue(8));
            if (!reader.IsDBNull(9))
                column.NumericScale = Convert.ToInt32(reader.GetValue(9));
            if (!reader.IsDBNull(10))
                column.DateTimePrecision = Convert.ToInt32(reader.GetValue(10));
            return column;
        }

        /// <summary>
        /// Returns the set of schemas that are visible to this instance
        /// </summary>
        /// <returns></returns>
        public override ISet<string> GetSchemas(bool isCacheable = false)
        {
            if (isCacheable)
            {
                return _dataCache.GetItem(
                    string.Format("{0}.{1}.tables", _databaseDefinition.DatabaseHost, _databaseDefinition.DatabaseName),
                    () => GetSchemas(false));
            }

            return GetSchemasCommon(
                "^public$",
                "^storm_catalog$",
                "^information_schema$",
                "^pg_"
            );
        }

        public override ColumnDefinition GetCommonColumnDefinition(ColumnDefinition sourceColumnDefinition)
        {
            var targetColumnDefinition = new ColumnDefinition();
            targetColumnDefinition.Name = sourceColumnDefinition.Name;
            targetColumnDefinition.OrdinalPosition = sourceColumnDefinition.OrdinalPosition;
            targetColumnDefinition.Default = sourceColumnDefinition.Default;
            targetColumnDefinition.IsNullable = sourceColumnDefinition.IsNullable;
            targetColumnDefinition.IsIdentity = sourceColumnDefinition.IsIdentity;
            targetColumnDefinition.CharMaximumLength = sourceColumnDefinition.CharMaximumLength;
            targetColumnDefinition.NumericPrecision = sourceColumnDefinition.NumericPrecision;
            targetColumnDefinition.NumericScale = sourceColumnDefinition.NumericScale;
            targetColumnDefinition.DateTimePrecision = sourceColumnDefinition.DateTimePrecision;

            switch (sourceColumnDefinition.DataType)
            {
                case "bit":
                    targetColumnDefinition.DataType = "boolean";
                    break;
                case "int":
                    targetColumnDefinition.DataType = "integer";
                    break;
                case "tinyint":
                case "smallint":
                    targetColumnDefinition.DataType = "smallint";
                    break;
                case "bigint":
                    targetColumnDefinition.DataType = "bigint";
                    break;
                case "datetime":
                case "smalldatetime":
                    targetColumnDefinition.DataType = "timestamp without time zone";
                    break;
                case "date":
                    targetColumnDefinition.DataType = "date";
                    break;
                case "real":
                case "float":
                    targetColumnDefinition.DataType = "double precision";
                    break;
                case "numeric":
                case "decimal":
                    targetColumnDefinition.DataType = "numeric";
                    break;
                case "smallmoney":
                case "money":
                    targetColumnDefinition.DataType = "numeric";
                    break;
                case "varchar":
                case "nvarchar":
                case "sysname":
                    if (sourceColumnDefinition.CharMaximumLength <= 0)
                        targetColumnDefinition.DataType = "text";
                    else
                        targetColumnDefinition.DataType = "character varying";
                    break;
                case "char":
                    if (sourceColumnDefinition.CharMaximumLength == 1)
                        targetColumnDefinition.DataType = "character";
                    else
                        targetColumnDefinition.DataType = "character varying";
                    break;
                case "nchar":
                    targetColumnDefinition.DataType = "character varying";
                    break;
                case "text":
                case "ntext":
                    targetColumnDefinition.DataType = "text";
                    break;
                case "uniqueidentifier":
                    targetColumnDefinition.DataType = "uuid";
                    break;
                case "xml":
                    targetColumnDefinition.DataType = "text";
                    break;
                default:
                    targetColumnDefinition.DataType = null;
                    break;
            }

            return targetColumnDefinition;
        }

        internal static string GetPgsqlDataType(ColumnDefinition column)
        {
            switch (column.DataType)
            {
                case "bit":
                    return("boolean");
                case "int":
                    return("integer");
                case "tinyint":
                case "smallint":
                    return("smallint");
                case "bigint":
                    return("bigint");
                case "datetime":
                case "smalldatetime":
                    return("timestamp without time zone");
                case "date":
                    return("date");
                case "real":
                case "float":
                    return("float");
                case "numeric":
                case "decimal":
                    return(string.Format("numeric({0},{1})", column.NumericPrecision, column.NumericScale));
                case "smallmoney":
                case "money":
                    return("numeric");
                case "varchar":
                case "nvarchar":
                case "sysname":
                    if (column.CharMaximumLength <= 0)
                        return("text");
                    return(string.Format("varchar({0})", column.CharMaximumLength));
                case "char":
                    return (string.Format("char({0})", column.CharMaximumLength));
                case "text":
                case "ntext":
                    return("text");
                case "uniqueidentifier":
                    return("uuid");
                case "xml":
                    return("xml");
            }

            return("unknown");
        }

        public override void CreateTable(TableDefinition tableDefinition)
        {
            var queryWriter = new StringWriter();
            var delimiter = "\n\t";

            queryWriter.Write("CREATE TABLE {0}.{1}", tableDefinition.Schema, tableDefinition.Name);
            queryWriter.Write("(");

            foreach (var column in tableDefinition.Columns.OrderBy(c => c.OrdinalPosition))
            {
                queryWriter.Write(delimiter);
                queryWriter.Write("\"{0}\"", column.Name.ToLowerInvariant());
                queryWriter.Write(" ");
                queryWriter.Write(GetPgsqlDataType(column));
                queryWriter.Write(column.IsNullable ? " NULL" : " NOT NULL");

                if (!string.IsNullOrWhiteSpace(column.Default))
                {
                    queryWriter.Write(" DEFAULT {0}", RewriteDefault(column.DataType, column.Default));
                }

                delimiter = ",\n\t";
            }

            if (tableDefinition.HasPrimaryKey)
            {
                queryWriter.Write(delimiter);
                queryWriter.Write("PRIMARY KEY (");
                queryWriter.Write(string.Join(",", tableDefinition.PrimaryKey));
                queryWriter.Write(")");
            }

            queryWriter.WriteLine();
            queryWriter.WriteLine(") WITH ( OIDS = FALSE ) DISTRIBUTE BY REPLICATION");

            if (Logger != null)
            {
                Logger.Debug(" + {0}.{1}", tableDefinition.Schema, tableDefinition.Name);
            }

            using (var connection = CreateConnection())
            {
                try
                {
                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = queryWriter.ToString();
                    command.ExecuteNonQuery();
                }
                catch (NpgsqlException e)
                {
                    switch (e.Code)
                    {
                        case "42P07":
                            break;
                        default:
                            throw;
                    }
                }
            }
        }

        public override void CreateSchema(string schemaName)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = string.Format("CREATE SCHEMA \"{0}\"", schemaName.ToLowerInvariant());
                command.ExecuteNonQuery();
            }
        }

        private static string RewriteDefault(string dataType, string value)
        {
            value = value.Replace("getdate()", "now()");
            value = value.Replace("getutcdate()", "cast(now() at time zone 'utc' AS date)");
            value = value.Replace("suser_sname()", "user");
            value = value.Replace("newid()", "uuid_generate_v4()");

            if (dataType == "bit")
            {
                value = value.Replace("(1)", "true");
                value = value.Replace("(0)", "false");
            }

            return value;
        }

        public override IList<ForeignReference> GetForeignReferences(bool isCacheable = false)
        {
            if (isCacheable)
            {
                return _dataCache.GetItem(
                    string.Format("{0}.{1}.references", _databaseDefinition.DatabaseHost, _databaseDefinition.DatabaseName),
                    () => GetForeignReferences(false));
            }

            var references = new List<ForeignReference>();

            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = (
                    "SELECT r.conrelid, n.nspname, c.relname, conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef " +
                    "  FROM pg_catalog.pg_constraint r " +
                    "  LEFT JOIN pg_catalog.pg_class c on c.oid = r.conrelid " +
                    "  LEFT JOIN pg_catalog.pg_namespace n on n.oid = c.relnamespace " +
                    " WHERE r.contype = 'f' " +
                    " ORDER BY 1"
                    );

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var conmin = reader.GetString(4)
                            .RegexReplace("^FOREIGN KEY.*REFERENCES ", "")
                            .Replace("\"", "");
                        var contable = conmin
                            .RegexReplace("\\(.*", "");
                        var concolumns = conmin
                            .RegexReplace("^.*\\(", "")
                            .RegexReplace("\\).*", "")
                            .Split(',');

                        var reference = new ForeignReference()
                        {
                            SourceOID = Convert.ToString(reader.GetValue(0)),
                            SourceSchema = reader.GetString(1),
                            SourceTable = reader.GetString(2),
                            ReferenceName = reader.GetString(3),
                            ReferenceTable = contable,
                            ReferenceColumns = concolumns
                        };

                        references.Add(reference);
                    }
                }
            }

            return references;
        }

        /// <summary>
        /// Truncates the table
        /// </summary>
        /// <param name="qualifiedTableName"></param>
        public override void TruncateTable(string qualifiedTableName)
        {
            using (var connection = CreateConnection())
            {
                try
                {
                    var schemaAndTable = qualifiedTableName.Split('.');
                    var query = string.Format("TRUNCATE TABLE \"{0}\".\"{1}\" CASCADE", schemaAndTable[0], schemaAndTable[1]);
                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = query;
                    command.ExecuteNonQuery();
                }
                catch (NpgsqlException e)
                {
                    switch(e.Code)
                    {
                        case "3F000":
                            return;
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Unwraps the npgsql connection
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private NpgsqlConnection UnwrapConnection(IDbConnection connection)
        {
            if (connection is NpgsqlConnection)
                return (NpgsqlConnection)connection;
            else if (connection is ConnectionWrapper)
                return UnwrapConnection(((ConnectionWrapper)connection).WrappedConnection);

            throw new ArgumentException("invalid connection type", "connection");
        }

        private StreamReader OpenDataFile(string fileName)
        {
            if (fileName.EndsWith(".csv"))
                return new StreamReader(fileName, Encoding.Default, true, 128 * 1024);
            if (fileName.EndsWith(".csv.gz")) {
                var fstream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
                var gstream = new GZipStream(fstream, CompressionMode.Decompress, false);
                var sreader = new StreamReader(gstream, Encoding.Default, true, 128 * 1024);
                return sreader;
            }

            throw new ArgumentException("invalid file type", "fileName");
        }

        private string RewriteNullValues(string line)
        {
            line = line.RegexReplace(",$", ",@null@");
            line = line.RegexReplace("^,", "@null@,");

            string temp;

            do
            {
                temp = line;
                line = line.Replace(",,", ",@null@,");
            } while (temp != line);

            return line;
        }

        /// <summary>
        /// Inserts the 
        /// </summary>
        /// <param name="qualifiedTableName"></param>
        /// <param name="fileName"></param>
        public override void InsertIntoTable(string qualifiedTableName, string fileName)
        {
            using (var iconnection = CreateConnection())
            {
                using (var reader = OpenDataFile(fileName))
                {
                    // read the header
                    var header = reader.ReadLine();
                    // get the real connection
                    var connection = UnwrapConnection(iconnection);
                    var command = new NpgsqlCommand(string.Format("COPY {0} FROM STDIN WITH CSV HEADER NULL '@null@' ESCAPE '\\'", qualifiedTableName), connection);
                    var copyIn = new NpgsqlCopyIn(command, connection);
                    var lineCount = 0;

                    string line = string.Empty;

                    try
                    {
                        copyIn.Start();

                        var copyInStream = copyIn.CopyStream;
                        var copyInWriter = new StreamWriter(copyInStream, System.Text.Encoding.UTF8, 16384);

                        // write the header
                        copyInWriter.WriteLine(header);
                        // stream the lines
                        while ((line = reader.ReadLine()) != null)
                        {
                            // lines must be cleaned up so that nulls are transformed properly into null values
                            line = RewriteNullValues(line);
                            // write the line
                            copyInWriter.WriteLine(line);
                            lineCount++;
                        }

                        copyInWriter.Flush();
                        copyInStream.Close();

                        if (Logger != null)
                        {
                            Logger.Info("Loaded table: {0} : {1}", qualifiedTableName, lineCount);
                        }
                    }
                    catch (NpgsqlException e)
                    {
                        copyIn.Cancel("undo copy");

                        if (Logger != null)
                        {
                            Logger.Info("Loading table: {0} : FAILURE", qualifiedTableName);
                            Logger.Info(e.Message);
                            Logger.Info(e.StackTrace);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Provides a context for altering a table in the database.
        /// </summary>
        /// <param name="tableDefinition"></param>
        /// <returns></returns>
        public override AlterTableContext AlterTable(TableDefinition tableDefinition)
        {
            return new PgAlterTableContext(this, tableDefinition);
        }

        /// <summary>
        /// A table alter context for pgsql
        /// </summary>
        internal class PgAlterTableContext : AlterTableContext
        {
            /// <summary>
            /// Gets or sets the database associated with this context
            /// </summary>
            internal PgDatabaseInstance Database { get; set; }
            /// <summary>
            /// Gets or sets the table associated with this context
            /// </summary>
            internal TableDefinition Table { get; set; }

            internal PgAlterTableContext() { }
            internal PgAlterTableContext(PgDatabaseInstance database, TableDefinition table)
            {
                Database = database;
                Table = table;
            }

            /// <summary>
            /// Wrapper to execute a nonquery and eat errors
            /// </summary>
            /// <param name="commandText"></param>
            /// <param name="errorsToEat"></param>
            private void ExecuteNonQuery(string commandText, params string[] errorsToEat)
            {
                using (var connection = Database.CreateConnection())
                {
                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = commandText;

                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (NpgsqlException e)
                    {
                        if ((errorsToEat != null) && (errorsToEat.Length > 0))
                        {
                            if (errorsToEat.All(err => e.Code != err))
                            {
                                Console.Error.WriteLine(commandText);
                                throw;
                            }
                        }
                    }
                }
            }

            /// <summary>
            /// Sets the definition for a column
            /// </summary>
            /// <param name="columnDefinition"></param>
            public void SetColumnDefinition(ColumnDefinition columnDefinition)
            {
                var baseWriter = new StringWriter();
                baseWriter.Write("ALTER TABLE {0}.{1} ", Table.Schema, Table.Name);
                baseWriter.Write("ALTER COLUMN {0} ", columnDefinition.Name);
                var baseSQL = baseWriter.ToString();

                ExecuteNonQuery(
                    baseSQL + "SET DATA TYPE " + GetPgsqlDataType(columnDefinition));
                ExecuteNonQuery(
                    baseSQL + (columnDefinition.IsNullable ? "DROP NOT NULL" : "SET NOT NULL"));

                if (string.IsNullOrWhiteSpace(columnDefinition.Default))
                {
                    ExecuteNonQuery(baseSQL + "DROP DEFAULT");
                }
                else
                {
                    ExecuteNonQuery(baseSQL + "SET DEFAULT " + RewriteDefault(columnDefinition.DataType, columnDefinition.Default));
                }
            }

            /// <summary>
            /// Adds a column to the table
            /// </summary>
            /// <param name="columnDefinition"></param>
            public void AddColumn(ColumnDefinition columnDefinition)
            {
                var queryWriter = new StringWriter();

                queryWriter.Write("ALTER TABLE {0}.{1} ", Table.Schema, Table.Name);
                queryWriter.Write("ADD COLUMN \"{0}\" ", columnDefinition.Name.ToLowerInvariant());
                queryWriter.Write(GetPgsqlDataType(columnDefinition));
                queryWriter.Write(columnDefinition.IsNullable ? " NULL" : " NOT NULL");

                if (!string.IsNullOrWhiteSpace(columnDefinition.Default))
                {
                    queryWriter.Write(" DEFAULT {0}", RewriteDefault(columnDefinition.DataType, columnDefinition.Default));
                }

                ExecuteNonQuery(queryWriter.ToString(), "42701");
            }

            /// <summary>
            /// Drops a column from a table
            /// </summary>
            /// <param name="columnName"></param>
            public void DropColumn(string columnName)
            {
                var queryWriter = new StringWriter();

                queryWriter.Write("ALTER TABLE {0}.{1} ", Table.Schema, Table.Name);
                queryWriter.Write("DROP COLUMN IF EXISTS \"{0}\" CASCADE", columnName.ToLower());

                ExecuteNonQuery(queryWriter.ToString(), "42701");
            }

        }
    }
}
