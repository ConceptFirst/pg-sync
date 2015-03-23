using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace PgSync.Common
{
    public class MsDatabaseInstance : BaseDatabaseInstance
    {
        private DatabaseDefinition _databaseDefinition;

        /// <summary>
        /// The data-cache instance
        /// </summary>
        private IDataCache _dataCache;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="databaseDefinition"></param>

        public MsDatabaseInstance(DatabaseDefinition databaseDefinition, IDataCache dataCache = null)
        {
            _databaseDefinition = databaseDefinition;
            _dataCache = dataCache ?? new SimpleFileCache();
        }

        /// <summary>
        /// Gets a connection to the database.
        /// </summary>
        /// <returns></returns>
        public SqlConnection CreateConnection(DatabaseDefinition databaseDefinition = null)
        {
            if (databaseDefinition == null)
                databaseDefinition = _databaseDefinition;

            var builder = new SqlConnectionStringBuilder();

            if (!string.IsNullOrWhiteSpace(databaseDefinition.DatabaseHost))
            {
                builder.DataSource = databaseDefinition.DatabaseHost;
            }

            if (!string.IsNullOrWhiteSpace(databaseDefinition.DatabaseName))
            {
                builder.InitialCatalog = databaseDefinition.DatabaseName;
            }

            if (string.IsNullOrWhiteSpace(databaseDefinition.DatabaseUser) ||
                string.IsNullOrWhiteSpace(databaseDefinition.DatabasePassword))
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.UserID = databaseDefinition.DatabaseUser;
                builder.Password = databaseDefinition.DatabasePassword;
            }

            builder.ConnectTimeout = databaseDefinition.Timeout;
            builder.Pooling = true;
            builder.MaxPoolSize = databaseDefinition.MaxPoolSize;
            builder.AsynchronousProcessing = true;
            builder.PacketSize = 32 * 1024;

            var connection = new SqlConnection(builder.ConnectionString);
            connection.Open();
            return connection;
        }

        /// <summary>
        /// Returns a new connection for this database instance.
        /// </summary>
        /// <returns></returns>
        public override IDbConnection GetConnection()
        {
            return CreateConnection(_databaseDefinition);
        }

        internal class MsIndexDefinition : IndexDefinition
        {
            public int TableId { get; set; }
            public int IndexId { get; set; }
        }

        /// <summary>
        /// Gets the indexes associated with this database
        /// </summary>
        /// <param name="isCacheable"></param>
        /// <returns></returns>
        public IList<IndexDefinition> GetIndexes(bool isCacheable = false)
        {
            if (isCacheable)
            {
                return _dataCache.GetItem(
                    string.Format("{0}.{1}.indexes", _databaseDefinition.DatabaseHost, _databaseDefinition.DatabaseName),
                    () => GetIndexes(false));
            }


            var indexes = new List<IndexDefinition>();

            using (var connection = CreateConnection())
            {
                do
                {
                    var query = (
                        "SELECT " +
                        " schemas.name as [schema], " +
                        " tables.name as [table], " +
                        " indexes.name as [name], " +
                        " indexes.type_desc as type, " +
                        " indexes.is_unique, " +
                        " indexes.is_primary_key, " +
                        " indexes.object_id, " +
                        " indexes.index_id " +
                        " from sys.indexes as indexes " +
                        "  JOIN sys.tables as tables on indexes.object_id = tables.object_id " +
                        "  JOIN sys.schemas as schemas on tables.schema_id = schemas.schema_id"
                        );

                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = query;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableId = Convert.ToInt32(reader.GetValue(6));
                            var indexId = Convert.ToInt32(reader.GetValue(7));
                            var indexName = reader.GetValue(2);
                            if (DBNull.Value.Equals(indexName))
                                indexName = string.Empty;
                            else
                                indexName = ((string)indexName).ToLowerInvariant();

                            var indexDefinition = new MsIndexDefinition()
                            {
                                Name = (string)indexName,
                                Schema = reader.GetString(0).ToLowerInvariant(),
                                Table = reader.GetString(1).ToLowerInvariant(),
                                Type = reader.GetString(3).ToLowerInvariant(),
                                IsUnique = 1.Equals(Convert.ToInt32(reader.GetValue(4))),
                                IsPrimaryKey = 1.Equals(Convert.ToInt32(reader.GetValue(5))),
                                Columns = new HashSet<string>(),
                                TableId = tableId,
                                IndexId = indexId
                            };

                            indexes.Add(indexDefinition);
                        }
                    }
                } while (false);

                var indexTable = indexes
                    .Cast<MsIndexDefinition>()
                    .ToDictionary(index => new Pair<int, int>(index.TableId, index.IndexId));

                do
                {
                    var query = (
                        "select " +
                        " indexes.object_id, " +
                        " indexes.index_id, " +
                        " columns.name as [column] " + 
                        "   from sys.indexes as indexes" +
                        "   join sys.index_columns as index_columns on indexes.object_id = index_columns.object_id and indexes.index_id = index_columns.index_id" +
                        "   join sys.columns as columns on indexes.object_id = columns.object_id and index_columns.column_id = columns.column_id"
                        );

                    var command = connection.CreateCommand();
                    command.CommandType = CommandType.Text;
                    command.CommandText = query;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableId = Convert.ToInt32(reader.GetValue(0));
                            var indexId = Convert.ToInt32(reader.GetValue(1));
                            var columnName = reader.GetString(2);
                            var index = indexTable.Find(new Pair<int, int>(tableId, indexId));
                            if (index != null)
                            {
                                index.Columns.Add(columnName);
                            }
                        }
                    }
                } while (false);
            }

            return indexes;
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
                           " s.name as schema_name," +
                           " t.name as table_name," +
                           " c.name as column_name," +
                           " c.column_id," +
                           " d.definition," +
                           " c.is_nullable," +
                           " y.name as data_type," +
                           " c.max_length," +
                           " c.precision," +
                           " c.scale," +
                           " c.is_identity" +
                           " FROM sys.columns c" +
                           " JOIN sys.tables t ON t.object_id = c.object_id" +
                           " JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                           " JOIN sys.types y ON y.user_type_id = c.user_type_id" +
                           " LEFT JOIN sys.default_constraints d on d.parent_column_id = c.column_id and d.parent_object_id = t.object_id" +
                           " WHERE t.type = 'U' " +
                           " ORDER BY c.column_id"
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
        private ColumnDefinition ReadColumnDefinition(IDataReader reader)
        {
            var column = new ColumnDefinition()
            {
                Name = reader.GetString(2).ToLowerInvariant(),
                OrdinalPosition = Convert.ToInt32(reader.GetValue(3)),
                Default = reader.GetStringOrNull(4),
                IsNullable = reader.GetBoolean(5),
                DataType = reader.GetStringOrNull(6)
            };

            if (!reader.IsDBNull(7))
                column.CharMaximumLength = Convert.ToInt32(reader.GetValue(7));
            if (!reader.IsDBNull(8))
                column.NumericPrecision = Convert.ToInt32(reader.GetValue(8));
            if (!reader.IsDBNull(9))
                column.NumericScale = Convert.ToInt32(reader.GetValue(9));
            if (!reader.IsDBNull(10))
                column.IsIdentity = reader.GetBoolean(10);
            return column;
        }

        /// <summary>
        /// Returns the set of schemas that are visible to this instance
        /// </summary>
        /// <returns></returns>
        public override ISet<string> GetSchemas(bool isCacheable)
        {
            if (isCacheable)
            {
                return _dataCache.GetItem(
                    string.Format("{0}.{1}.schemas", _databaseDefinition.DatabaseHost, _databaseDefinition.DatabaseName),
                    () => GetSchemas(false));
            }

            return GetSchemasCommon(
                "^dbo$",
                "^sys$",
                "^guest$",
                "^public$",
                "^archive$",
                "^information_schema$",
                "^db_"
            );
        }

        private Dictionary<long, AllocationUnit> GetAllocationUnits(bool isCacheable = false)
        {
            if (isCacheable)
            {
                return _dataCache.GetItem(
                    string.Format("{0}.{1}.allocations", _databaseDefinition.DatabaseHost, _databaseDefinition.DatabaseName),
                    () => GetAllocationUnits(false));
            }

            var query = (
                "select alloc.allocation_unit_id, schemas.name as [schema], objects.name, objects.object_id " +
                "  from sys.allocation_units as alloc " +
                "  join sys.partitions as parts on alloc.container_id = parts.partition_id and alloc.type in (1,3)" +
                "  join sys.objects as objects on objects.object_id = parts.object_id" +
                "  join sys.schemas as schemas on schemas.schema_id = objects.schema_id" +
                " where objects.type = 'U' " +
                " order by [schema], [name] asc;"
            );

            var allocationsTable = new Dictionary<long, AllocationUnit>();

            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = query;

                using (var reader = command.ExecuteReader())
                {
                    while(reader.Read()) {
                        var allocationUnit = new AllocationUnit()
                        {
                            AllocationUnitId = Convert.ToInt64(reader.GetValue(0)),
                            Schema = reader.GetString(1),
                            TableName = reader.GetString(2),
                            ObjectId = Convert.ToInt64(reader.GetInt32(3))
                        };

                        allocationsTable[allocationUnit.AllocationUnitId] = allocationUnit;
                    }
                }
            }

            return allocationsTable;
        }

        public void LogScan()
        {
            var databaseTables = GetTables(true);
            var allocationUnits = GetAllocationUnits(true);

            using (var connection = CreateConnection())
            {
                var query = (
                    "SELECT TOP 5 * FROM ::fn_dblog(null, null) " +
                    " WHERE Operation = 'LOP_INSERT_ROWS'"
                );

                var command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = query;

                using (var reader = command.ExecuteReader())
                {
                    var iLsn = reader.GetOrdinal("Current LSN");
                    var iContext = reader.GetOrdinal("Context");
                    var iAllocationUnit = reader.GetOrdinal("AllocUnitId");
                    var iRowLog0 = reader.GetOrdinal("RowLog Contents 0");
                    var iRowLog1 = reader.GetOrdinal("RowLog Contents 1");
                    var iRowLog2 = reader.GetOrdinal("RowLog Contents 2");
                    var iRowLog3 = reader.GetOrdinal("RowLog Contents 3");
                    var iRowLog4 = reader.GetOrdinal("RowLog Contents 4");


                    while (reader.Read())
                    {
                        var lsn = reader.GetString(iLsn);
                        var context = reader.GetString(iContext);
                        if (context == "LCX_INDEX_LEAF")
                            continue;

                        var allocationUnitId = Convert.ToInt64(reader.GetValue(iAllocationUnit));
                        var allocationUnit = allocationUnits.Find(allocationUnitId);
                        if (allocationUnit == null)
                            Console.WriteLine("null allocation unit");
                        var rowLogContents0 = (byte[]) reader.GetValue(iRowLog0);
                        var rowLogContents1 = (byte[]) reader.GetValue(iRowLog1);
                        var rowLogContents2 = (byte[]) reader.GetValue(iRowLog2);
                        var rowLogContents3 = (byte[]) reader.GetValue(iRowLog3);
                        var rowLogContents4 = (byte[]) reader.GetValue(iRowLog4);

                        LogRowReader.Read(rowLogContents0, allocationUnit);
                    }
                }
            }
        }

        public override IList<ForeignReference> GetForeignReferences(bool isCacheable)
        {
            var references = new List<ForeignReference>();

            using (var connection = CreateConnection())
            {
                var query = (
                    "SELECT RC.CONSTRAINT_NAME FK_Name " +
                    " , KF.TABLE_SCHEMA FK_Schema" +
                    " , KF.TABLE_NAME FK_Table" +
                    " , KF.COLUMN_NAME FK_Column" +
                    " , RC.UNIQUE_CONSTRAINT_NAME PK_Name" +
                    " , KP.TABLE_SCHEMA PK_Schema" +
                    " , KP.TABLE_NAME PK_Table" +
                    " , KP.COLUMN_NAME PK_Column" +
                    " , RC.MATCH_OPTION MatchOption" +
                    " , RC.UPDATE_RULE UpdateRule" +
                    " , RC.DELETE_RULE DeleteRule" +
                    " FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC" +
                    " JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF ON RC.CONSTRAINT_NAME = KF.CONSTRAINT_NAME" +
                    " JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KP ON RC.UNIQUE_CONSTRAINT_NAME = KP.CONSTRAINT_NAME"
                );

                var command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = query;

                using (var reader = command.ExecuteReader())
                {
                    // foreign key
                    var fkName = reader.GetString(0);
                    var fkSchema = reader.GetString(1);
                    var fkTable = reader.GetString(2);
                    // source key
                    var skSchema = reader.GetString(5);
                    var skTable = reader.GetString(6);
                    var skColumn = reader.GetString(7);

                    // find the reference

                    var reference = new ForeignReference()
                    {
                        SourceSchema = fkSchema,
                        SourceTable = fkTable,
                        ReferenceName = fkName,
                        ReferenceTable = skTable
                    };

                    references.Add(reference);

                }
            }

            return references;
        }

        public override void TruncateTable(string qualifiedTableName)
        {
            using (var connection = CreateConnection())
            {
                var schemaAndTable = qualifiedTableName.Split('.');
                var query = string.Format("TRUNCATE TABLE [{0}].[{1}]", schemaAndTable[0], schemaAndTable[1]);
                var command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = query;
                command.ExecuteNonQuery();
            }
        }

        public override void InsertIntoTable(string qualifiedTableName, string fileName)
        {
            throw new NotImplementedException();
        }
    }
}
