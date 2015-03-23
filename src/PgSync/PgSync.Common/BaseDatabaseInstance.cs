using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;

namespace PgSync.Common
{
    public abstract class BaseDatabaseInstance : IDatabaseInstance
    {
        public virtual IDbConnection GetConnection()
        {
            throw new NotImplementedException();
        }

        public virtual IDictionary<string, TableDefinition> GetTables(bool isCacheable = false)
        {
            throw new NotImplementedException();
        }

        public virtual void CreateTable(TableDefinition tableDefinition)
        {
            throw new NotImplementedException();
        }

        public virtual void CreateSchema(string schemaName)
        {
            throw new NotImplementedException();
        }

        public virtual AlterTableContext AlterTable(TableDefinition tableDefinition)
        {
            throw new NotImplementedException();
        }

        public virtual ColumnDefinition GetCommonColumnDefinition(ColumnDefinition sourceColumnDefinition)
        {
            throw new NotImplementedException();
        }

        abstract public IList<ForeignReference> GetForeignReferences(bool isCacheable);
        abstract public void TruncateTable(string qualifiedTableName);
        abstract public void InsertIntoTable(string qualifiedTableName, string fileName);

        public virtual IDisposable IsolatedConnections()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns all schemas visible to this database instance
        /// </summary>
        /// <returns></returns>
        abstract public ISet<string> GetSchemas(bool isCacheable);

        /// <summary>
        /// Returns all schemas except those that match the regex filter.
        /// </summary>
        /// <param name="regexFilters"></param>
        /// <returns></returns>
        public virtual ISet<string> GetSchemasCommon(params string[] regexFilters)
        {
            var filters =
                regexFilters != null && regexFilters.Length > 0 ?
                regexFilters.Select(sr => new Regex(sr, RegexOptions.IgnoreCase)).ToList() :
                null;

            using (var srcConnection = GetConnection())
            {
                var command = srcConnection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";

                var schemas = new SortedSet<string>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schema = reader.GetString(0).ToLowerInvariant();
                        if ((filters == null) || !filters.Any(r => r.IsMatch(schema)))
                            schemas.Add(reader.GetString(0).ToLowerInvariant());
                    }
                }

                return schemas;
            }
        }
    }
}
