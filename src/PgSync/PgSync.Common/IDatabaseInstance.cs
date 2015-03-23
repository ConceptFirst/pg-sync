using System;
using System.Collections.Generic;
using System.Data;

namespace PgSync.Common
{
    public interface IDatabaseInstance
    {
        IDbConnection GetConnection();
        ISet<string> GetSchemas(bool isCacheable = false);
        IDictionary<string, TableDefinition> GetTables(bool isCacheable = false);
        void CreateSchema(string schemaName);
        void CreateTable(TableDefinition tableDefinition);
        AlterTableContext AlterTable(TableDefinition tableDefinition);
        ColumnDefinition GetCommonColumnDefinition(ColumnDefinition sourceColumnDefinition);
        IList<ForeignReference> GetForeignReferences(bool isCacheable = false);
        void TruncateTable(string qualifiedTableName);
        void InsertIntoTable(string qualifiedTableName, string fileName);
        IDisposable IsolatedConnections();
    }
}
