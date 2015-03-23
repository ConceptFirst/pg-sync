using System;
using System.Collections.Generic;
using System.Data;

using Npgsql;

namespace PgSync.Common
{
    public static class PgDatabaseExtensions
    {
        public static NpgsqlDataReader ExecuteCommand(this NpgsqlConnection connection, string commandText, params KeyValuePair<string, object>[] args)
        {
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.CommandType = CommandType.Text;

            if (args != null)
            {
                foreach (var arg in args)
                {
                    command.Parameters.AddWithValue(arg.Key, arg.Value);
                }
            }

            return command.ExecuteReader();
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

        public static IDictionary<string, TableDefinition> GetTables(this NpgsqlConnection connection, ISet<string> schemaBound = null)
        {
            var tableMap = new SortedDictionary<string, TableDefinition>();

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
                    " c.NUMERIC_SCALE" +
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
                        if ((schemaBound == null) || (schemaBound.Contains(schemaName)))
                        {
                            var tableName = reader.GetString(1).ToLowerInvariant();
                            var qualifiedTableName = schemaName + "." + tableName;
                            var table = tableMap.GetOrAdd(qualifiedTableName, t => new TableDefinition(schemaName, tableName));
                            table.Columns.Add(ReadColumnDefinition(reader));
                        }
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
                        if ((schemaBound == null) || (schemaBound.Contains(schemaName)))
                        {
                            var tableName = reader.GetString(1).ToLowerInvariant();
                            var qualifiedTableName = schemaName + "." + tableName;
                            var table = tableMap.Find(qualifiedTableName);
                            if (table == null)
                                continue;

                            table.MarkAsPrimaryKey(
                                reader.GetString(2).ToLowerInvariant());
                        }
                    }
                }
            } while (false);

            return tableMap;
        }
    }
}
