using System.Collections.Generic;
using System.Linq;

namespace PgSync.Common
{
    public class TableDefinition
    {
        public string Schema { get; set; }
        public string Name { get; set; }
        public IList<ColumnDefinition> Columns { get; set; }
        public ISet<string> PrimaryKey { get; set; }

        /// <summary>
        /// Returns true if this table has a primary key
        /// </summary>
        
        public bool HasPrimaryKey
        {
            get { return PrimaryKey != null && PrimaryKey.Count > 0; }
        }

        /// <summary>
        /// Returns true if the column name is part of the primary key.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public bool IsPrimaryKey(string columnName)
        {
            return PrimaryKey.Contains(columnName);
        }

        public void MarkAsPrimaryKey(string columnName)
        {
            PrimaryKey.Add(columnName);
        }

        /// <summary>
        /// Returns the column that matches the given column name.
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public ColumnDefinition GetColumn(string columnName)
        {
            columnName = columnName.ToLower();
            return Columns.FirstOrDefault(c => c.Name == columnName);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public TableDefinition()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        public TableDefinition(string schema, string name)
        {
            Schema = schema;
            Name = name;
            Columns = new List<ColumnDefinition>();
            PrimaryKey = new HashSet<string>();
        }
    }
}
