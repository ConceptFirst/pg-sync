using System.Collections.Generic;

namespace PgSync.Common
{
    public class IndexDefinition
    {
        public string Schema { get; set; }
        public string Table { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public ISet<string> Columns { get; set; }
    }
}
