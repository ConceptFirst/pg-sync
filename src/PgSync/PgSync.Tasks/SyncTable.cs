using System.Collections.Generic;

namespace PgSync.Tasks
{
    public class SyncTable
    {
        public string Schema { get; set; }
        public string Table { get; set; }
        public ISet<string> Dependencies { get; set; }
        public IList<SyncTask> Tasks { get; set; }
    }
}
