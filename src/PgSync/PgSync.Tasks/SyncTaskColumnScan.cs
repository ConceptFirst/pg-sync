using System.Collections.Generic;

namespace PgSync.Tasks
{
    /// <summary>
    /// A brute force approach that limits analysis to the columns of the
    /// table.  In this approach we may generate a compound key for analysis and
    /// submit it to the remote database for evaluation.
    /// </summary>
    public class SyncTaskColumnScan : SyncTask
    {
        public ISet<string> Columns { get; set; }

        /// <summary>
        /// Indicates that this scan is a rainbow scan, meaning its looking at all columns
        /// in the source and target and generating a unique value for each row in the database
        /// which it then compares between the two databases.  Rainbow scans can be
        /// exceptionally intensive and as such we highlight them so that we can expressly
        /// exclude them from execution.
        /// </summary>
        public bool IsRainbowScan { get; set; }

        public SyncTaskColumnScan() { }
        public SyncTaskColumnScan(ISet<string> columns)
        {
            Columns = new SortedSet<string>(columns);
        }
    }
}
