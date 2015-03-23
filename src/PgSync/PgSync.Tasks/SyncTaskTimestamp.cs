namespace PgSync.Tasks
{
    /// <summary>
    /// The task that looks for timestamp values in a column of the database.
    /// </summary>
    public class SyncTaskTimestamp : SyncTask
    {
        public string Column { get; set; }

        public SyncTaskTimestamp() { }
        public SyncTaskTimestamp(string column)
        {
            Column = column;
        }
    }
}
