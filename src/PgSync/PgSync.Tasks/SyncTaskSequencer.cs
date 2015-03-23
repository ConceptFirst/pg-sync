namespace PgSync.Tasks
{
    /// <summary>
    /// The task that looks for sequential values in a column of the database.
    /// </summary>
    public class SyncTaskSequencer : SyncTask
    {
        public string Column { get; set; }

        public SyncTaskSequencer() { }
        public SyncTaskSequencer(string column)
        {
            Column = column;
        }
    }
}
