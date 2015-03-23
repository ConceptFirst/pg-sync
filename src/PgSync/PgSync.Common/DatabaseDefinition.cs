namespace PgSync.Common
{
    public class DatabaseDefinition
    {
        public string DatabaseUser { get; set; }
        public string DatabasePassword { get; set; }
        public string DatabaseHost { get; set; }
        public string DatabaseName { get; set; }
        public int Timeout { get; set; }
        public int MaxPoolSize { get; set; }
    }
}
