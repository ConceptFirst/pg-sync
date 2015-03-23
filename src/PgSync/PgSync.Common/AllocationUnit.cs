namespace PgSync.Common
{
    public class AllocationUnit
    {
        public long AllocationUnitId { get; set; }
        public string Schema { get; set; }
        public long ObjectId { get; set; }
        public string TableName { get; set; }
    }

    public class AllocationUnit<T> : AllocationUnit
    {
        public T CustomValue { get; set; }
    }
}
