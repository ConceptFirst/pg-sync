namespace PgSync.Common
{
    public class ColumnDefinition
    {
        public string Name { get; set; }
        public int OrdinalPosition { get; set; }
        public string DataType { get; set; }
        public string Default { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public int CharMaximumLength { get; set; }
        public int NumericPrecision { get; set; }
        public int NumericScale { get; set; }
        public int DateTimePrecision { get; set; }
    }
}
