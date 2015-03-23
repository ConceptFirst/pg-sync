namespace PgSync.Common
{
    public interface AlterTableContext
    {
        void AddColumn(ColumnDefinition column);
        void SetColumnDefinition(ColumnDefinition column);
        void DropColumn(string columnName);
    }
}
