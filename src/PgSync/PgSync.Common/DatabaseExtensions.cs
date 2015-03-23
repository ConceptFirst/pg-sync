using System.Data;

namespace PgSync.Common
{
    static class DatabaseExtensions
    {
        public static string GetStringOrNull(this IDataReader reader, int position)
        {
            return reader.IsDBNull(position) ? null : reader.GetString(position);
        }
    }
}
