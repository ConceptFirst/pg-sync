using System;

namespace PgSync.Common
{
    public interface IDataCache
    {
        T GetItem<T>(string resourceId, Func<T> itemFactory);
    }
}
