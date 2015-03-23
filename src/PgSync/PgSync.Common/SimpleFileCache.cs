using System;
using System.IO;

using MsgPack.Serialization;

namespace PgSync.Common
{
    public class SimpleFileCache : IDataCache
    {
        /// <summary>
        /// Gets or sets the location where data is stored.
        /// </summary>
        public string DataStorePath { get; set; }

        /// <summary>
        /// Gets or sets the default cache age
        /// </summary>
        public int DefaultCacheAge { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public SimpleFileCache()
        {
            DefaultCacheAge = 60 * 60; // one hour cache
            DataStorePath = Path.Combine(Directory.GetCurrentDirectory(), "cache");
            if (!Directory.Exists(DataStorePath))
                Directory.CreateDirectory(DataStorePath);
        }

        /// <summary>
        /// Gets an item from the cache.  If the item does not exist, it invokes the item factory
        /// to create the item.  The item will be stored until the cache policy associated with
        /// the value has been satisfied.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="resourceId"></param>
        /// <param name="itemFactory"></param>
        /// <param name="cacheAge"></param>
        /// <returns></returns>
        public T GetItem<T>(string resourceId, Func<T> itemFactory)
        {
            var serializer = SerializationContext.Default.GetSerializer<SimpleFileCacheValue<T>>();
            var cacheFile = Path.Combine(DataStorePath, string.Format("{0}.dat", resourceId));
            if (File.Exists(cacheFile))
            {
                var cacheValue = serializer.UnpackSingleObject(File.ReadAllBytes(cacheFile));
                if (cacheValue.Expiration > DateTime.UtcNow)
                    return cacheValue.Value;
            }

            var expiration = DateTime.UtcNow.AddSeconds(DefaultCacheAge);
            var itemInstance = itemFactory();
            var itemCacheValue = new SimpleFileCacheValue<T>(itemInstance, expiration);
            File.WriteAllBytes(cacheFile, serializer.PackSingleObject(itemCacheValue));
            return itemInstance;
        }
    }
}
