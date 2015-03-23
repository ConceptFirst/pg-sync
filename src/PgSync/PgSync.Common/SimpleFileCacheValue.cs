using System;

namespace PgSync.Common
{
    public class SimpleFileCacheValue<T>
    {
        /// <summary>
        /// Gets or sets the underlying value
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// Gets or sets the expiration time for the value
        /// </summary>
        public DateTime Expiration { get; set; }

        /// <summary>
        /// Default constructor
        /// </summary>
        public SimpleFileCacheValue()
        {
            Value = default(T);
            Expiration = DateTime.UtcNow;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="value"></param>
        /// <param name="expiration"></param>
        public SimpleFileCacheValue(T value, DateTime expiration)
        {
            Value = value;
            Expiration = expiration;
        }
    }
}
