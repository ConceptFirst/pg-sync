using System;
using System.Collections.Generic;

namespace PgSync.Common
{
    public static class ForEachExtension
    {
        public static void ForEach<T>(this IEnumerable<T> enumerable, Action<T> action)
        {
            foreach (var value in enumerable)
            {
                action(value);
            }
        }
    }
}
