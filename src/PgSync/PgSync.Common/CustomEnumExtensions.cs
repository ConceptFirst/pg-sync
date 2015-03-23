using System;
using System.Collections.Generic;

namespace PgSync.Common
{
    public static class CustomEnumExtensions
    {
        public static V GetOrAdd<K, V>(this IDictionary<K, V> dictionary, K key, Func<K,V> valueFactory)
        {
            V value;
            if (dictionary.TryGetValue(key, out value))
                return value;

            dictionary[key] = value = valueFactory(key);
            return value;
        }

        /// <summary>
        /// Finds a key in the dictionary
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="dictionary"></param>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static V Find<K, V>(this IDictionary<K, V> dictionary, K key, V defaultValue = default(V))
        {
            V value;
            if (!dictionary.TryGetValue(key, out value))
                return defaultValue;
            return value;
        }

        public static SortedSet<T> ToSortedSet<T>(
            this IEnumerable<T> input)
        {
            var result = new SortedSet<T>();

            foreach (T inputValue in input)
            {
                result.Add(inputValue);
            }

            return result;
        }

        /// <summary>
        /// Converts the input to a sorted dictionary assuming default comparison operators.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="input"></param>
        /// <param name="keySelector"></param>
        /// <param name="valSelector"></param>
        /// <returns></returns>
        
        public static SortedDictionary<K,V> ToSortedDictionary<K,V,T>(
            this IEnumerable<T> input,
            Func<T, K> keySelector,
            Func<T, V> valSelector)
        {
            var result = new SortedDictionary<K, V>();

            foreach (T inputValue in input)
            {
                result[keySelector(inputValue)] = valSelector.Invoke(inputValue);
            }

            return result;
        }
    }
}
