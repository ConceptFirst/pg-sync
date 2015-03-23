using System.Text.RegularExpressions;

namespace PgSync.Common
{
    public static class StringExtensions
    {
        public static string RegexReplace(this string input, string pattern, string replacement)
        {
            return Regex.Replace(input, pattern, replacement);
        }

    }
}
