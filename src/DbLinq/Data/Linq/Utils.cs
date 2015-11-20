using System;
using System.Linq;
using System.Globalization;
using System.Collections;
using System.Collections.Generic;

namespace DbLinq.Data.Linq
{
    public static class Utils
    {
        private static NumberFormatInfo FormatPoint = new NumberFormatInfo() { NumberDecimalSeparator = "." };

        public static string DictionaryToHStoreString(IDictionary dict)
        {
            if (dict is Dictionary<string, string>)
                return string.Join(",", (dict as Dictionary<string, string>).Select(x => CreateHStoreItemString(x.Key, x.Value)).ToArray());
            else if (dict is Dictionary<string, int>)
                return string.Join(",", (dict as Dictionary<string, int>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString())).ToArray());
            else if (dict is Dictionary<string, float>)
                return string.Join(",", (dict as Dictionary<string, float>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString(FormatPoint))).ToArray());
            else if (dict is Dictionary<string, double>)
                return string.Join(",", (dict as Dictionary<string, double>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString(FormatPoint))).ToArray());
            else if (dict is Dictionary<string, DateTime>)
                return string.Join(",", (dict as Dictionary<string, DateTime>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString("yyyy.MM.dd HH:mm:ss.fffffff"))).ToArray());

            return null;
        }

        public static string ToHStoreString(this IDictionary dict)
        {
            return DictionaryToHStoreString(dict);
        }

        public static string CreateHStoreItemString(string key, string value)
        {
            return string.Format("\"{0}\"=>\"{1}\"", key, value);
        }
    }
}
