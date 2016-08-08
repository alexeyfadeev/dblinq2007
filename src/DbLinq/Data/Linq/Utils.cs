using System;
using System.Linq;
using System.Globalization;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using DbLinq.Data.Linq.Implementation;

namespace DbLinq.Data.Linq
{
    public static class Utils
    {
        private static NumberFormatInfo FormatPoint = new NumberFormatInfo() { NumberDecimalSeparator = "." };

        public static string DictionaryToHStoreString(IDictionary dict)
        {
			IEnumerable<string> hstoreItemString = null;
			
            if (dict is Dictionary<string, string>)
				hstoreItemString = (dict as Dictionary<string, string>).Select(x => CreateHStoreItemString(x.Key, x.Value));
            else if (dict is Dictionary<string, int>)
                hstoreItemString = (dict as Dictionary<string, int>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString()));
            else if (dict is Dictionary<string, float>)
                hstoreItemString = (dict as Dictionary<string, float>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString(FormatPoint)));
            else if (dict is Dictionary<string, double>)
                hstoreItemString = (dict as Dictionary<string, double>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString(FormatPoint)));
            else if (dict is Dictionary<string, DateTime>)
                hstoreItemString = (dict as Dictionary<string, DateTime>).Select(x => CreateHStoreItemString(x.Key, x.Value.ToString("yyyy.MM.dd HH:mm:ss.fffffff")));

			if(hstoreItemString != null)
				return string.Join(",", hstoreItemString.Where(x => x != null).ToArray());
			
            else return null;
        }

        public static string ToHStoreString(this IDictionary dict)
        {
            return DictionaryToHStoreString(dict);
        }

        public static string CreateHStoreItemString(string key, string value)
        {
            if (value == null)
            {
                return null;
            }
            else
            {
                value = value.Replace("\"", "\\\"");
            }
            return string.Format("\"{0}\"=>\"{1}\"", key.Replace("\"", "\\\""), value);
        }

        public static string ToArrayString(this IEnumerable array)
        {
            string ret = "{";

            if (array is string[])
            {
                ret += string.Join(",", (array as string[]).Select(x => "\"" + x + "\"").ToArray());
            }
            else if (array is float[])
            {
                ret += string.Join(",", (array as float[]).Select(x => x.ToString(FormatPoint)).ToArray());
            }
            else if (array is double[])
            {
                ret += string.Join(",", (array as double[]).Select(x => x.ToString(FormatPoint)).ToArray());
            }
            else if (array is DateTime[])
            {
                ret += string.Join(",", (array as DateTime[]).Select(x => x.ToString("yyyy.MM.dd HH:mm:ss.fffffff")).ToArray());
            }
            else if (array is int[])
            {
                ret += string.Join(",", (array as int[]).Select(x => x.ToString()).ToArray());
            }
            else if (array is long[])
            {
                ret += string.Join(",", (array as long[]).Select(x => x.ToString()).ToArray());
            }

            ret += "}";
            return ret;
        }

        public static string ListToTsVectorString(List<string> tokens)
        {
            if (tokens.SelectMany(x => x).Any(c => c == ':'))
            {
                var sb = new StringBuilder();
                string weightPrev = "";
                var weightGroup = new List<string>();

                foreach (string token in tokens)
                {
                    int separatorPos = token.IndexOf(':');
                    string weight = separatorPos >= 0 ? token.Substring(separatorPos + 1) : "";

                    if (weight != weightPrev && weightGroup.Any())
                    {
                        sb.Append(CreateTsVectorStringItem(weightGroup, weightPrev)).Append(" || ");
                    }

                    weightGroup.Add(separatorPos >= 0 ? token.Substring(0, separatorPos) : token);
                    weightPrev = weight;
                }
                sb.Append(CreateTsVectorStringItem(weightGroup, weightPrev));

                return sb.ToString();
            }
            else
            {
                return string.Format("to_tsvector('{0}')", string.Join(" ", tokens.Select(x => 
                    (string.IsNullOrEmpty(x) || x.Trim() == "") ? "or" : x).ToArray()));
            }
        }

        private static string CreateTsVectorStringItem(List<string> weightGroup, string weight)
        {
            string ret = "";

            if (string.IsNullOrEmpty(weight))
            {
                if (weightGroup.Any(x => !string.IsNullOrEmpty(x) && x.Trim() != ""))
                {
                    ret = string.Format("to_tsvector('{0}')", string.Join(" ", weightGroup.Select(x =>
                        (string.IsNullOrEmpty(x) || x.Trim() == "") ? "or" : x).ToArray()));
                }
                else ret = string.Format("'{0}'", string.Join(" ", weightGroup.Select(x => "or").ToArray()));
            }
            else
            {
                ret = string.Format("setweight(to_tsvector('{0}'),'{1}')", string.Join(" ", weightGroup.Select(x =>
                            (string.IsNullOrEmpty(x) || x.Trim() == "") ? "or" : x).ToArray()), weight);
            }
            weightGroup.Clear();

            return ret;
        }

        public static string ToTsVectorString(this List<string> tokens)
        {
            return ListToTsVectorString(tokens);
        }

        public static void ExecuteDelete<T>(this IQueryable<T> query)
        {
            var provider = query as IQueryProvider<T>;
            var command = provider.Context.GetCommand(query);

            var parts = command.CommandText.Split(new string[] { "FROM" }, StringSplitOptions.None);
            string sql = "DELETE FROM " + parts.Last();

            command.CommandText = sql;
            command.ExecuteScalar();
        }
    }
}
