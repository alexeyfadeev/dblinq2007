#region MIT license
// 
// MIT license
//
// Copyright (c) 2007-2008 Jiri Moudry, Pascal Craponne
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// 
#endregion
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Globalization;

namespace DbLinq.Util
{
#if !MONO_STRICT
    public
#endif
    static class IDataRecordExtensions
    {
        // please note that sometimes (depending on driver), GetValue() returns DBNull instead of null
        // so at this level, we handle both

        private static NumberFormatInfo formatPoint = new NumberFormatInfo() { NumberDecimalSeparator = "." };

        public static string GetAsString(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return null;
            object o = dataRecord.GetValue(index);
            if (o == null) // this is not supposed to happen
                return null;
            return o.ToString();
        }

        public static bool GetAsBool(this IDataRecord dataRecord, int index)
        {
            object b = dataRecord.GetValue(index);
            return TypeConvert.ToBoolean(b);
        }

        public static bool? GetAsNullableBool(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return null;
            return GetAsBool(dataRecord, index);
        }

        public static char GetAsChar(this IDataRecord dataRecord, int index)
        {
            object c = dataRecord.GetValue(index);
            return TypeConvert.ToChar(c);
        }

        public static char? GetAsNullableChar(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return null;
            return GetAsChar(dataRecord, index);
        }

        public static U GetAsNumeric<U>(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return default(U);
            return GetAsNumeric<U>(dataRecord.GetValue(index));
        }

        public static U? GetAsNullableNumeric<U>(this IDataRecord dataRecord, int index)
            where U : struct
        {
            if (dataRecord.IsDBNull(index))
                return null;
            return GetAsNumeric<U>(dataRecord.GetValue(index));
        }

        private static U GetAsNumeric<U>(object o)
        {
            if (o == null || o is DBNull)
                return default(U);
            return TypeConvert.ToNumber<U>(o);
        }

        public static int GetAsEnum(this IDataRecord dataRecord, Type enumType, int index)
        {
            int enumAsInt = dataRecord.GetAsNumeric<int>(index);
            return enumAsInt;
        }

        public static byte[] GetAsBytes(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return new byte[0];
            object obj = dataRecord.GetValue(index);
            if (obj == null)
                return new byte[0]; //nullable blob?
            byte[] bytes = obj as byte[];
            if (bytes != null)
                return bytes; //works for BLOB field
            Console.WriteLine("GetBytes: received unexpected type:" + obj);
            //return _rdr.GetInt32(index);
            return new byte[0];
        }

        public static System.Data.Linq.Binary GetAsBinary(this IDataRecord dataRecord, int index)
        {
            byte[] bytes = GetAsBytes(dataRecord, index);
            if (bytes.Length == 0)
                return null;
            return new System.Data.Linq.Binary(bytes);
        }

        public static object GetAsObject(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return null;
            object obj = dataRecord.GetValue(index);
            return obj;
        }

        public static Dictionary<string, string> GetAsStringDictionary(this IDataRecord dataRecord, int index)
        {
            string str = GetAsString(dataRecord, index);
            if (string.IsNullOrEmpty(str))
                return new Dictionary<string, string>();

            var items = str.Split(',');
            var pairs = items.Select(x => new
            {
                key = Regex.Replace(x, @"\""(.*?)\""=>\""(.*?)\""", "$1").Trim(),
                value = Regex.Replace(x, @"\""(.*?)\""=>\""(.*?)\""", "$2").Trim()
            });
            return pairs.ToDictionary(x => x.key, x => x.value);
        }

        private static T ConvertFromString<T>(string str)        
        {
            if(typeof(T) == typeof(DateTime))
            {
                return (T)(object)DateTime.ParseExact(str, "yyyy.MM.dd HH:mm:ss.fffffff", null);
            }

            return (T)Convert.ChangeType(str, typeof(T), formatPoint);
        }

        public static Dictionary<string, T> GetAsDictionary<T>(this IDataRecord dataRecord, int index)
        {
            string str = GetAsString(dataRecord, index);
            if (string.IsNullOrEmpty(str))
                return new Dictionary<string, T>();

            var items = str.Split(',');
            var pairs = items.Select(x => new 
            {
                key = Regex.Replace(x, @"\""(.*?)\""=>\""(.*?)\""", "$1").Trim(),
                value = Regex.Replace(x, @"\""(.*?)\""=>\""(.*?)\""", "$2").Trim()
            });
            return pairs.ToDictionary(x => x.key, x => ConvertFromString<T>(x.value));
        }

        public static DateTime GetAsDateTime(this IDataRecord dataRecord, int index)
        {
            // Convert an InvalidCastException (thrown for example by Npgsql when an
            // operation like "SELECT '2012-'::timestamp - NULL" that is perfectly
            // legal on PostgreSQL returns a null instead of a DateTime) into the
            // correct InvalidOperationException.

            if (dataRecord.IsDBNull(index))
                throw new InvalidOperationException("NULL found where DateTime expected");
            return dataRecord.GetDateTime(index);
        }

        public static DateTime? GetAsNullableDateTime(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return null;
            return GetAsDateTime(dataRecord, index);
        }

        public static Guid GetAsGuid(this IDataRecord dataRecord, int index)
        {
            return dataRecord.GetGuid(index);
        }
        public static Guid? GetAsNullableGuid(this IDataRecord dataRecord, int index)
        {
            if (dataRecord.IsDBNull(index))
                return null;
            return GetAsGuid(dataRecord, index);
        }
    }
}
