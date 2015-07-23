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
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;

using DbLinq.Data.Linq.Sugar.Expressions;

namespace DbLinq.Data.Linq.Sugar.Expressions
{
    [DebuggerDisplay("ObjectInputParameterExpression")]
#if !MONO_STRICT
    public
#endif
    class ObjectInputParameterExpression : MutableExpression
    {
        public const ExpressionType ExpressionType = (ExpressionType)CustomExpressionType.ObjectInputParameter;

        public string Alias { get; private set; }
        public Type ValueType { get; private set; }

        private static NumberFormatInfo formatPoint = new NumberFormatInfo() { NumberDecimalSeparator = "." };

        private readonly Delegate getValueDelegate;
        /// <summary>
        /// Returns the outer parameter value
        /// </summary>
        /// <returns></returns>
        public object GetValue(object o)
        {
            var ret = getValueDelegate.DynamicInvoke(o);

            // For hstore data type
            if (ret is IDictionary)
            {
                if(ret is Dictionary<string, string>)
                    return string.Join(",", (ret as Dictionary<string, string>).Select(x => x.Key + "=>" + x.Value).ToArray());
                else if (ret is Dictionary<string, int>)
                    return string.Join(",", (ret as Dictionary<string, int>).Select(x => x.Key + "=>" + x.Value.ToString()).ToArray());
                else if (ret is Dictionary<string, float>)
                    return string.Join(",", (ret as Dictionary<string, float>).Select(x => x.Key + "=>" + x.Value.ToString(formatPoint)).ToArray());
                else if (ret is Dictionary<string, double>)
                    return string.Join(",", (ret as Dictionary<string, double>).Select(x => x.Key + "=>" + x.Value.ToString(formatPoint)).ToArray());
                else if (ret is Dictionary<string, DateTime>)
                    return string.Join(",", (ret as Dictionary<string, DateTime>).Select(x => x.Key + "=>" + x.Value.ToString("yyyy.MM.dd HH:mm:ss.fffffff")).ToArray());
            }

            return ret;
        }

        public ObjectInputParameterExpression(LambdaExpression lambda, Type valueType, string alias)
            : base(ExpressionType, lambda.Type)
        {
            if (lambda.Parameters.Count != 1)
                throw Error.BadArgument("S0055: Lambda must take 1 argument");
            getValueDelegate = lambda.Compile();
            Alias = alias;
            ValueType = valueType;
        }
    }
}
