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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using DbLinq.Util;

using Mono.Options;

namespace DbMetal
{
    [DebuggerDisplay("Parameters from {Provider}, server={Server}")]
    public class Parameters
    {
        /// <summary>
        /// user name for database access
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// user password for database access
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// server host name
        /// </summary>
        public string Server { get; set; }

        /// <summary>
        /// database name
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// This connection string if present overrides User, Password, Server.
        /// Database is always used to generate the specific DataContext name
        /// </summary>
        public string Conn { get; set; }

        /// <summary>
        /// the namespace to put classes into
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// the namespace to put test classes into
        /// </summary>
        public string TestNamespace { get; set; }

        /// <summary>
        /// the namespaces to put entity classes into
        /// </summary>
        public IList<string> AdditionalNamespaces { get; set; }

        /// <summary>
        /// the language to generate classes for
        /// </summary>
        public string Language { get; set; }

        /// <summary>
        /// If present, write out C# code
        /// </summary>
        public string Code => this.ContextName + "Context.cs";

        /// <summary>
        /// if present, write out DBML XML representing the DB
        /// </summary>
        public string Dbml { get; set; }

        /// <summary>
        /// when true, we will call Singularize()/Pluralize() functions.
        /// </summary>
        public bool Pluralize { get; set; }

        /// <summary>
        /// the culture used for word recognition and pluralization
        /// </summary>
        public string Culture { get; set; }

        /// <summary>
        /// load object renamings from an xml file
        /// </summary>
        public string Aliases { get; set; }

        /// <summary>
        /// this is the "input file" parameter
        /// </summary>
        public string SchemaXmlFile { get; set; }

        public bool Schema { get; set; }

        /// <summary>
        /// base class from which all generated entities will inherit
        /// </summary>
        public string EntityBase { get; set; }

        /// <summary>
        /// interfaces to be implemented
        /// </summary>
        public string[] EntityInterfaces { get; set; }

        /// <summary>
        /// extra attributes to be implemented by class members
        /// </summary>
        public IList<string> MemberAttributes { get; set; }

        /// <summary>
        /// generate Equals() and GetHashCode()
        /// </summary>
        public bool GenerateEqualsHash { get; set; }

        /// <summary>
        /// export stored procedures
        /// </summary>
        public bool Sprocs { get; set; }

        /// <summary>
        /// preserve case of database names
        /// </summary>
        public string Case { get; set; }

        /// <summary>
        /// force a Console.ReadKey at end of program.
        /// Useful when running from Studio, so the output window does not disappear
        /// picrap comment: you may use the tool to write output to Visual Studio output window instead of a console window
        /// </summary>
        public bool Readline { get; set; }

        /// <summary>
        /// specifies a provider (which here is a pair or ISchemaLoader and IDbConnection implementors)
        /// </summary>
        public string Provider { get; set; }

        /// <summary>
        /// for fine tuning, we allow to specifiy an ISchemaLoader
        /// </summary>
        public string DbLinqSchemaLoaderProvider { get; set; }

        /// <summary>
        /// for fine tuning, we allow to specifiy an IDbConnection
        /// </summary>
        public string DatabaseConnectionProvider { get; set; }

        /// <summary>
        /// the SQL dialect used by the database
        /// </summary>
        public string SqlDialectType { get; set; }

        /// <summary>
        /// the types to be generated
        /// </summary>
        public IList<string> GenerateTypes { get; set; }

        /// <summary>
        /// if true, put a timestamp comment before the generated code
        /// </summary>
        public bool GenerateTimestamps { get; set; }

        /// <summary>
        /// Show stack traces in error messages, etc., instead of just the message.
        /// </summary>
        public bool Debug { get; set; }

        /// <summary>
        /// DB-context class name
        /// </summary>
        public string ContextName { get; set; }

        /// <summary>
        /// Enable Fast-insert method
        /// </summary>
        public bool MultiInsert { get; set; }

        /// <summary>
        /// Generate IContext interface
        /// </summary>
        public bool IContext { get; set; }

        /// <summary>
        /// Use Z.EntityFramework.Plus extension
        /// </summary>
        public bool BulkExtensions { get; set; }

        /// <summary>
        /// Separate folder for entity classes
        /// </summary>
        public string EntityFolder { get; set; }

        /// <summary>
        /// Tables to ignore
        /// </summary>
        public List<string> IgnoreTables { get; set; }

        /// <summary>
        /// If this parameter specified, only these tables will be included
        /// </summary>
        public List<string> IncludeOnlyTables { get; set; }

        /// <summary>
        /// Schemes to ignore
        /// </summary>
        public List<string> IgnoreSchemes { get; set; }

        TextWriter log;
        public TextWriter Log
        {
            get { return log ?? Console.Out; }
            set { log = value; }
        }

        protected OptionSet Options;

        public Parameters()
        {
            Schema = true;
            Culture = "en";
            GenerateTypes = new List<string>();
            MemberAttributes = new List<string>();
            GenerateTimestamps = true;
            EntityInterfaces = new []{ "INotifyPropertyChanging", "INotifyPropertyChanged" };
        }

        #region Help

        public void WriteHelp()
        {
            WriteHeader(); // includes a WriteLine()
            WriteSyntax();
            WriteLine();
            WriteSummary();
            WriteLine();
            Options.WriteOptionDescriptions(Log);
            WriteLine();
            WriteExamples();
        }

        bool headerWritten;

        /// <summary>
        /// Writes the application header
        /// </summary>
        public void WriteHeader()
        {
            if (!headerWritten)
            {
                WriteHeaderContents();
                WriteLine();
                headerWritten = true;
            }
        }

        protected void WriteHeaderContents()
        {
            var version = ApplicationVersion;
            Write("DbLinq Database mapping generator 2008 version {0}.{1}", version.Major, version.Minor);
            Write("for Microsoft (R) .NET Framework version 3.5");
            Write("Distributed under the MIT licence (http://linq.to/db/license)");
        }

        /// <summary>
        /// Writes a small summary
        /// </summary>
        public void WriteSummary()
        {
            Write("  Generates code and mapping for DbLinq. SqlMetal can:");
            Write("  - Generate source code and mapping attributes or a mapping file from a database.");
            Write("  - Generate an intermediate dbml file for customization from the database.");
            Write("  - Generate code and mapping attributes or mapping file from a dbml file.");
        }

        public void WriteSyntax()
        {
            var syntax = new StringBuilder();
            syntax.AppendFormat("{0} [OPTIONS] [<DBML INPUT FILE>]", ApplicationName);
            Write(syntax.ToString());
        }

        /// <summary>
        /// Writes examples
        /// </summary>
        public void WriteExamples()
        {
        }

        /// <summary>
        /// Outputs a formatted string to the console.
        /// We're not using the ILogger here, since we want console output.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void Write(string format, params object[] args)
        {
            Output.WriteLine(Log, OutputLevel.Information, format, args);
        }

        /// <summary>
        /// Outputs an empty line
        /// </summary>
        public void WriteLine()
        {
            Output.WriteLine(Log, OutputLevel.Information, string.Empty);
        }

        /// <summary>
        /// Returns the application (assembly) name (without extension)
        /// </summary>
        protected static string ApplicationName
        {
            get
            {
                return Assembly.GetEntryAssembly().GetName().Name;
            }
        }

        /// <summary>
        /// Returns the application (assembly) version
        /// </summary>
        protected static Version ApplicationVersion
        {
            get
            {
                // Assembly.GetEntryAssembly() is null when loading from the
                // non-default AppDomain.
                var a = Assembly.GetEntryAssembly();
                return a != null ? a.GetName().Version : new Version();
            }
        }

        #endregion
    }
}
