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
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;

using Microsoft.CSharp;
using Microsoft.VisualBasic;

using DbLinq.Schema.Dbml;
using DbLinq.Schema.Dbml.Adapter;
using DbLinq.Language;
using DbLinq.Schema.Implementation;

namespace DbMetal.Generator
{
#if !MONO_STRICT
    public
#endif
    class CodeDomGenerator : ICodeGenerator
    {
        CodeDomProvider Provider { get; set; }
        ILanguageWords LanguageWords { get; set; }
        NameFormatter NameFormatter { get; set; }

        // Provided only for Processor.EnumerateCodeGenerators().  DO NOT USE.
        public CodeDomGenerator()
        {
            NameFormatter = new NameFormatter();
        }

        public CodeDomGenerator(CodeDomProvider provider)
        {
            this.Provider = provider;
            NameFormatter = new NameFormatter();
        }

        public string LanguageCode {
            get { return "*"; }
        }

        public string Extension {
            get { return "*"; }
        }

        public bool NetCoreMode { get; set; }

        public string EntityFolder { get; set; }

        public void CheckLanguageWords(string cultureName)
        {
            if (LanguageWords == null)
            {
                LanguageWords = NameFormatter.GetLanguageWords(new CultureInfo(cultureName));
            }
        }

        public static CodeDomGenerator CreateFromFileExtension(string extension)
        {
            return CreateFromLanguage(CodeDomProvider.GetLanguageFromExtension(extension));
        }

        public static CodeDomGenerator CreateFromLanguage(string language)
        {
            return new CodeDomGenerator(CodeDomProvider.CreateProvider(language));
        }

        public void Write(TextWriter textWriter, Database dbSchema, GenerationContext context)
        {
        }

        public void WriteEf(TextWriter textWriter, Database dbSchema, Table table, GenerationContext context)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                GenerateEfDomModel(dbSchema, table), textWriter,
                new CodeGeneratorOptions
                    {
                        BracingStyle = "C",
                        IndentString = "\t",
                    });
        }

        public void WriteEfContext(TextWriter textWriter,
            Database dbSchema,
            GenerationContext context,
            string contextName,
            string provider)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                GenerateEfContextDomModel(dbSchema, contextName, provider.ToLower()), textWriter,
                new CodeGeneratorOptions()
                    {
                        BracingStyle = "C",
                        IndentString = "\t",
                    });
        }

        public void WriteIRepository(TextWriter textWriter, Database dbSchema, GenerationContext context, bool bulkExtensions)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                this.GenerateIRepositoryDomModel(dbSchema, bulkExtensions),
                textWriter,
                new CodeGeneratorOptions()
                {
                    BracingStyle = "C",
                    IndentString = "\t",
                });
        }

        public void WriteRepository(TextWriter textWriter, Database dbSchema, GenerationContext context, bool bulkExtensions)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                this.GenerateRepositoryDomModel(dbSchema, bulkExtensions),
                textWriter,
                new CodeGeneratorOptions()
                    {
                        BracingStyle = "C",
                        IndentString = "\t",
                    });
        }

        public void WriteMockRepository(TextWriter textWriter, Database dbSchema, GenerationContext context, bool bulkExtensions)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                this.GenerateMockRepositoryDomModel(dbSchema, bulkExtensions),
                textWriter,
                new CodeGeneratorOptions()
                {
                    BracingStyle = "C",
                    IndentString = "\t",
                });
        }

        private CodeTypeMember CreatePartialMethod(string methodName, params CodeParameterDeclarationExpression[] parameters)
        {
            string prototype = null;
            if (Provider is CSharpCodeProvider)
            {
                prototype =
                    "\t\tpartial void {0}({1});" + Environment.NewLine +
                    "\t\t";
            }
            else if (Provider is VBCodeProvider)
            {
                prototype =
                    "\t\tPartial Private Sub {0}({1})" + Environment.NewLine +
                    "\t\tEnd Sub" + Environment.NewLine +
                    "\t\t";
            }

            if (prototype == null)
            {
                var method = new CodeMemberMethod() {
                    Name = methodName,
                };
                method.Parameters.AddRange(parameters);
                return method;
            }

            var methodDecl = new StringWriter();
            var gen = Provider.CreateGenerator(methodDecl);

            bool comma = false;
            foreach (var p in parameters)
            {
                if (comma)
                    methodDecl.Write(", ");
                comma = true;
                gen.GenerateCodeFromExpression(p, methodDecl, null);
            }
            return new CodeSnippetTypeMember(string.Format(prototype, methodName, methodDecl.ToString()));
        }

        protected GenerationContext Context { get; set; }

        protected virtual CodeNamespace GenerateEfDomModel(Database database, Table table)
        {
            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;
            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpaceName = $"{nameSpaceName}.{this.EntityFolder}";
            }

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport("System.ComponentModel.DataAnnotations"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.ComponentModel.DataAnnotations.Schema"));

            nameSpace.Types.Add(this.GenerateEfClass(table, database));

            return nameSpace;
        }

        protected virtual CodeNamespace GenerateIRepositoryDomModel(Database database, bool bulkExtensions)
        {
            CheckLanguageWords(Context.Parameters.Culture);

            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport("System"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));

            if (bulkExtensions)
            {
                nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq.Expressions"));
            }

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            var iface = new CodeTypeDeclaration("I" + database.Class.Replace("Context", "Repository"))
            {
                IsInterface = true,
                IsPartial = true
            };

            iface.BaseTypes.Add(new CodeTypeReference("IDisposable"));

            // Tables
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string name = this.GetTableNamePluralized(table.Member);

                var field = new CodeMemberProperty
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = name,
                    Type = new CodeTypeReference("IQueryable", tableType),
                };

                field.HasGet = true;

                field.Comments.Add(new CodeCommentStatement($"<summary> {name} </summary>", true));

                iface.Members.Add(field);
            }

            var voidTypeRef = new CodeTypeReference(typeof(void));

            // Add methods
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                var method = new CodeMemberMethod()
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "Add" + table.Member,
                    ReturnType = voidTypeRef
                };

                method.Comments.Add(new CodeCommentStatement($"<summary> Add {table.Member} </summary>", true));

                method.Parameters.Add(new CodeParameterDeclarationExpression(tableType, GetLowerCamelCase(table.Member)));

                iface.Members.Add(method);
            }

            // AddRange methods
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string paramName = GetLowerCamelCase(this.GetTableNamePluralized(table.Member));

                var method = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "AddRange" + this.GetTableNamePluralized(table.Member),
                    ReturnType = voidTypeRef
                };

                method.Parameters.Add(new CodeParameterDeclarationExpression(new CodeTypeReference("IEnumerable", tableType), paramName));

                method.Comments.Add(new CodeCommentStatement($"<summary> Add range of {table.Member} </summary>", true));

                iface.Members.Add(method);
            }

            // Get methods (by PK)
            foreach (Table table in database.Tables)
            {
                var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();
                if (!pkColumns.Any()) continue;

                var tableType = new CodeTypeReference(table.Type.Name);

                var method = new CodeMemberMethod()
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "Get" + table.Member,
                    ReturnType = tableType
                };

                foreach (var col in pkColumns)
                {
                    method.Parameters.Add(new CodeParameterDeclarationExpression(ToCodeTypeReference(col), GetStorageFieldName(col).Replace("_", "")));
                }

                method.Comments.Add(new CodeCommentStatement($"<summary> Get {table.Member} </summary>", true));

                iface.Members.Add(method);
            }

            if (bulkExtensions)
            {
                // Bulk delete methods (by PK)
                foreach (Table table in database.Tables)
                {
                    var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();

                    if (!pkColumns.Any()) continue;

                    var method = new CodeMemberMethod()
                    {
                        Attributes =
                            MemberAttributes.Public | MemberAttributes.Final,
                        Name = "BulkDelete" + table.Member,
                        ReturnType = voidTypeRef
                    };

                    foreach (var col in pkColumns)
                    {
                        method.Parameters.Add(
                            new CodeParameterDeclarationExpression(
                                ToCodeTypeReference(col),
                                GetStorageFieldName(col).Replace("_", "")));
                    }

                    method.Comments.Add(
                        new CodeCommentStatement($"<summary> Bulk delete {table.Member} </summary>", true));

                    iface.Members.Add(method);
                }

                // Bulk delete methods (by expression)
                foreach (Table table in database.Tables)
                {
                    var tableType = new CodeTypeReference(table.Type.Name + ", bool");

                    var name = this.GetTableNamePluralized(table.Member);

                    var method = new CodeMemberMethod
                    {
                        Attributes = MemberAttributes.Public | MemberAttributes.Final,
                        Name = "BulkDelete" + name,
                        ReturnType = voidTypeRef
                    };

                    method.Parameters.Add(
                        new CodeParameterDeclarationExpression(
                            new CodeTypeReference("Expression", new CodeTypeReference("Func", tableType)),
                            "filerExpression"));

                    method.Comments.Add(new CodeCommentStatement($"<summary> Bulk delete {name} </summary>", true));

                    iface.Members.Add(method);
                }
            }

            var methodSubmit = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "SaveChanges",
                ReturnType = voidTypeRef
            };

            iface.Members.Add(methodSubmit);

            nameSpace.Types.Add(iface);

            return nameSpace;
        }

        protected virtual CodeNamespace GenerateRepositoryDomModel(Database database, bool bulkExtensions)
        {
            this.CheckLanguageWords(this.Context.Parameters.Culture);

            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            if (bulkExtensions)
            {
                nameSpace.Imports.Add(new CodeNamespaceImport("System"));
            }

            nameSpace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));            

            if (bulkExtensions)
            {
                nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq.Expressions"));
                nameSpace.Imports.Add(new CodeNamespaceImport("Z.EntityFramework.Plus"));
            }

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            var cls = new CodeTypeDeclaration(database.Class.Replace("Context", "Repository"))
            {
                IsPartial = true,
                Attributes = MemberAttributes.Public | MemberAttributes.Final
            };

            cls.Comments.Add(new CodeCommentStatement("<summary> Database repository </summary>", true));

            cls.BaseTypes.Add(new CodeTypeReference("I" + cls.Name));

            var contextType = new CodeTypeReference(database.Class.Replace("Context", "DbContext"));

            // Constructor
            var constructor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public,
                Parameters = { new CodeParameterDeclarationExpression(contextType, "context") },
            };

            constructor.Comments.Add(new CodeCommentStatement("<summary> Database repository constructor </summary>", true));

            var contextRef = new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), "Context");

            var statementAssign = new CodeAssignStatement(
                contextRef,
                new CodeArgumentReferenceExpression("context"));

            constructor.Statements.Add(statementAssign);

            cls.Members.Add(constructor);

            // Context property
            var contextField = new CodeMemberField
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "Context",
                Type = contextType
            };

            contextField.Comments.Add(new CodeCommentStatement("<summary> Database context </summary>", true));

            contextField.Name += " { get; set; }";

            cls.Members.Add(contextField);

            // Tables
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string name = this.GetTableNamePluralized(table.Member);
                
                var field = new CodeMemberField
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = name,
                    Type = new CodeTypeReference("IQueryable", tableType),
                };

                field.Comments.Add(new CodeCommentStatement($"<summary> {name} </summary>", true));

                field.Name += $" => this.Context.{name}.AsQueryable()";

                cls.Members.Add(field);
            }

            var voidTypeRef = new CodeTypeReference(typeof(void));

            // Add methods
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string paramName = GetLowerCamelCase(table.Member);

                var method = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "Add" + table.Member,
                    ReturnType = voidTypeRef
                };

                method.Parameters.Add(new CodeParameterDeclarationExpression(tableType, paramName));

                var prop = new CodePropertyReferenceExpression(contextRef, this.GetTableNamePluralized(table.Member));
                method.Statements.Add(new CodeMethodInvokeExpression(prop, "Add", new CodeVariableReferenceExpression(paramName)));

                method.Comments.Add(new CodeCommentStatement($"<summary> Add {table.Member} </summary>", true));

                cls.Members.Add(method);
            }

            // AddRange methods
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string paramName = GetLowerCamelCase(this.GetTableNamePluralized(table.Member));

                var method = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "AddRange" + this.GetTableNamePluralized(table.Member),
                    ReturnType = voidTypeRef
                };
                                
                method.Parameters.Add(new CodeParameterDeclarationExpression(new CodeTypeReference("IEnumerable", tableType), paramName));

                var prop = new CodePropertyReferenceExpression(contextRef, this.GetTableNamePluralized(table.Member));
                method.Statements.Add(new CodeMethodInvokeExpression(prop, "AddRange", new CodeVariableReferenceExpression(paramName)));

                method.Comments.Add(new CodeCommentStatement($"<summary> Add range of {table.Member} </summary>", true));

                cls.Members.Add(method);
            }

            // Get methods (by PK)
            foreach (Table table in database.Tables)
            {
                var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();

                if (!pkColumns.Any()) continue;

                var tableType = new CodeTypeReference(table.Type.Name);

                var method = new CodeMemberMethod()
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "Get" + table.Member,
                    ReturnType = tableType
                };

                foreach (var col in pkColumns)
                {
                    method.Parameters.Add(new CodeParameterDeclarationExpression(ToCodeTypeReference(col), GetStorageFieldName(col).Replace("_", "")));
                }

                var prop = new CodePropertyReferenceExpression(contextRef, this.GetTableNamePluralized(table.Member));
                var statement = new CodeMethodInvokeExpression(prop, "FirstOrDefault", new CodeSnippetExpression(
                    "x => " + string.Join(" && ", pkColumns.Select(c => "x." + c.Member + " == " +
                                                                        GetStorageFieldName(c).Replace("_", "")).ToArray())));
                method.Statements.Add(new CodeMethodReturnStatement(statement));

                method.Comments.Add(new CodeCommentStatement($"<summary> Get {table.Member} </summary>", true));

                cls.Members.Add(method);
            }

            if (bulkExtensions)
            {
                // Bulk delete methods (by PK)
                foreach (Table table in database.Tables)
                {
                    var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();

                    if (!pkColumns.Any()) continue;

                    var method = new CodeMemberMethod()
                    {
                        Attributes =
                            MemberAttributes.Public | MemberAttributes.Final,
                        Name = "BulkDelete" + table.Member,
                        ReturnType = voidTypeRef
                    };

                    foreach (var col in pkColumns)
                    {
                        method.Parameters.Add(
                            new CodeParameterDeclarationExpression(
                                ToCodeTypeReference(col),
                                GetStorageFieldName(col).Replace("_", "")));
                    }

                    var prop = new CodePropertyReferenceExpression(
                        contextRef,
                        this.GetTableNamePluralized(table.Member));
                    var statement = new CodeMethodInvokeExpression(
                        prop,
                        "Where",
                        new CodeSnippetExpression(
                            "x => " + string.Join(
                                " && ",
                                pkColumns.Select(
                                        c => "x." + c.Member + " == " + GetStorageFieldName(c).Replace("_", ""))
                                    .ToArray())));

                    method.Statements.Add(new CodeMethodInvokeExpression(statement, "Delete"));

                    method.Comments.Add(
                        new CodeCommentStatement($"<summary> Bulk delete {table.Member} </summary>", true));

                    cls.Members.Add(method);
                }

                // Bulk delete methods (by expression)
                foreach (Table table in database.Tables)
                {
                    var tableType = new CodeTypeReference(table.Type.Name + ", bool");

                    var name = this.GetTableNamePluralized(table.Member);

                    var method = new CodeMemberMethod
                    {
                        Attributes = MemberAttributes.Public | MemberAttributes.Final,
                        Name = "BulkDelete" + name,
                        ReturnType = voidTypeRef
                    };

                    method.Parameters.Add(
                        new CodeParameterDeclarationExpression(
                            new CodeTypeReference("Expression", new CodeTypeReference("Func", tableType)),
                            "filerExpression"));

                    var prop = new CodePropertyReferenceExpression(
                        contextRef,
                        this.GetTableNamePluralized(table.Member));
                    var statement = new CodeMethodInvokeExpression(
                        prop,
                        "Where",
                        new CodeVariableReferenceExpression("filerExpression"));

                    method.Statements.Add(new CodeMethodInvokeExpression(statement, "Delete"));

                    method.Comments.Add(new CodeCommentStatement($"<summary> Bulk delete {name} </summary>", true));

                    cls.Members.Add(method);
                }
            }

            // SaveChanges method
            var methodSubmit = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "SaveChanges",
                ReturnType = voidTypeRef
            };

            var mtd = new CodeMethodInvokeExpression(contextRef, "SaveChanges");
            methodSubmit.Statements.Add(mtd);

            methodSubmit.Comments.Add(new CodeCommentStatement($"<summary> Save changes </summary>", true));

            cls.Members.Add(methodSubmit);

            // Dispose method
            var methodDispose = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "Dispose",
                ReturnType = voidTypeRef
            };

            mtd = new CodeMethodInvokeExpression(contextRef, "Dispose");
            methodDispose.Statements.Add(mtd);

            methodDispose.Comments.Add(new CodeCommentStatement($"<summary> Dispose </summary>", true));

            cls.Members.Add(methodDispose);

            nameSpace.Types.Add(cls);

            return nameSpace;
        }

        protected virtual CodeNamespace GenerateEfContextDomModel(Database database, string contextName, string provider)
        {
            this.CheckLanguageWords(this.Context.Parameters.Culture);

            string nameSpaceName = this.Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport(this.NetCoreMode ? "Microsoft.EntityFrameworkCore" : "System.Data.Entity"));

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            var cls = new CodeTypeDeclaration(contextName + "DbContext")
            {
                IsPartial = true,
                Attributes = MemberAttributes.Public | MemberAttributes.Final
            };

            cls.Comments.Add(new CodeCommentStatement("<summary> Database context </summary>", true));

            cls.BaseTypes.Add(new CodeTypeReference("DbContext"));

            // Constructors
            var constructor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public,
                Parameters = { new CodeParameterDeclarationExpression(typeof(string), "connectionString") },
            };

            if (!this.NetCoreMode)
            {
                constructor.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("connectionString"));
            }
            else
            {
                var fieldConnStr = new CodeMemberField
                {
                    Attributes = MemberAttributes.Final | MemberAttributes.Private,
                    Name = "connectionString",
                    Type = new CodeTypeReference(typeof(string))
                };

                cls.Members.Add(fieldConnStr);

                var assignStatement = new CodeAssignStatement(new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), "connectionString"),
                    new CodeArgumentReferenceExpression("connectionString"));

                constructor.Statements.Add(assignStatement);
            }

            constructor.Comments.Add(new CodeCommentStatement("<summary> Database context constructor </summary>", true));
            cls.Members.Add(constructor);

            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string name = this.GetTableNamePluralized(table.Member);

                var field = new CodeMemberField
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = name,
                    Type = new CodeTypeReference("DbSet", tableType),
                };

                field.Comments.Add(new CodeCommentStatement($"<summary> {name} </summary>", true));

                field.Name += " { get; set; }";

                cls.Members.Add(field);
            }

            if (this.NetCoreMode)
            {
                // OnModelCreating
                var complexKeyTables = database.Tables
                    .Select(x => new { table = x, pkCols = x.Type.Columns.Where(col => col.IsPrimaryKey).ToList() })
                    .Where(x => x.pkCols.Count > 1).ToList();

                if (complexKeyTables.Any())
                {
                    var method = new CodeMemberMethod
                    {
                        Attributes = MemberAttributes.Family | MemberAttributes.Override,
                        Name = "OnModelCreating",
                        ReturnType = new CodeTypeReference(typeof(void)),
                        Parameters = { new CodeParameterDeclarationExpression(new CodeTypeReference("ModelBuilder"), "modelBuilder") }
                    };

                    foreach (var item in complexKeyTables)
                    {
                        var invk = new CodeMethodInvokeExpression(new CodeArgumentReferenceExpression("modelBuilder"),
                            $"Entity<{item.table.Member}>");

                        var statement = new CodeMethodInvokeExpression(invk, "HasKey", new CodeSnippetExpression(
                            $"x => new {{ {string.Join(", ", item.pkCols.Select(x => "x." + x.Member))} }}"));

                        method.Statements.Add(statement);
                    }

                    method.Comments.Add(new CodeCommentStatement("<summary> On model creating </summary>", true));

                    cls.Members.Add(method);
                }

                // OnConfiguring
                var methodConf = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Family | MemberAttributes.Override,
                    Name = "OnConfiguring",
                    ReturnType = new CodeTypeReference(typeof(void)),
                    Parameters = { new CodeParameterDeclarationExpression(new CodeTypeReference("DbContextOptionsBuilder"), "optionsBuilder") }
                };

                var coreProviders = new Dictionary<string, string>
                    {
                        { "SqlServer", "UseSqlServer" },
                        { "PostgreSQL", "UseNpgsql" },
                        { "MySQL", "UseMySql" },
                        { "SQLite", "UseSqlite" },
                        { "SqlCe", "UseSqlCe" },
                        { "Firebird", "UseFirebirdSql" }
                    }
                    .ToDictionary(x => x.Key.ToLower(), x => x.Value);

                if (!coreProviders.ContainsKey(provider))
                {
                    throw new NotSupportedException("Provider not supported: " + provider);
                }

                methodConf.Statements.Add(
                    new CodeMethodInvokeExpression(
                        new CodeArgumentReferenceExpression("optionsBuilder"),
                        coreProviders[provider],
                        new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), "connectionString")));

                methodConf.Comments.Add(new CodeCommentStatement("<summary> On configuring </summary>", true));

                cls.Members.Add(methodConf);
            }

            nameSpace.Types.Add(cls);

            return nameSpace;
        }

        protected string GetTableNamePluralized(string name)
        {
            var nameWords = NameFormatter.ExtractWords(LanguageWords, name, DbLinq.Schema.WordsExtraction.FromCase);
            string pluralized = LanguageWords.Pluralize(nameWords.Last());

            if(pluralized == nameWords.Last() && !pluralized.EndsWith("s"))
            {
                pluralized += "s";
            }

            nameWords.RemoveAt(nameWords.Count - 1);

            string ret = string.Join("", nameWords.ToArray()) + pluralized;
            return ret;
        }

        protected virtual CodeNamespace GenerateMockRepositoryDomModel(Database database, bool bulkExtensions)
        {
            CheckLanguageWords(Context.Parameters.Culture);

            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName); ;

            nameSpace.Imports.Add(new CodeNamespaceImport("System"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));

            if (bulkExtensions)
            {
                nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq.Expressions"));
            }

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            var cls = new CodeTypeDeclaration("Mock" + database.Class.Replace("Context", "Repository"))
            {
                IsClass = true,
                IsPartial = true
            };

            cls.BaseTypes.Add(new CodeTypeReference("I" + database.Class.Replace("Context", "Repository")));
            cls.BaseTypes.Add(new CodeTypeReference("IDisposable"));

            var privateListNames = new Dictionary<Table, string>();

            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                privateListNames.Add(table, GetLowerCamelCase(GetTableNamePluralized(table.Member)));

                var field = new CodeMemberField
                {
                    Attributes = MemberAttributes.Final | MemberAttributes.Private,
                    Name = $"{privateListNames[table]} = new List<{table.Type.Name}>()",
                    Type = new CodeTypeReference("List", tableType),
                };

                cls.Members.Add(field);
            }
            
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                var field = new CodeMemberProperty
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = GetTableNamePluralized(table.Member),
                    Type = new CodeTypeReference("IQueryable", tableType),
                };
                field.HasGet = true;

                var prop = new CodeVariableReferenceExpression(privateListNames[table]);
                field.GetStatements.Add(new CodeMethodReturnStatement(new CodeMethodInvokeExpression(prop, "AsQueryable")));

                cls.Members.Add(field);
            }
            
            var voidTypeRef = new CodeTypeReference(typeof(void));

            var integerTypes = new List<System.Type> { typeof(int), typeof(Int16), typeof(Int64), typeof(UInt16), typeof(uint), typeof(UInt64) };

            // Add methods
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string paramName = GetLowerCamelCase(table.Member);

                var method = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "Add" + table.Member,
                    ReturnType = voidTypeRef
                };

                method.Parameters.Add(new CodeParameterDeclarationExpression(tableType, paramName));

                var listField = new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), privateListNames[table]);

                var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();
                if (pkColumns.Count == 1)
                {
                    // Primary key auto-increment

                    var pkColumn = pkColumns.First();
                    var pkType = System.Type.GetType(pkColumn.Type);
                    if(integerTypes.Contains(pkType))
                    {                        
                        var maxStatement = new CodeMethodInvokeExpression(listField, "Max", new CodeSnippetExpression(
                            "x => x." + pkColumn.Member));

                        var pkProperty = new CodeFieldReferenceExpression(new CodeVariableReferenceExpression(paramName), pkColumn.Member);

                        var assignStatement = new CodeAssignStatement(pkProperty, new CodeBinaryOperatorExpression(maxStatement,
                            CodeBinaryOperatorType.Add, new CodePrimitiveExpression(1)));

                        var innerIfStatement = new CodeConditionStatement(new CodeMethodInvokeExpression(listField, "Any"),
                            new CodeStatement[] { assignStatement }, new CodeStatement[] { new CodeAssignStatement(pkProperty, new CodePrimitiveExpression(1)) });

                        var ifStatement = new CodeConditionStatement(new CodeBinaryOperatorExpression(pkProperty, CodeBinaryOperatorType.LessThan, new CodePrimitiveExpression(1)),
                            new CodeStatement[] { innerIfStatement });

                        method.Statements.Add(ifStatement);
                    }
                }

                var addStatement = new CodeMethodInvokeExpression(listField, "Add", new CodeVariableReferenceExpression(paramName));

                method.Statements.Add(addStatement);

                cls.Members.Add(method);
            }

            // AddRange methods
            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string paramName = GetLowerCamelCase(this.GetTableNamePluralized(table.Member));

                var method = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "AddRange" + this.GetTableNamePluralized(table.Member),
                    ReturnType = voidTypeRef
                };

                method.Parameters.Add(new CodeParameterDeclarationExpression(new CodeTypeReference("IEnumerable", tableType), paramName));

                // Declares and initializes an integer var i
                var iterator = new CodeVariableDeclarationStatement(typeof(int), "i", new CodePrimitiveExpression(0));

                // Creates a for loop with i
                var forLoop = new CodeIterationStatement(
                    new CodeAssignStatement(new CodeVariableReferenceExpression("i"), new CodePrimitiveExpression(0)),
                    new CodeBinaryOperatorExpression(new CodeVariableReferenceExpression("i"),
                        CodeBinaryOperatorType.LessThan,
                        new CodeMethodInvokeExpression(new CodeArgumentReferenceExpression(paramName), "Count")),
                    new CodeAssignStatement(new CodeVariableReferenceExpression("i"),
                        new CodeBinaryOperatorExpression(
                            new CodeVariableReferenceExpression("i"),
                            CodeBinaryOperatorType.Add,
                            new CodePrimitiveExpression(1))),
                    new CodeStatement[] 
                    {
                        new CodeExpressionStatement(new CodeMethodInvokeExpression(
                            new CodeThisReferenceExpression(),
                            "Add" + table.Member,
                            new CodeMethodInvokeExpression(new CodeArgumentReferenceExpression(paramName),
                                "ElementAt",
                                new CodeVariableReferenceExpression("i"))) ) 
                    });

                method.Statements.Add(iterator);
                method.Statements.Add(forLoop);

                method.Comments.Add(new CodeCommentStatement($"<summary> Add range of {table.Member} </summary>", true));

                cls.Members.Add(method);
            }

            // Get methods (by PK)
            foreach (Table table in database.Tables)
            {
                var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();
                if (!pkColumns.Any()) continue;

                var tableType = new CodeTypeReference(table.Type.Name);

                var method = new CodeMemberMethod()
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = "Get" + table.Member,
                    ReturnType = tableType
                };

                foreach (var col in pkColumns)
                {
                    method.Parameters.Add(new CodeParameterDeclarationExpression(ToCodeTypeReference(col), GetStorageFieldName(col).Replace("_", "")));
                }

                var listField = new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), privateListNames[table]);
                var statement = new CodeMethodInvokeExpression(listField, "FirstOrDefault", new CodeSnippetExpression(
                    "x => " + string.Join(" && ", pkColumns.Select(c => "x." + c.Member + " == " +
                    GetStorageFieldName(c).Replace("_", "")).ToArray())));

                method.Statements.Add(new CodeMethodReturnStatement(statement));

                method.Comments.Add(new CodeCommentStatement($"<summary> Get {table.Member} </summary>", true));

                cls.Members.Add(method);
            }

            if (bulkExtensions)
            {
                // Delete methods (by PK)
                foreach (Table table in database.Tables)
                {
                    var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();
                    if (!pkColumns.Any()) continue;

                    var tableType = new CodeTypeReference(table.Type.Name);
                    string paramName = GetLowerCamelCase(table.Member);

                    var method = new CodeMemberMethod
                    {
                        Attributes =
                            MemberAttributes.Public | MemberAttributes.Final,
                        Name = "BulkDelete" + table.Member,
                        ReturnType = voidTypeRef
                    };

                    foreach (var col in pkColumns)
                    {
                        method.Parameters.Add(
                            new CodeParameterDeclarationExpression(
                                ToCodeTypeReference(col),
                                GetStorageFieldName(col).Replace("_", "")));
                    }

                    var paramExpression = new CodeVariableReferenceExpression(paramName);

                    // Getting an item by primary key fields
                    method.Statements.Add(new CodeVariableDeclarationStatement(tableType, paramName));
                    method.Statements.Add(
                        new CodeAssignStatement(
                            paramExpression,
                            new CodeMethodInvokeExpression(
                                new CodeThisReferenceExpression(),
                                "Get" + table.Member,
                                pkColumns.Select(
                                        c => new CodeVariableReferenceExpression(
                                            GetStorageFieldName(c).Replace("_", "")))
                                    .ToArray())));

                    var deleteStatement = new CodeExpressionStatement(
                        new CodeMethodInvokeExpression(
                            new CodeFieldReferenceExpression(
                                new CodeThisReferenceExpression(),
                                privateListNames[table]),
                            "Remove",
                            paramExpression));

                    var ifNullStatement = new CodeConditionStatement(
                        new CodeBinaryOperatorExpression(
                            paramExpression,
                            CodeBinaryOperatorType.IdentityInequality,
                            new CodePrimitiveExpression(null)),
                        new CodeStatement[] { deleteStatement });

                    method.Statements.Add(ifNullStatement);

                    method.Comments.Add(
                        new CodeCommentStatement($"<summary> Bulk delete {table.Member} </summary>", true));

                    cls.Members.Add(method);
                }

                // Bulk delete methods (by expression)
                foreach (Table table in database.Tables)
                {
                    var tableType = new CodeTypeReference(table.Type.Name + ", bool");

                    var name = this.GetTableNamePluralized(table.Member);

                    var method = new CodeMemberMethod
                    {
                        Attributes = MemberAttributes.Public | MemberAttributes.Final,
                        Name = "BulkDelete" + name,
                        ReturnType = voidTypeRef
                    };

                    method.Parameters.Add(
                        new CodeParameterDeclarationExpression(
                            new CodeTypeReference("Expression", new CodeTypeReference("Func", tableType)),
                            "filerExpression"));

                    var listField = new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), name);

                    var filtered = new CodeMethodInvokeExpression(
                        new CodeMethodInvokeExpression(
                            listField,
                            "Where",
                            new CodeVariableReferenceExpression("filerExpression")),
                        "ToList");

                    var itemsDecl = new CodeVariableDeclarationStatement(
                        new CodeTypeReference("var"),
                        "items",
                        filtered);

                    var iterator = new CodeVariableDeclarationStatement(
                        typeof(int),
                        "i",
                        new CodePrimitiveExpression(0));

                    var itemsRef = new CodeVariableReferenceExpression("items");

                    // Creates a for loop with i
                    var forLoop = new CodeIterationStatement(
                        new CodeAssignStatement(
                            new CodeVariableReferenceExpression("i"),
                            new CodePrimitiveExpression(0)),
                        new CodeBinaryOperatorExpression(
                            new CodeVariableReferenceExpression("i"),
                            CodeBinaryOperatorType.LessThan,
                            new CodePropertyReferenceExpression(itemsRef, "Count")),
                        new CodeAssignStatement(
                            new CodeVariableReferenceExpression("i"),
                            new CodeBinaryOperatorExpression(
                                new CodeVariableReferenceExpression("i"),
                                CodeBinaryOperatorType.Add,
                                new CodePrimitiveExpression(1))),
                        new CodeStatement[]
                            {
                                new CodeExpressionStatement(
                                    new CodeMethodInvokeExpression(
                                        new CodeFieldReferenceExpression(
                                            new CodeThisReferenceExpression(),
                                            privateListNames[table]),
                                        "Remove",
                                        new CodeArrayIndexerExpression(
                                            itemsRef,
                                            new CodeVariableReferenceExpression("i"))))
                            });

                    method.Statements.Add(itemsDecl);
                    method.Statements.Add(iterator);
                    method.Statements.Add(forLoop);

                    method.Comments.Add(new CodeCommentStatement($"<summary> Bulk delete {name} </summary>", true));

                    cls.Members.Add(method);
                }
            }

            var methodSubmit = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "SaveChanges",
                ReturnType = voidTypeRef
            };

            methodSubmit.Comments.Add(new CodeCommentStatement("<summary> Save changes (stub) </summary>", true));

            cls.Members.Add(methodSubmit);

            var methodDispose = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "Dispose",
                ReturnType = voidTypeRef
            };

            cls.Members.Add(methodDispose);

            methodDispose.Comments.Add(new CodeCommentStatement("<summary> Dispose (stub) </summary>", true));

            nameSpace.Types.Add(cls);

            return nameSpace;
        }

        CodeTypeReference GetFunctionReturnType(Function function)
        {
            CodeTypeReference type = null;
            if (function.Return != null)
            {
                type = GetFunctionType(function.Return.Type);
            }

            bool isDataShapeUnknown = function.ElementType == null
                                      && function.BodyContainsSelectStatement
                                      && !function.IsComposable;
            if (isDataShapeUnknown)
            {
                //if we don't know the shape of results, and the proc body contains some selects,
                //we have no choice but to return an untyped DataSet.
                //
                //TODO: either parse proc body like microsoft, 
                //or create a little GUI tool which would call the proc with test values, to determine result shape.
                type = new CodeTypeReference(typeof(DataSet));
            }
            return type;
        }

        static CodeTypeReference GetFunctionType(string type)
        {
            var t = System.Type.GetType(type);
            if (t == null)
                return new CodeTypeReference(type);
            if (t.IsValueType)
                return new CodeTypeReference(typeof(Nullable<>)) {
                    TypeArguments = {
                        new CodeTypeReference(t),
                    },
                };
            return new CodeTypeReference(t);
        }

        CodeParameterDeclarationExpression GetFunctionParameterType(Parameter parameter)
        {
            var p = new CodeParameterDeclarationExpression(GetFunctionType(parameter.Type), parameter.Name) {
                CustomAttributes = {
                    new CodeAttributeDeclaration("Parameter",
                        new CodeAttributeArgument("Name", new CodePrimitiveExpression(parameter.Name)),
                        new CodeAttributeArgument("DbType", new CodePrimitiveExpression(parameter.DbType))),
                },
            };
            switch (parameter.Direction)
            {
                case DbLinq.Schema.Dbml.ParameterDirection.In:
                    p.Direction = FieldDirection.In;
                    break;
                case DbLinq.Schema.Dbml.ParameterDirection.Out:
                    p.Direction = FieldDirection.Out;
                    break;
                case DbLinq.Schema.Dbml.ParameterDirection.InOut:
                    p.Direction = FieldDirection.In | FieldDirection.Out;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            return p;
        }

        protected CodeAssignStatement AddExpressionToSb(CodeExpression expression, CodeExpression sbReference)
        {
            var sbAppendInvoke = new CodeMethodInvokeExpression(sbReference, "Append");
            sbAppendInvoke.Parameters.Add(expression);

            return new CodeAssignStatement(sbReference, sbAppendInvoke);
        }

        protected CodeAssignStatement AddTextExpressionToSb(string text, CodeExpression sbReference)
        {
            return AddExpressionToSb(new CodePrimitiveExpression(text), sbReference);
        }

        protected CodeTypeDeclaration GenerateEfClass(Table table, Database database)
        {
            string schemaName = "public";
            string tableName = table.Name;

            if (table.Name.Contains('.'))
            {
                schemaName = table.Name.Split('.').First();
                tableName = table.Name.Substring(schemaName.Length + 1);
            }

            var cls = new CodeTypeDeclaration()
                {
                    IsClass = true,
                    IsPartial = false,
                    Name = table.Type.Name,
                    TypeAttributes = TypeAttributes.Public,
                    CustomAttributes =
                    {
                        new CodeAttributeDeclaration("Table",
                           new CodeAttributeArgument(new CodePrimitiveExpression(tableName)),
                           new CodeAttributeArgument("Schema", new CodePrimitiveExpression(schemaName))),
                    }
                };

            cls.Comments.Add(new CodeCommentStatement($"<summary> {table.Type.Name} entity </summary>", true));

            this.WriteCustomTypes(cls, table);

            // For InsertSql Property
            if (this.Context.Parameters.MultiInsert)
            {
                cls.BaseTypes.Add("IInsertSqlEntity");
            }

            var pkColumns = table.Type.Columns.Where(col => col.IsPrimaryKey).ToList();

            // InsertSql Property for using in ExecuteFastInsert method
            var sbDeclare = new CodeVariableDeclarationStatement(new CodeTypeReference(typeof(StringBuilder)), "sb", new CodeObjectCreateExpression(typeof(System.Text.StringBuilder), new CodeExpression[] { }));
            var sbReference = new CodeVariableReferenceExpression("sb");
            var propertyInsert = new CodeMemberProperty();
            propertyInsert.Type = new CodeTypeReference(typeof(string));
            propertyInsert.Name = "InsertSql";
            propertyInsert.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            propertyInsert.HasGet = true;
            propertyInsert.GetStatements.Add(sbDeclare);
            propertyInsert.GetStatements.Add(AddTextExpressionToSb("(", sbReference));

            bool firstColumn = true;
            var numericTypes = new List<System.Type>() { typeof(int), typeof(Int16), typeof(Int64), typeof(UInt16), typeof(uint), typeof(UInt64),
                                                           typeof(float), typeof(double), typeof(decimal), typeof(bool), typeof(List<string>) };

            foreach (Column column in table.Type.Columns)
            {
                var type = ToCodeTypeReference(column);
                var columnMember = column.Member ?? column.Name;

                var columnAttrArgs = new List<CodeAttributeArgument>
                {
                    new CodeAttributeArgument(new CodePrimitiveExpression(column.Name))
                };

                if (!this.NetCoreMode && column.IsPrimaryKey && pkColumns.Count > 1)
                {
                    var index = pkColumns.FindIndex(x => x == column);
                    columnAttrArgs.Add(new CodeAttributeArgument("Order", new CodePrimitiveExpression(index)));                    
                }

                var field = new CodeMemberField(type, columnMember)
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    CustomAttributes =
                    {
                        new CodeAttributeDeclaration("Column", columnAttrArgs.ToArray())
                    }
                };

                field.Comments.Add(new CodeCommentStatement($"<summary> {columnMember} </summary>", true));

                if (column.IsPrimaryKey && (!this.NetCoreMode || pkColumns.Count == 1))
                {
                    field.CustomAttributes.Add(new CodeAttributeDeclaration("Key"));
                }

                field.Name += " { get; set; }";

                cls.Members.Add(field);

                var relatedAssociation = (from a in table.Type.Associations
                                           where a.IsForeignKey && a.TheseKeys.Contains(column.Member)
                                           select a)
                                       .FirstOrDefault();

                if (relatedAssociation != null)
                {
                    var fieldRel = new CodeMemberField(relatedAssociation.Member, relatedAssociation.Member)
                                    {
                                        Attributes = MemberAttributes.Static | MemberAttributes.FamilyAndAssembly,
                                        CustomAttributes =
                                        {
                                            new CodeAttributeDeclaration("ForeignKey",
                                                new CodeAttributeArgument(
                                                    new CodeMethodInvokeExpression(null,
                                                        "nameof",
                                                         new CodeVariableReferenceExpression(relatedAssociation.ThisKey))))
                                        }
                                    };

                    fieldRel.Name += " { get; set; }";

                    cls.Members.Add(fieldRel);
                }

                // Add to InsertSql Property
                if (this.Context.Parameters.MultiInsert)
                {
                    var t = System.Type.GetType(column.Type);
                    bool isNumericType = numericTypes.Contains(t);

                    if (!firstColumn)
                    {
                        propertyInsert.GetStatements.Add(AddTextExpressionToSb(", ", sbReference));
                    }

                    var columnProperty = new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), columnMember);

                    var addColumnField = new Func<List<CodeStatement>>(() =>
                        {
                            var codeStatementList = new List<CodeStatement>();

                            if (!isNumericType)
                            {
                                codeStatementList.Add(AddTextExpressionToSb("'", sbReference));
                            }

                            string toStringMethodName = "ToString";
                            if (typeof(IDictionary).IsAssignableFrom(t))
                            {
                                toStringMethodName = "ToHStoreString";
                            }
                            else if (t != typeof(string) && typeof(IEnumerable).IsAssignableFrom(t))
                            {
                                toStringMethodName = "ToArrayString";
                            }
                            else if (t == typeof(List<string>))
                            {
                                toStringMethodName = "ToTsVectorString";
                            }

                            CodeMethodInvokeExpression toStringInvoke;
                            if (t.IsValueType && column.CanBeNull)
                            {
                                var valueProp = new CodePropertyReferenceExpression(columnProperty, "Value");
                                toStringInvoke = new CodeMethodInvokeExpression(valueProp, toStringMethodName);
                            }
                            else
                            {
                                toStringInvoke = new CodeMethodInvokeExpression(columnProperty, toStringMethodName);
                            }

                            if (t == typeof(DateTime))
                            {
                                toStringInvoke.Parameters.Add(new CodePrimitiveExpression("yyyy.MM.dd HH:mm:ss.fffffff"));
                            }
                            else if (t == typeof(bool))
                            {
                                toStringInvoke = new CodeMethodInvokeExpression(toStringInvoke, "ToUpper");
                            }
                            else if (t == typeof(float) || t == typeof(double) || t == typeof(decimal))
                            {
                                toStringInvoke = new CodeMethodInvokeExpression(toStringInvoke, "Replace");
                                toStringInvoke.Parameters.Add(new CodePrimitiveExpression(','));
                                toStringInvoke.Parameters.Add(new CodePrimitiveExpression('.'));
                            }

                            if (!isNumericType && t != typeof(DateTime))
                            {
                                // Processing quotes
                                var expession = (t == typeof(string)) 
                                    ? (CodeExpression)columnProperty 
                                    : toStringInvoke;

                                toStringInvoke = new CodeMethodInvokeExpression(expession, "Replace");
                                toStringInvoke.Parameters.Add(new CodePrimitiveExpression("'"));
                                toStringInvoke.Parameters.Add(new CodePrimitiveExpression("''"));
                            }

                            codeStatementList.Add(AddExpressionToSb(toStringInvoke, sbReference));

                            if (!isNumericType)
                            {
                                codeStatementList.Add(AddTextExpressionToSb("'", sbReference));
                            }

                            // For reference types: if/else statement and null assigment
                            if (t.IsClass)
                            {
                                var codeStatements = codeStatementList.ToArray();

                                var inequality = new CodeBinaryOperatorExpression(columnProperty,
                                    CodeBinaryOperatorType.IdentityInequality, new CodePrimitiveExpression(null));

                                var ifCondition = new CodeConditionStatement(inequality, codeStatements,
                                    new CodeStatement[] { AddTextExpressionToSb("null", sbReference) });

                                return new List<CodeStatement>() { ifCondition };
                            }
                            else if (column.CanBeNull)
                            {
                                var codeStatements = codeStatementList.ToArray();

                                var hasValue = new CodePropertyReferenceExpression(columnProperty, "HasValue");

                                var ifCondition = new CodeConditionStatement(hasValue, codeStatements,
                                    new CodeStatement[] { AddTextExpressionToSb("null", sbReference) });

                                return new List<CodeStatement>() { ifCondition };
                            }
                            else
                            {
                                return codeStatementList;
                            }
                        });

                    if (!string.IsNullOrEmpty(column.Expression))
                    {
                        if (isNumericType)
                        {
                            var codeStatementList = addColumnField().ToArray();

                            var inequality = new CodeBinaryOperatorExpression(columnProperty,
                                CodeBinaryOperatorType.GreaterThan, new CodePrimitiveExpression(0));

                            var ifCondition = new CodeConditionStatement(inequality, codeStatementList,
                                new CodeStatement[] { AddTextExpressionToSb(column.Expression, sbReference) });

                            propertyInsert.GetStatements.Add(ifCondition);
                        }
                        else
                        {
                            propertyInsert.GetStatements.Add(AddTextExpressionToSb(column.Expression, sbReference));
                        }
                    }
                    else
                    {
                        addColumnField().ForEach(x => propertyInsert.GetStatements.Add(x));
                    }

                    firstColumn = false;
                }
            }

            if (Context.Parameters.MultiInsert)
            {
                propertyInsert.GetStatements.Add(AddTextExpressionToSb(")", sbReference));

                var sbToStringInvoke = new CodeMethodInvokeExpression(sbReference, "ToString");

                propertyInsert.GetStatements.Add(new CodeMethodReturnStatement(sbToStringInvoke));

                propertyInsert.Comments.Add(new CodeCommentStatement($"<summary> Insert Sql </summary>", true));

                cls.Members.Add(propertyInsert);

                var commonInsField = new CodeMemberField(typeof(string), "CommonInsertSql")
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final
                };

                commonInsField.Comments.Add(new CodeCommentStatement($"<summary> Common insert Sql </summary>", true));

                var tableCols = string.Join(", ", table.Type.Columns.Select(x => x.Name).ToArray());
                commonInsField.Name += $" => \"INSERT INTO {table.Name} ({tableCols}) VALUES \"";

                cls.Members.Add(commonInsField);
            }

            return cls;
        }

        void WriteCustomTypes(CodeTypeDeclaration entity, Table table)
        {
            // detect required custom types
            foreach (var column in table.Type.Columns)
            {
                var extendedType = column.ExtendedType;
                var enumType = extendedType as EnumType;
                if (enumType != null)
                {
                    Context.ExtendedTypes[column] = new GenerationContext.ExtendedTypeAndName {
                        Type = column.ExtendedType,
                        Table = table
                    };
                }
            }

            var customTypesNames = new List<string>();

            // create names and avoid conflits
            foreach (var extendedTypePair in Context.ExtendedTypes)
            {
                if (extendedTypePair.Value.Table != table)
                    continue;

                if (string.IsNullOrEmpty(extendedTypePair.Value.Type.Name))
                {
                    string name = extendedTypePair.Key.Member + "Type";
                    for (; ; )
                    {
                        if ((from t in Context.ExtendedTypes.Values where t.Type.Name == name select t).FirstOrDefault() == null)
                        {
                            extendedTypePair.Value.Type.Name = name;
                            break;
                        }
                        // at 3rd loop, it will look ugly, however we will never go there
                        name = extendedTypePair.Value.Table.Type.Name + name;
                    }
                }
                customTypesNames.Add(extendedTypePair.Value.Type.Name);
            }

            // write custom types
            if (customTypesNames.Count > 0)
            {
                var customTypes = new List<CodeTypeDeclaration>(customTypesNames.Count);

                foreach (var extendedTypePair in Context.ExtendedTypes)
                {
                    if (extendedTypePair.Value.Table != table)
                        continue;

                    var extendedType = extendedTypePair.Value.Type;
                    var enumValue = extendedType as EnumType;

                    if (enumValue != null)
                    {
                        var enumType = new CodeTypeDeclaration(enumValue.Name) {
                            TypeAttributes = TypeAttributes.Public,
                            IsEnum = true,
                        };
                        customTypes.Add(enumType);
                        var orderedValues = from nv in enumValue orderby nv.Value select nv;
                        int currentValue = 1;
                        foreach (var nameValue in orderedValues)
                        {
                            var field = new CodeMemberField() {
                                Name = nameValue.Key,
                            };
                            enumType.Members.Add(field);
                            if (nameValue.Value != currentValue)
                            {
                                currentValue = nameValue.Value;
                                field.InitExpression = new CodePrimitiveExpression(nameValue.Value);
                            }
                            currentValue++;
                        }
                    }
                }

                if (customTypes.Count == 0)
                    return;
                customTypes.First().StartDirectives.Add(new CodeRegionDirective(CodeRegionMode.Start,
                        string.Format("Custom type definitions for {0}", string.Join(", ", customTypesNames.ToArray()))));
                customTypes.Last().EndDirectives.Add(new CodeRegionDirective(CodeRegionMode.End, null));
                entity.Members.AddRange(customTypes.ToArray());
            }
        }

        static string GetChangedMethodName(string columnName)
        {
            return string.Format("On{0}Changed", columnName);
        }

        CodeTypeMember CreateChangedMethodDecl(Column column)
        {
            return CreatePartialMethod(GetChangedMethodName(column.Member));
        }

        static string GetChangingMethodName(string columnName)
        {
            return string.Format("On{0}Changing", columnName);
        }

        CodeTypeMember CreateChangingMethodDecl(Column column)
        {
            return CreatePartialMethod(GetChangingMethodName(column.Member),
                    new CodeParameterDeclarationExpression(ToCodeTypeReference(column), "value"));
        }

        static CodeTypeReference ToCodeTypeReference(Column column)
        {
            System.Type t = null;
            try
            {
                t = System.Type.GetType(column.Type);
                if (t == null)
                    return new CodeTypeReference(column.Type);
                return t.IsValueType && column.CanBeNull
                    ? new CodeTypeReference("System.Nullable", new CodeTypeReference(column.Type))
                    : new CodeTypeReference(column.Type);
            }
            catch (Exception)
            {
                return new CodeTypeReference(column.Type);
            }
        }

        CodeBinaryOperatorExpression ValuesAreNotEqual(CodeExpression a, CodeExpression b)
        {
            return new CodeBinaryOperatorExpression(a, CodeBinaryOperatorType.IdentityInequality, b);
        }

        CodeBinaryOperatorExpression ValuesAreNotEqual_Ref(CodeExpression a, CodeExpression b)
        {
            return new CodeBinaryOperatorExpression(
                        new CodeBinaryOperatorExpression(
                            a,
                            CodeBinaryOperatorType.IdentityEquality,
                            b),
                        CodeBinaryOperatorType.ValueEquality,
                        new CodePrimitiveExpression(false));
        }

        CodeBinaryOperatorExpression ValueIsNull(CodeExpression value)
        {
            return new CodeBinaryOperatorExpression(
                value,
                CodeBinaryOperatorType.IdentityEquality,
                new CodePrimitiveExpression(null));
        }

        CodeBinaryOperatorExpression ValueIsNotNull(CodeExpression value)
        {
            return new CodeBinaryOperatorExpression(
                value,
                CodeBinaryOperatorType.IdentityInequality, 
                new CodePrimitiveExpression(null));
        }

        static string GetStorageFieldName(Column column)
        {
            return GetStorageFieldName(column.Storage ?? column.Member);
        }

        static string GetStorageFieldName(string storage)
        {
            if (storage.StartsWith("_"))
                return storage;
            return "_" + storage;
        }

        static string GetLowerCamelCase(string str)
        {
            return char.ToLowerInvariant(str[0]) + str.Substring(1);
        }

        static string GetStorageFieldName(Association association)
        {
            return association.Storage != null 
                ? GetStorageFieldName(association.Storage) 
                : "_" + CreateIdentifier(association.Member ?? association.Name);
        }

        static string CreateIdentifier(string value)
        {
            return Regex.Replace(value, @"\W", "_");
        }
    }
}
