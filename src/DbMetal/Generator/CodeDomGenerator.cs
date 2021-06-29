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
    using System.Linq.Expressions;
    using System.Threading.Tasks;

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

        public string EntityFolder { get; set; }

        public string ContextName { get; set; }

        public bool SqlXml { get; set; }

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
            string provider)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                GenerateEfContextDomModel(dbSchema, provider.ToLower()), textWriter,
                new CodeGeneratorOptions
                    {
                        BracingStyle = "C",
                        IndentString = "\t",
                    });
        }

        public void WriteIContext(TextWriter textWriter, Database dbSchema, GenerationContext context, bool bulkExtensions)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                this.GenerateIContextDomModel(dbSchema),
                textWriter,
                new CodeGeneratorOptions()
                {
                    BracingStyle = "C",
                    IndentString = "\t",
                });
        }

        /*
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
        */

        public void WriteTestContext(TextWriter textWriter, Database dbSchema, GenerationContext context, bool bulkExtensions)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                this.GenerateTestContextDomModel(dbSchema),
                textWriter,
                new CodeGeneratorOptions()
                {
                    BracingStyle = "C",
                    IndentString = "\t",
                });
        }

        public void WriteHelper(TextWriter textWriter, Database dbSchema, GenerationContext context)
        {
            Context = context;

            Provider.CreateGenerator(textWriter).GenerateCodeFromNamespace(
                this.GenerateHelperDomModel(dbSchema),
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

            if (table.Type.Columns.Any(x => x.Type == "System.DateTime"))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport("System"));
            }

            nameSpace.Imports.Add(new CodeNamespaceImport("LinqToDB.Mapping"));

            bool additionalNamespaces = Context.Parameters.AdditionalNamespaces?.Any() == true
                                        && table.Type.Columns.Any(x => !string.IsNullOrWhiteSpace(x.Comment));

            if (additionalNamespaces)
            {
                foreach (var additionalNamespace in Context.Parameters.AdditionalNamespaces)
                {
                    nameSpace.Imports.Add(new CodeNamespaceImport(additionalNamespace));
                }
            }

            nameSpace.Types.Add(this.GenerateEfClass(table, database, additionalNamespaces));

            return nameSpace;
        }

        protected virtual CodeNamespace GenerateIContextDomModel(Database database)
        {
            CheckLanguageWords(Context.Parameters.Culture);

            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Threading"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Threading.Tasks"));

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            nameSpace.Imports.Add(new CodeNamespaceImport("LinqToDB"));
            nameSpace.Imports.Add(new CodeNamespaceImport("LinqToDB.Data"));

            var iface = new CodeTypeDeclaration($"I{this.ContextName}DbContext")
            {
                IsInterface = true,
                IsPartial = true
            };

            iface.Comments.Add(new CodeCommentStatement("<summary> Interface for database context </summary>", true));

            iface.BaseTypes.Add(new CodeTypeReference("IDataContext"));

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

            var taskTypeRef = new CodeTypeReference("Task");

            var methodBeginTrans = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "BeginTransaction",
                ReturnType = new CodeTypeReference("DataConnectionTransaction")
            };

            methodBeginTrans.Comments.Add(new CodeCommentStatement($"<summary> Begin Transaction </summary>", true));

            iface.Members.Add(methodBeginTrans);
            
            var methodCommitTrans = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "CommitTransactionAsync",
                ReturnType = taskTypeRef
            };

            methodCommitTrans.Parameters.Add(new CodeParameterDeclarationExpression("CancellationToken", "cancellationToken = default"));
            methodCommitTrans.Comments.Add(new CodeCommentStatement($"<summary> Commit Transaction </summary>", true));

            iface.Members.Add(methodCommitTrans);

            var methodRollbackTrans = new CodeMemberMethod()
            {
                Attributes = MemberAttributes.Public | MemberAttributes.Final,
                Name = "RollbackTransactionAsync",
                ReturnType = taskTypeRef
            };

            methodRollbackTrans.Parameters.Add(new CodeParameterDeclarationExpression("CancellationToken", "cancellationToken = default"));
            methodRollbackTrans.Comments.Add(new CodeCommentStatement($"<summary> Rollback Transaction </summary>", true));

            iface.Members.Add(methodRollbackTrans);

            nameSpace.Types.Add(iface);

            return nameSpace;
        }

        protected virtual CodeNamespace GenerateEfContextDomModel(Database database, string provider)
        {
            this.CheckLanguageWords(this.Context.Parameters.Culture);

            string nameSpaceName = this.Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            nameSpace.Imports.Add(new CodeNamespaceImport("LinqToDB.Data"));

            var cls = new CodeTypeDeclaration(this.ContextName + "DbContext")
            {
                IsPartial = true,
                Attributes = MemberAttributes.Public | MemberAttributes.Final
            };

            cls.Comments.Add(new CodeCommentStatement("<summary> Database context </summary>", true));

            cls.BaseTypes.Add(new CodeTypeReference("DataConnection"));
            cls.BaseTypes.Add(new CodeTypeReference($"I{this.ContextName}DbContext"));

            // Constructors
            var constructor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public,
                Parameters = { new CodeParameterDeclarationExpression(typeof(string), "providerName") },
            };

            constructor.Parameters.Add(new CodeParameterDeclarationExpression(typeof(string), "connectionString"));

            constructor.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("providerName"));
            constructor.BaseConstructorArgs.Add(new CodeArgumentReferenceExpression("connectionString"));

            /*
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
            */

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
                    Type = new CodeTypeReference("IQueryable", tableType),
                };

                field.Comments.Add(new CodeCommentStatement($"<summary> {name} </summary>", true));

                field.Name += $" => GetTable<{table.Type.Name}>()";

                cls.Members.Add(field);
            }

            /*
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
            */

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

        private Dictionary<Table, string> CreatePrivateListNames(Database database)
        {
            var privateListNames = new Dictionary<Table, string>();

            foreach (Table table in database.Tables)
            {
                privateListNames.Add(table, GetLowerCamelCase(this.GetTableNamePluralized(table.Member)));
            }

            return privateListNames;
        }

        protected virtual CodeNamespace GenerateTestContextDomModel(Database database)
        {
            CheckLanguageWords(Context.Parameters.Culture);

            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;
            string testNameSpaceName = !string.IsNullOrWhiteSpace(Context.Parameters.TestNamespace)
                ? Context.Parameters.TestNamespace
                : nameSpaceName;

            CodeNamespace nameSpace = new CodeNamespace(testNameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport("System"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));
            nameSpace.Imports.Add(new CodeNamespaceImport("LinqToDB"));
            nameSpace.Imports.Add(new CodeNamespaceImport(nameSpaceName));

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            var cls = new CodeTypeDeclaration($"{this.ContextName}TestDbContext")
            {
                IsClass = true,
                IsPartial = true
            };

            cls.Comments.Add(new CodeCommentStatement("<summary> Test DB context </summary>", true));

            var contextInterfaceName = $"I{this.ContextName}DbContext";

            var contextField = new CodeMemberField
            {
                Attributes = MemberAttributes.Final | MemberAttributes.Private,
                Name = "context",
                Type = new CodeTypeReference($"readonly {contextInterfaceName}"),
            };

            cls.Members.Add(contextField);

            var hashSetField = new CodeMemberField
            {
                Attributes = MemberAttributes.Final | MemberAttributes.Private,
                Name = "createdTables = new HashSet<Type>()",
                Type = new CodeTypeReference("readonly HashSet<Type>"),
            };

            cls.Members.Add(hashSetField);

            // Constructors
            var constructor = new CodeConstructor
            {
                Attributes = MemberAttributes.Public,
                Parameters = { new CodeParameterDeclarationExpression(contextInterfaceName, "context") },
            };

            /*
            else
            {
                var fieldConnStr = new CodeMemberField
                {
                    Attributes = MemberAttributes.Final | MemberAttributes.Private,
                    Name = "connectionString",
                    Type = new CodeTypeReference(typeof(string))
                };

                cls.Members.Add(fieldConnStr);

                constructor.Statements.Add(assignStatement);
            }
            */

            var contextProp = new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), "context");

            var assignStatement = new CodeAssignStatement(
                contextProp,
                new CodeArgumentReferenceExpression("context"));

            constructor.Statements.Add(assignStatement);

            constructor.Comments.Add(new CodeCommentStatement("<summary> Database test context constructor </summary>", true));
            cls.Members.Add(constructor);

            foreach (Table table in database.Tables)
            {
                var tableType = new CodeTypeReference(table.Type.Name);

                string name = GetTableNamePluralized(table.Member);

                var field = new CodeMemberProperty
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = name,
                    Type = new CodeTypeReference("IQueryable", tableType),
                };
                field.HasGet = true;

                var checkMethod = new CodeMethodInvokeExpression(
                    new CodeThisReferenceExpression(),
                    $"CheckTable<{table.Type.Name}>"
                );

                field.GetStatements.Add(checkMethod);

                field.GetStatements.Add(new CodeMethodReturnStatement(new CodePropertyReferenceExpression(contextProp, name)));

                field.Comments.Add(new CodeCommentStatement($"<summary> {name} </summary>", true));

                cls.Members.Add(field);
            }

            var method = new CodeMemberMethod
            {
                Attributes = MemberAttributes.Private,
                Name = "CheckTable<T>",
                ReturnType = new CodeTypeReference(typeof(void))
            };

            var createdTablesProp = new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), "createdTables");
            var notCreatedTablesProp = new CodePropertyReferenceExpression(new CodeSnippetExpression("!this"), "createdTables");

            var typeOfExpr = new CodeSnippetExpression("typeof(T)");

            var containsMethod = new CodeMethodInvokeExpression(
                notCreatedTablesProp,
                "Contains",
                typeOfExpr);

            var createMethod = new CodeExpressionStatement(new CodeMethodInvokeExpression(contextProp, "CreateTable<T>"));
            var addMethod = new CodeExpressionStatement(new CodeMethodInvokeExpression(createdTablesProp, "Add", typeOfExpr));

            var ifStatement = new CodeConditionStatement(containsMethod,
                new CodeStatement[] { createMethod, addMethod },
                new CodeStatement[0]);

            method.Statements.Add(ifStatement);

            cls.Members.Add(method);

            nameSpace.Types.Add(cls);

            return nameSpace;
        }

        protected virtual CodeNamespace GenerateHelperDomModel(Database database)
        {
            CheckLanguageWords(Context.Parameters.Culture);

            string nameSpaceName = Context.Parameters.Namespace ?? database.ContextNamespace;

            CodeNamespace nameSpace = new CodeNamespace(nameSpaceName);

            nameSpace.Imports.Add(new CodeNamespaceImport("System.Linq"));

            if (!string.IsNullOrWhiteSpace(this.EntityFolder))
            {
                nameSpace.Imports.Add(new CodeNamespaceImport($"{nameSpaceName}.{this.EntityFolder}"));
            }

            var cls = new CodeTypeDeclaration($"static Mock{this.ContextName}RepositoryHelper")
            {
                IsClass = true,
                Attributes = MemberAttributes.Public
            };

            cls.Comments.Add(new CodeCommentStatement("<summary> Mock repository helper </summary>", true));

            var voidTypeRef = new CodeTypeReference(typeof(void));

            foreach (Table table in database.Tables)
            {
                var relatedAssociations = (from a in table.Type.Associations
                                           where a.IsForeignKey
                                           select a)
                    .ToList();

                if (!relatedAssociations.Any())
                {
                    continue;
                }

                var methodLinks = new CodeMemberMethod
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final | MemberAttributes.Static,
                    Name = "SetLinks",
                    ReturnType = voidTypeRef
                };

                var tableType = new CodeTypeReference("this " + table.Type.Name);

                methodLinks.Parameters.Add(new CodeParameterDeclarationExpression(tableType, "item"));
                methodLinks.Parameters.Add(new CodeParameterDeclarationExpression(
                    new CodeTypeReference($"Mock{this.ContextName}Repository"), 
                    "repo"));

                foreach (var ra in relatedAssociations)
                {
                    var datasetName = this.GetTableNamePluralized(ra.Member);

                    string otherKey = ra.OtherKey == "ID" ? "Id" : ra.OtherKey;
                    string thisKey = ra.ThisKey.EndsWith("ID") ? ra.ThisKey.Replace("ID", "Id") : ra.ThisKey;

                    var statement = new CodeSnippetExpression(
                            $"item.{ra.Member} = repo.{datasetName}.FirstOrDefault(k => k.{otherKey} == item.{thisKey})");

                    methodLinks.Statements.Add(statement);
                }

                methodLinks.Comments.Add(new CodeCommentStatement("<summary> Set FK links </summary>", true));

                cls.Members.Add(methodLinks);
            }

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

        protected CodeTypeDeclaration GenerateEfClass(Table table, Database database, bool additionalNamespaces)
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
            propertyInsert.Name = this.SqlXml ? "SqlXml" : "InsertSql";
            propertyInsert.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            propertyInsert.HasGet = true;
            propertyInsert.GetStatements.Add(sbDeclare);
            propertyInsert.GetStatements.Add(AddTextExpressionToSb(this.SqlXml ? "<Item>\n" : "(", sbReference));

            bool firstColumn = true;
            var numericTypes = new List<System.Type>() { typeof(int), typeof(Int16), typeof(Int64), typeof(UInt16), typeof(uint), typeof(UInt64),
                                                           typeof(float), typeof(double), typeof(decimal), typeof(bool), typeof(List<string>) };

            var entityFolderPrefix = !string.IsNullOrWhiteSpace(this.EntityFolder)
                ? $"{this.EntityFolder}."
                : string.Empty;

            foreach (Column column in table.Type.Columns)
            {
                var type = ToCodeTypeReference(column, additionalNamespaces);
                var columnMember = column.Member ?? column.Name;

                var columnAttrArgs = new List<CodeAttributeArgument>
                {
                    new CodeAttributeArgument(new CodePrimitiveExpression(column.Name))
                };

                /*
                if (!this.NetCoreMode && column.IsPrimaryKey && pkColumns.Count > 1)
                {
                    var index = pkColumns.FindIndex(x => x == column);
                    columnAttrArgs.Add(new CodeAttributeArgument("Order", new CodePrimitiveExpression(index)));                    
                }
                else if (column.DbType == "jsonb")
                {
                    columnAttrArgs.Add(new CodeAttributeArgument("TypeName", new CodePrimitiveExpression("jsonb")));
                }*/

                var field = new CodeMemberField(type, columnMember)
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    CustomAttributes =
                    {
                        new CodeAttributeDeclaration("Column", columnAttrArgs.ToArray())
                    }
                };

                field.Comments.Add(new CodeCommentStatement($"<summary> {columnMember} </summary>", true));

                if (column.IsPrimaryKey && pkColumns.Count == 1)
                {
                    field.CustomAttributes.Add(new CodeAttributeDeclaration("PrimaryKey"));
                }

                if (column.IsDbGenerated)
                {
                    field.CustomAttributes.Add(new CodeAttributeDeclaration("Identity"));
                }

                field.Name += " { get; set; }";

                cls.Members.Add(field);

                var relatedAssociation = (from a in table.Type.Associations
                                           where a.IsForeignKey && a.TheseKeys.Contains(column.Member)
                                           select a)
                                       .FirstOrDefault();

                if (relatedAssociation != null)
                {
                    string memberName = relatedAssociation.Member;

                    if (!relatedAssociation.ThisKey.Contains(relatedAssociation.Type)
                        && relatedAssociation.ThisKey.EndsWith("Id")
                        && relatedAssociation.ThisKey.Length > 2
                        && relatedAssociation.Member.StartsWith(relatedAssociation.Type))
                    {
                        memberName = $"{relatedAssociation.ThisKey.Substring(0, relatedAssociation.ThisKey.Length - 2)}{relatedAssociation.Type}";
                    }
                    else if (relatedAssociation.Member.StartsWith(relatedAssociation.Type)
                        && relatedAssociation.ThisKey.Contains(relatedAssociation.Type)
                        && (relatedAssociation.Member.EndsWith("1")
                                || relatedAssociation.Member.EndsWith("2")
                                || relatedAssociation.Member.EndsWith("3")))
                    {
                        memberName = relatedAssociation.Type;
                    }

                    var fieldRel = new CodeMemberField(relatedAssociation.Type, memberName)
                    {
                        Attributes = MemberAttributes.Static | MemberAttributes.FamilyAndAssembly,
                        CustomAttributes =
                        {
                            new CodeAttributeDeclaration("Association",
                                new CodeAttributeArgument(
                                    new CodeMethodInvokeExpression(null,
                                        "ThisKey = nameof",
                                         new CodeVariableReferenceExpression(relatedAssociation.ThisKey))),
                                new CodeAttributeArgument(
                                    new CodeMethodInvokeExpression(null,
                                        "OtherKey = nameof",
                                        new CodeVariableReferenceExpression($"{entityFolderPrefix}{relatedAssociation.Type}.{relatedAssociation.OtherKey}"))))
                        }
                    };

                    fieldRel.Name += " { get; set; }";

                    fieldRel.Comments.Add(new CodeCommentStatement($"<summary> Association for {columnMember} </summary>", true));

                    cls.Members.Add(fieldRel);
                }

                // Add to InsertSql Property
                if (this.Context.Parameters.MultiInsert && !this.SqlXml)
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
                else if (this.Context.Parameters.MultiInsert && this.SqlXml)
                {
                    propertyInsert.GetStatements.Add(this.AddTextExpressionToSb($"<{columnMember}>", sbReference));

                    var columnProperty = new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), columnMember);

                    var t = System.Type.GetType(column.Type);

                    if (t.IsValueType && column.CanBeNull)
                    {
                        t = typeof(Nullable<>).MakeGenericType(t);
                    }

                    CodeExpression expression = columnProperty;

                    if (t == typeof(float) || t == typeof(double) || t == typeof(decimal) || t == typeof(DateTime))
                    {
                        var toStringInvoke = new CodeMethodInvokeExpression(columnProperty, "ToString");

                        if (t == typeof(float) || t == typeof(double) || t == typeof(decimal))
                        {
                            toStringInvoke = new CodeMethodInvokeExpression(toStringInvoke, "Replace");
                            toStringInvoke.Parameters.Add(new CodePrimitiveExpression(','));
                            toStringInvoke.Parameters.Add(new CodePrimitiveExpression('.'));
                        }
                        else if (t == typeof(DateTime))
                        {
                            toStringInvoke.Parameters.Add(new CodePrimitiveExpression("yyyyMMdd HH:mm:ss"));
                        }

                        expression = toStringInvoke;
                    }
                    else if (t == typeof(DateTime?))
                    {
                        expression = new CodeSnippetExpression($"this.{columnMember}.HasValue ? this.{columnMember}.Value.ToString(\"yyyyMMdd HH:mm:ss\") : \"\"");
                    }
                    else if (t == typeof(float?) || t == typeof(double?) || t == typeof(decimal?))
                    {
                        expression = new CodeSnippetExpression($"(this.{columnMember}.HasValue ? this.{columnMember}.Value.ToString() : \"\")");

                        var toStringInvoke = new CodeMethodInvokeExpression(expression, "Replace");
                        toStringInvoke.Parameters.Add(new CodePrimitiveExpression(','));
                        toStringInvoke.Parameters.Add(new CodePrimitiveExpression('.'));

                        expression = toStringInvoke;
                    }

                    propertyInsert.GetStatements.Add(this.AddExpressionToSb(expression, sbReference));

                    propertyInsert.GetStatements.Add(this.AddTextExpressionToSb($"</{columnMember}>\n", sbReference));
                }
            }

            if (Context.Parameters.MultiInsert)
            {
                if (!this.SqlXml)
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

                    commonInsField.Comments.Add(
                        new CodeCommentStatement($"<summary> Common insert Sql </summary>", true));

                    var tableCols = string.Join(", ", table.Type.Columns.Select(x => x.Name).ToArray());
                    commonInsField.Name += $" => \"INSERT INTO {table.Name} ({tableCols}) VALUES \"";

                    cls.Members.Add(commonInsField);
                }
                else
                {
                    propertyInsert.GetStatements.Add(this.AddTextExpressionToSb($"</Item>\n", sbReference));

                    var sbToStringInvoke = new CodeMethodInvokeExpression(sbReference, "ToString");
                    var sbReplaceInvoke = new CodeMethodInvokeExpression(sbToStringInvoke, "Replace");
                    sbReplaceInvoke.Parameters.Add(new CodePrimitiveExpression("'"));
                    sbReplaceInvoke.Parameters.Add(new CodePrimitiveExpression("''"));

                    propertyInsert.GetStatements.Add(new CodeMethodReturnStatement(sbReplaceInvoke));

                    propertyInsert.Comments.Add(new CodeCommentStatement($"<summary> Sql Xml </summary>", true));

                    cls.Members.Add(propertyInsert);

                    // Common Sql xml
                    var propertyCommonSql = new CodeMemberProperty();
                    propertyCommonSql.Type = new CodeTypeReference(typeof(string));
                    propertyCommonSql.Name = "CommonSql";
                    propertyCommonSql.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                    propertyCommonSql.HasGet = true;
                    propertyCommonSql.GetStatements.Add(sbDeclare);

                    sbReference = new CodeVariableReferenceExpression("sb");

                    propertyCommonSql.Comments.Add(
                        new CodeCommentStatement($"<summary> Common sql </summary>", true));

                    propertyCommonSql.GetStatements.Add(this.AddTextExpressionToSb("WITH q1 AS(\nSELECT\n", sbReference));

                    foreach (var col in table.Type.Columns)
                    {
                        string dbType = col.DbType;
                        if (dbType.EndsWith("char"))
                        {
                            dbType += "(max)";
                        }

                        propertyCommonSql.GetStatements.Add(this.AddExpressionToSb(
                            new CodeSnippetExpression($"\"t.value('({col.Member}/text())[1]', '{dbType}') AS [{col.Name}]{(col == table.Type.Columns.Last() ? "" : ", ")}\""),
                            sbReference));
                    }

                    propertyCommonSql.GetStatements.Add(this.AddTextExpressionToSb("\nFROM @xml.nodes('/Item') AS x(t)\n)\n\n", sbReference));

                    propertyCommonSql.GetStatements.Add(new CodeMethodReturnStatement(sbToStringInvoke));

                    cls.Members.Add(propertyCommonSql);

                    var commonProperty = new CodePropertyReferenceExpression(new CodeThisReferenceExpression(), "CommonSql");
                    
                    // Common insert Sql xml
                    var propertyCommonInsertSql = new CodeMemberProperty();
                    propertyCommonInsertSql.Type = new CodeTypeReference(typeof(string));
                    propertyCommonInsertSql.Name = "CommonInsertSql";
                    propertyCommonInsertSql.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                    propertyCommonInsertSql.HasGet = true;

                    propertyCommonInsertSql.Comments.Add(new CodeCommentStatement($"<summary> Common insert sql </summary>", true));
                    
                    var tableCols = string.Join(", ", table.Type.Columns.Select(x => x.Name).ToArray());

                    var textExpression = new CodePrimitiveExpression($"INSERT INTO {table.Name} ({tableCols}) SELECT * FROM q1;");
                    var operatorExpression = new CodeBinaryOperatorExpression(commonProperty, CodeBinaryOperatorType.Add, textExpression);

                    propertyCommonInsertSql.GetStatements.Add(new CodeMethodReturnStatement(operatorExpression));

                    cls.Members.Add(propertyCommonInsertSql);

                    // Common update Sql xml
                    var propertyCommonUpdateSql = new CodeMemberProperty();
                    propertyCommonUpdateSql.Type = new CodeTypeReference(typeof(string));
                    propertyCommonUpdateSql.Name = "CommonUpdateSql";
                    propertyCommonUpdateSql.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                    propertyCommonUpdateSql.HasGet = true;

                    propertyCommonUpdateSql.Comments.Add(new CodeCommentStatement($"<summary> Common update sql </summary>", true));
                    
                    var updateCols = string.Join(", ", table.Type.Columns
                            .Where(x => !x.IsPrimaryKey)
                            .Select(x => $"t.{x.Name}=q1.{x.Name}")
                            .ToArray());

                    var whereCols = string.Join(" AND ", table.Type.Columns
                        .Where(x => x.IsPrimaryKey)
                        .Select(x => $"t.{x.Name}=q1.{x.Name}")
                        .ToArray());

                    textExpression = new CodePrimitiveExpression($"UPDATE t SET {updateCols} FROM q1 JOIN {table.Name} t ON {whereCols};");
                    operatorExpression = new CodeBinaryOperatorExpression(commonProperty, CodeBinaryOperatorType.Add, textExpression);

                    propertyCommonUpdateSql.GetStatements.Add(new CodeMethodReturnStatement(operatorExpression));

                    cls.Members.Add(propertyCommonUpdateSql);
                }
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

        static CodeTypeReference ToCodeTypeReference(Column column, bool additionalNamespaces)
        {
            var reference = new CodeTypeReference(column.Type);

            try
            {
                var t = System.Type.GetType(column.Type);

                if (t == null)
                    return reference;

                var isNullable = t.IsValueType && column.CanBeNull;

                if (additionalNamespaces && !string.IsNullOrWhiteSpace(column.Comment)
                                         && (t == typeof(int) || t == typeof(long) || t == typeof(short)
                                             || t == typeof(uint) || t == typeof(ulong) || t == typeof(ushort)))
                {
                    return isNullable
                        ? new CodeTypeReference($"{column.Comment}?")
                        : new CodeTypeReference(column.Comment);
                }

                return isNullable
                    ? new CodeTypeReference("System.Nullable", new CodeTypeReference(column.Type))
                    : reference;
            }
            catch (Exception)
            {
                return reference;
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
