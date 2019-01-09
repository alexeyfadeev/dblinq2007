namespace DbMetal.Generator.Implementation
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Microsoft.CSharp;

    using Newtonsoft.Json;

    public static class EnumDefinitionReader
    {
        public static List<EnumDefinition> Read(string path)
        {
            string code1 = @"
            using System;
            using System.Collections.Generic;
                
            namespace DynamicCode2
            {
                public class EnumDefinition
                {
                    public string Schema { get; set; }
                    public string Table { get; set; }
                    public string Column { get; set; }
                    public string EnumType { get; set; }
                }

                public class EnumDef
                {
                    List<EnumDefinition> EnumsDefinitions = new List<EnumDefinition>();

                    public List<EnumDefinition> FillEnumsDefinitions()
                    {";

            string code2 = @"
                        return this.EnumsDefinitions;
                    }
                }
            }";

            var lines = File.ReadAllLines(path)
                .Where(x => x.Contains("this.EnumsDefinitions.Add"));

            string code = $"{code1}{string.Join(Environment.NewLine, lines)}{code2}";

            var provider = new CSharpCodeProvider();
            var results = provider.CompileAssemblyFromSource(new CompilerParameters(), code);
            var t = results.CompiledAssembly.GetType("DynamicCode2.EnumDef");

            var obj = Activator.CreateInstance(t);
            var method = t.GetMethod("FillEnumsDefinitions");
            var result = method.Invoke(obj, new object[0]);

            var json = JsonConvert.SerializeObject(result);
            var list = JsonConvert.DeserializeObject<List<EnumDefinition>>(json);
            return list;
        }
    }
}
