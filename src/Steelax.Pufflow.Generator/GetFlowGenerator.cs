using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Steelax.Pufflow.Generator;

/// <summary>
/// An incremental Roslyn source generator that produces IFlowable interface implementations
/// implementations for classes annotated with the <c>[Flow]</c> attribute.
/// </summary>
/// <remarks>
/// The generator inspects the public, non-static handler methods of a flow-annotated class
/// (<c>GetEnumerator</c>, <c>GetAsyncEnumerator</c>, <c>Handle</c>, <c>GetConsumator</c>,
/// <c>GetAsyncConsumator</c>, <c>GetProducator</c>, <c>GetAsyncProducator</c>, <c>Execute</c>,
/// <c>ExecuteAsync</c>) and emits a partial class implementing the appropriate
/// IFlowable (Source, Sink, or Pipe) interface, plus disambiguation view properties.
/// It also supports composing two single-interface handlers into a composite pipe shape.
/// </remarks>
[Generator]
public class GetFlowGenerator : IIncrementalGenerator
{
    private const string FlowContextName = "Steelax.Pufflow.FlowContext";
    private const string FlowInterfaceNs = "Steelax.Pufflow.Abstractions";
    private const string ReadInterfacesNs = "System.Collections.Generic";

    private static readonly string[] HandlerNames =
    [
        "GetEnumerator",
        "GetAsyncEnumerator",
        "Handle",
        "GetAsyncConsumator",
        "GetConsumator",
        "GetProducator",
        "GetAsyncProducator",
        "Execute",
        "ExecuteAsync"
    ];

    // Read interfaces (pull - data flows out)
    private static readonly string[] ReadInterfaceNames =
    [
        "System.Collections.Generic.IEnumerator",
        "System.Collections.Generic.IAsyncEnumerator",
        "Steelax.Pufflow.Abstractions.IConsumator",
        "Steelax.Pufflow.Abstractions.IAsyncConsumator"
    ];

    // Write interfaces (push - data flows in)
    private static readonly string[] WriteInterfaceNames =
    [
        "Steelax.Pufflow.Abstractions.IProducator",
        "Steelax.Pufflow.Abstractions.IAsyncProducator"
    ];

    private const string SourceName = "Steelax.Pufflow.Source";
    private const string SinkName = "Steelax.Pufflow.Sink";
    private const string PipeName = "Steelax.Pufflow.Pipe";
    private const string SyncName = "Steelax.Pufflow.Abstractions.Sync";
    private const string AsyncName = "Steelax.Pufflow.Abstractions.Async";

    /// <summary>
    /// Initializes the incremental generator pipeline.
    /// </summary>
    /// <param name="context">The incremental generator initialization context.</param>
    /// <remarks>
    /// Registers a syntax provider that filters class declarations with attributes
    /// and produces the <c>[Flow]</c>-annotated class symbols, then combines them
    /// with the compilation to emit the generated source.
    /// </remarks>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var provider = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => IsCandidate(node),
                static (ctx, _) => GetClassSymbol(ctx))
            .Where(static symbol => symbol is not null);

        context.RegisterSourceOutput(
            context.CompilationProvider.Combine(provider.Collect()),
            static (ctx, t) => GenerateCode(ctx, t.Left, t.Right!));
    }

    private static bool IsCandidate(SyntaxNode node)
    {
        return node is ClassDeclarationSyntax { AttributeLists.Count: > 0 };
    }

    private static INamedTypeSymbol? GetClassSymbol(GeneratorSyntaxContext context)
    {
        if (context.Node is not ClassDeclarationSyntax classDeclaration)
            return null;

        var semanticModel = context.SemanticModel;
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol is null)
            return null;

        var flowAttributeType = semanticModel.Compilation.GetTypeByMetadataName("Steelax.Pufflow.Abstractions.FlowAttribute");

        if (flowAttributeType is null)
            return null;

        var attrs = classSymbol.GetAttributes();

        foreach (var attr in attrs)
        {
            var attrClass = attr.AttributeClass;

            if (attrClass is null)
                continue;

            var attrName = attrClass.ToDisplayString();

            if (attrName == "Steelax.Pufflow.Abstractions.FlowAttribute" ||
                attrName is "FlowAttribute" or "Flow")
                return classSymbol;

            if (SymbolEqualityComparer.Default.Equals(attrClass, flowAttributeType))
                return classSymbol;
        }

        return null;
    }

    private static void GenerateCode(
        SourceProductionContext context,
        Compilation compilation,
        ImmutableArray<INamedTypeSymbol> classSymbols)
    {
        foreach (var classSymbol in classSymbols)
        {
            try
            {
                GenerateForClass(context, classSymbol);
            }
            catch (System.Exception ex)
            {
                context.ReportDiagnostic(
                    Diagnostic.Create(
                        new DiagnosticDescriptor(
                            "DFG001",
                            "GetFlowGenerator Error",
                            "Error generating GetFlow for {0}: {1}",
                            "GetFlowGenerator",
                            DiagnosticSeverity.Warning,
                            isEnabledByDefault: true),
                        classSymbol.Locations.FirstOrDefault(),
                        classSymbol.Name,
                        ex.Message));
            }
        }
    }

    private static void GenerateForClass(SourceProductionContext context, INamedTypeSymbol classSymbol)
    {
        var handlers = FindHandlerMethods(classSymbol);

        if (handlers.Count == 0)
            return;

        var interfaceLines = new List<string>();

        // Check for composite Pipe: exactly 2 return-based handlers with 0 flow params,
        // one returns writer (Sink), one returns reader (Source) → combine into Pipe<TWrite, TRead>
        var ret0Params = handlers.Where(h => !IsExecuteReturn(h.ReturnType) && CountFlowParams(h) == 0).ToList();
        var compositeHandlers = new HashSet<HandlerInfo>();

        if (ret0Params.Count == 2)
        {
            var h1 = ret0Params[0];
            var h2 = ret0Params[1];
            var h1IsWrite = IsWriteInterface(h1.ReturnType);
            var h2IsWrite = IsWriteInterface(h2.ReturnType);

            if (h1IsWrite && !h2IsWrite)
            {
                interfaceLines.Add(BuildPipe2Interface(h1.ReturnType, h2.ReturnType));
                compositeHandlers.Add(h1);
                compositeHandlers.Add(h2);
            }
            else if (!h1IsWrite && h2IsWrite)
            {
                interfaceLines.Add(BuildPipe2Interface(h2.ReturnType, h1.ReturnType));
                compositeHandlers.Add(h1);
                compositeHandlers.Add(h2);
            }
        }

        // Process remaining handlers that weren't part of a composite
        foreach (var handler in handlers)
        {
            // Skip handlers already processed as composite
            if (compositeHandlers.Contains(handler))
                continue;

            var isExecute = IsExecuteReturn(handler.ReturnType);
            var flowParamCount = CountFlowParams(handler);

            if (isExecute)
            {
                interfaceLines.Add(BuildExecuteBasedInterface(handler));
            }
            else if (flowParamCount == 0)
            {
                interfaceLines.Add(BuildReturnBased0ParamInterface(handler));
            }
            else if (flowParamCount == 1)
            {
                interfaceLines.Add(BuildReturnBased1ParamInterface(handler));
            }
        }

        if (interfaceLines.Count == 0)
            return;

        // Build combined interface declaration
        var interfaceLine = interfaceLines[0];
        for (int i = 1; i < interfaceLines.Count; i++)
        {
            var line = interfaceLines[i].TrimStart();
            if (line.StartsWith(": "))
                line = line.Substring(2);
            interfaceLine += ",\n      " + line;
        }

        Trace.WriteLine($"[GetFlowGenerator] GenerateForClass: class='{classSymbol.ToDisplayString()}', typeParams={classSymbol.TypeParameters.Length}");

        var namespaceName = classSymbol.ContainingNamespace.ToDisplayString();
        var className = classSymbol.Name;
        var isRecord = classSymbol.IsRecord;
        var recordPrefix = isRecord ? " record" : "";

        // Build nested class declaration chain
        var classDecl = "";
        var currentSymbol = classSymbol;
        var first = true;

        while (currentSymbol is not null)
        {
            var currentName = currentSymbol.Name;
            var currentTypeParams = "";

            // Collect type parameter names; constraints are declared by the user-authored partial
            // and must not be duplicated in the generated declaration.
            if (currentSymbol.TypeParameters.Length > 0)
            {
                currentTypeParams = "<" + string.Join(", ", currentSymbol.TypeParameters.Select(tp => tp.Name)) + ">";
            }

            var currentRecord = currentSymbol.IsRecord ? " record" : "";
            var currentPartial = currentSymbol.DeclaredAccessibility == Accessibility.Public ? "public partial" : "partial";

            // First (innermost) gets the interface + view properties, rest are containers
            if (first)
            {
                classDecl = currentPartial + currentRecord + " class " + currentName + currentTypeParams + "\n" + interfaceLine + "\n{\n";
                
                // Add view properties for disambiguation (skip composite handlers)
                foreach (var handler in handlers)
                {
                    if (compositeHandlers.Contains(handler))
                        continue;
                    
                    var propType = GetHandlerTFlowTypeName(handler);
                    if (propType is not null)
                    {
                        var propName = GetViewPropertyName(handler);
                        classDecl += "    /// <summary>Gets the " + propName + " view of the flow for typed chaining.</summary>\n";
                        classDecl += "    public " + FlowInterfaceNs + ".IFlowable<" + propType + "> " + propName + " => this;\n";
                    }
                }
                
                first = false;
            }
            else
            {
                classDecl = currentPartial + currentRecord + " class " + currentName + currentTypeParams + "\n{\n" + classDecl;
            }

            classDecl += "}\n";
            currentSymbol = currentSymbol.ContainingType;
        }

        var code = @"// <auto-generated/>
#nullable enable

namespace " + namespaceName + @";

" + classDecl + @"
";


        context.AddSource(className + ".g.cs", SourceText.From(code, Encoding.UTF8));
    }

    private static List<HandlerInfo> FindHandlerMethods(INamedTypeSymbol classSymbol)
    {
        var result = new List<HandlerInfo>();

        foreach (var member in classSymbol.GetMembers())
        {
            if (member is not IMethodSymbol { DeclaredAccessibility: Accessibility.Public, IsStatic: false } method)
                continue;

            if (!HandlerNames.Contains(method.Name))
                continue;

            // Must have FlowContext parameter
            var hasFlowContext = false;
            var flowParams = new List<IParameterSymbol>();

            foreach (var param in method.Parameters)
            {
                var paramType = param.Type.ToDisplayString();

                if (paramType == FlowContextName || paramType == FlowContextName + "?")
                {
                    hasFlowContext = true;
                }
                else if (IsFlowInterfaceType(param.Type))
                {
                    flowParams.Add(param);
                }
            }

            if (!hasFlowContext)
                continue;

            result.Add(new HandlerInfo
            {
                Method = method,
                ReturnType = method.ReturnType,
                FlowParams = flowParams
            });
        }

        return result;
    }

    /// <summary>Gets the TFlow type name for a handler method (without IFlowable wrapper).</summary>
    private static string? GetHandlerTFlowTypeName(HandlerInfo h)
    {
        var isExecute = IsExecuteReturn(h.ReturnType);
        var flowParamCount = CountFlowParams(h);

        if (isExecute)
        {
            var kind = GetKindName(h.ReturnType);
            if (flowParamCount == 1)
            {
                var p = h.FlowParams[0].Type.ToDisplayString();
                var isWrite = IsWriteInterface(h.FlowParams[0].Type);
                if (isWrite)
                    return SourceName + "<" + kind + ", " + p + ">";
                else
                    return SinkName + "<" + kind + ", " + p + ">";
            }
            else if (flowParamCount == 2)
            {
                var p1 = h.FlowParams[0].Type.ToDisplayString();
                var p2 = h.FlowParams[1].Type.ToDisplayString();
                return PipeName + "<" + kind + ", " + p1 + ", " + p2 + ">";
            }
        }
        else if (flowParamCount == 0)
        {
            var returnTypeStr = h.ReturnType.ToDisplayString();
            if (IsWriteInterface(h.ReturnType))
                return SinkName + "<" + returnTypeStr + ">";
            else
                return SourceName + "<" + returnTypeStr + ">";
        }
        else if (flowParamCount == 1)
        {
            var p = h.FlowParams[0].Type.ToDisplayString();
            var r = h.ReturnType.ToDisplayString();
            return PipeName + "<" + p + ", " + r + ">";
        }

        return null;
    }

    /// <summary>Gets a human-readable property name for a handler view.</summary>
    private static string GetViewPropertyName(HandlerInfo h)
    {
        var isExecute = IsExecuteReturn(h.ReturnType);
        var prefix = isExecute ? GetKindSimpleName(h.ReturnType) : "";
        var flowParamCount = CountFlowParams(h);

        if (isExecute && flowParamCount == 2)
        {
            // Pattern: AsyncFlowWithConsumatorToProducator
            var p1 = GetInterfaceSimpleName(h.FlowParams[0].Type);
            var p2 = GetInterfaceSimpleName(h.FlowParams[1].Type);
            return prefix + "FlowWith" + p1 + "To" + p2;
        }
        else if (isExecute && flowParamCount == 1)
        {
            var p = GetInterfaceSimpleName(h.FlowParams[0].Type);
            return prefix + "FlowWith" + p;
        }
        else if (!isExecute && flowParamCount == 0)
        {
            var r = GetInterfaceSimpleName(h.ReturnType);
            return r;
        }
        else if (!isExecute && flowParamCount == 1)
        {
            var p1 = GetInterfaceSimpleName(h.FlowParams[0].Type);
            var p2 = GetInterfaceSimpleName(h.ReturnType);
            return p1 + "To" + p2;
        }

        return "View";
    }

    /// <summary>
    /// Extracts the simple name from a flow interface type (e.g., IConsumator, IAsyncEnumerator).
    /// Uses the symbol's metadata name, so namespaced type arguments cannot corrupt the result.
    /// </summary>
    private static string GetInterfaceSimpleName(ITypeSymbol type)
    {
        var name = type.Name;

        // Remove leading "I" if present
        if (name.StartsWith("I") && name.Length > 1 && char.IsUpper(name[1]))
            name = name.Substring(1);

        return name;
    }

    /// <summary>Gets "Sync" or "Async" for a return type.</summary>
    private static string GetKindSimpleName(ITypeSymbol returnType)
    {
        var name = returnType.ToDisplayString();
        if (name == "void" || name == "System.Void")
            return "Sync";
        return "Async";
    }

    private static string? BuildSingleInterface(HandlerInfo h)
    {
        var isExecute = IsExecuteReturn(h.ReturnType);
        var flowParamCount = CountFlowParams(h);

        if (isExecute)
            return BuildExecuteBasedInterface(h);

        if (flowParamCount == 0)
            return BuildReturnBased0ParamInterface(h);

        if (flowParamCount == 1)
            return BuildReturnBased1ParamInterface(h);

        return null;
    }

    private static string BuildReturnBased0ParamInterface(HandlerInfo h)
    {
        var returnTypeStr = h.ReturnType.ToDisplayString();
        var isWrite = IsWriteInterface(h.ReturnType);

        if (isWrite)
            return "    : " + FlowInterfaceNs + ".IFlowable<" + SinkName + "<" + returnTypeStr + ">>";
        else
            return "    : " + FlowInterfaceNs + ".IFlowable<" + SourceName + "<" + returnTypeStr + ">>";
    }

    private static string BuildReturnBased1ParamInterface(HandlerInfo h)
    {
        var paramTypeStr = h.FlowParams[0].Type.ToDisplayString();
        var returnTypeStr = h.ReturnType.ToDisplayString();
        return "    : " + FlowInterfaceNs + ".IFlowable<" + PipeName + "<" + paramTypeStr + ", " + returnTypeStr + ">>";
    }

    private static string BuildExecuteBasedInterface(HandlerInfo h)
    {
        var kind = GetKindName(h.ReturnType);
        var flowParamCount = h.FlowParams.Count;

        if (flowParamCount == 1)
        {
            var paramTypeStr = h.FlowParams[0].Type.ToDisplayString();
            var isWrite = IsWriteInterface(h.FlowParams[0].Type);

            if (isWrite)
                return "    : " + FlowInterfaceNs + ".IFlowable<" + SourceName + "<" + kind + ", " + paramTypeStr + ">>";
            else
                return "    : " + FlowInterfaceNs + ".IFlowable<" + SinkName + "<" + kind + ", " + paramTypeStr + ">>";
        }
        else if (flowParamCount == 2)
        {
            var p1 = h.FlowParams[0].Type.ToDisplayString();
            var p2 = h.FlowParams[1].Type.ToDisplayString();
            return "    : " + FlowInterfaceNs + ".IFlowable<" + PipeName + "<" + kind + ", " + p1 + ", " + p2 + ">>";
        }

        return "";
    }

    private static string BuildPipe2Interface(ITypeSymbol left, ITypeSymbol right)
    {
        return "    : " + FlowInterfaceNs + ".IFlowable<" + PipeName + "<" + left.ToDisplayString() + ", " + right.ToDisplayString() + ">>";
    }

    private static string GetKindName(ITypeSymbol returnType)
    {
        var name = returnType.ToDisplayString();

        if (name == "void" || name == "System.Void")
            return SyncName;

        // Task, ValueTask, Task<T>, ValueTask<T>
        if (name is "System.Threading.Tasks.Task" or "System.Threading.Tasks.ValueTask")
            return AsyncName;

        // Generic task types
        if (returnType is INamedTypeSymbol named)
        {
            var baseName = named.ConstructedFrom.ToDisplayString();
            if (baseName is "System.Threading.Tasks.Task<TResult>" or "System.Threading.Tasks.ValueTask<TResult>")
                return AsyncName;
        }

        return SyncName;
    }

    private static bool IsExecuteReturn(ITypeSymbol returnType)
    {
        var name = returnType.ToDisplayString();
        if (name == "void" || name == "System.Void")
            return true;

        if (name is "System.Threading.Tasks.Task" or "System.Threading.Tasks.ValueTask")
            return true;

        if (returnType is INamedTypeSymbol named)
        {
            var baseName = named.ConstructedFrom.ToDisplayString();
            if (baseName is "System.Threading.Tasks.Task<TResult>" or "System.Threading.Tasks.ValueTask<TResult>")
                return true;
        }

        return false;
    }

    private static int CountFlowParams(HandlerInfo h)
    {
        return h.FlowParams.Count;
    }

    private static bool IsFlowInterfaceType(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol namedType)
            return false;

        var baseType = namedType.ConstructedFrom.ToDisplayString();

        foreach (var fi in ReadInterfaceNames)
        {
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;
        }

        foreach (var fi in WriteInterfaceNames)
        {
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;
        }

        return false;
    }

    private static bool IsReadInterface(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol namedType)
            return false;

        var baseType = namedType.ConstructedFrom.ToDisplayString();

        foreach (var fi in ReadInterfaceNames)
        {
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;
        }

        return false;
    }

    private static bool IsWriteInterface(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol namedType)
            return false;

        var baseType = namedType.ConstructedFrom.ToDisplayString();

        foreach (var fi in WriteInterfaceNames)
        {
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;
        }

        return false;
    }

    private struct HandlerInfo
    {
        public IMethodSymbol Method;
        public ITypeSymbol ReturnType;
        public List<IParameterSymbol> FlowParams;
    }
}
