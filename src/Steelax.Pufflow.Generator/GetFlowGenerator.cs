using System.Collections.Immutable;
using System.Diagnostics;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Steelax.Pufflow.Generator;

/// <summary>
///     An incremental Roslyn source generator that produces IFlowable interface implementations
///     implementations for classes annotated with the <c>[Flow]</c> attribute.
/// </summary>
/// <remarks>
///     The generator inspects the public, non-static <c>Fuse(...)</c> handler methods of a flow-annotated
///     class and emits a partial class implementing the appropriate IFlowable (Source, Sink, or Pipe)
///     interface, plus disambiguation view properties. The node's role is derived from the Fuse parameter
///     directions (out → Source, explicit in / plain read → Sink, two parameters → Pipe).
/// </remarks>
[Generator]
public class GetFlowGenerator : IIncrementalGenerator
{
    private const string FlowContextName = "Steelax.Pufflow.FlowContext";
    private const string FlowInterfaceNs = "Steelax.Pufflow.Abstractions";
    private const string ReadInterfacesNs = "System.Collections.Generic";

    private const string SourceName = "Steelax.Pufflow.Source";
    private const string SinkName = "Steelax.Pufflow.Sink";
    private const string PipeName = "Steelax.Pufflow.Pipe";
    private static readonly string[] HandlerNames = ["Fuse"];

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

    /// <summary>
    ///     Initializes the incremental generator pipeline.
    /// </summary>
    /// <param name="context">The incremental generator initialization context.</param>
    /// <remarks>
    ///     Registers a syntax provider that filters class declarations with attributes
    ///     and produces the <c>[Flow]</c>-annotated class symbols, then combines them
    ///     with the compilation to emit the generated source.
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

        var flowAttributeType =
            semanticModel.Compilation.GetTypeByMetadataName("Steelax.Pufflow.Abstractions.FlowAttribute");

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
            try
            {
                GenerateForClass(context, classSymbol);
            }
            catch (Exception ex)
            {
                context.ReportDiagnostic(
                    Diagnostic.Create(
                        new DiagnosticDescriptor(
                            "DFG001",
                            "GetFlowGenerator Error",
                            "Error generating GetFlow for {0}: {1}",
                            "GetFlowGenerator",
                            DiagnosticSeverity.Warning,
                            true),
                        classSymbol.Locations.FirstOrDefault(),
                        classSymbol.Name,
                        ex.Message));
            }
    }

    private static void GenerateForClass(SourceProductionContext context, INamedTypeSymbol classSymbol)
    {
        var handlers = FindHandlerMethods(classSymbol);

        if (handlers.Count == 0)
            return;

        var interfaceLines = new List<string>();

        foreach (var handler in handlers)
            interfaceLines.Add(BuildFuseBasedInterface(handler));

        if (interfaceLines.Count == 0)
            return;

        // Build combined interface declaration
        var interfaceLine = interfaceLines[0];
        for (var i = 1; i < interfaceLines.Count; i++)
        {
            var line = interfaceLines[i].TrimStart();
            if (line.StartsWith(": "))
                line = line.Substring(2);
            interfaceLine += ",\n      " + line;
        }

        Trace.WriteLine(
            $"[GetFlowGenerator] GenerateForClass: class='{classSymbol.ToDisplayString()}', typeParams={classSymbol.TypeParameters.Length}");

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
                currentTypeParams = "<" + string.Join(", ", currentSymbol.TypeParameters.Select(tp => tp.Name)) + ">";

            var currentRecord = currentSymbol.IsRecord ? " record" : "";
            var currentPartial = currentSymbol.DeclaredAccessibility == Accessibility.Public
                ? "public partial"
                : "partial";

            // First (innermost) gets the interface + view properties, rest are containers
            if (first)
            {
                classDecl = currentPartial + currentRecord + " class " + currentName + currentTypeParams + "\n" +
                            interfaceLine + "\n{\n";

                // Add view properties for disambiguation.
                foreach (var handler in handlers)
                {
                    var propType = GetHandlerTFlowTypeName(handler);
                    if (propType is not null)
                    {
                        var propName = GetViewPropertyName(handler);
                        classDecl += "    /// <summary>Gets the " + propName +
                                     " view of the flow for typed chaining.</summary>\n";
                        classDecl += "    public " + FlowInterfaceNs + ".IFlowable<" + propType + "> " + propName +
                                     " => this;\n";
                    }
                }

                first = false;
            }
            else
            {
                classDecl = currentPartial + currentRecord + " class " + currentName + currentTypeParams + "\n{\n" +
                            classDecl;
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
                Trace.WriteLine($"[GetFlowGenerator] FindHandlerMethods: method='{method.Name}' param='{paramType}' isRef='{param.RefKind}' isFlow='{IsFlowInterfaceType(param.Type)}'");

                if (paramType == FlowContextName || paramType == FlowContextName + "?")
                    hasFlowContext = true;
                else if (IsFlowInterfaceType(param.Type)) flowParams.Add(param);
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
        var flowParamCount = CountFlowParams(h);

        if (flowParamCount == 1)
        {
            var p = h.FlowParams[0].Type.ToDisplayString();
            var refKind = h.FlowParams[0].RefKind;
            var isWrite = IsWriteInterface(h.FlowParams[0].Type);

            // Fuse role for a single parameter:
            //  - out + read (IEnumerator/IConsumator): the node emits a stream → Source;
            //  - out + write (IProducator): the node hands out a target to be written into → Sink (passive consumer);
            //  - plain write (IProducator): the node pushes into the supplied target → Source;
            //  - plain/explicit-in read: the node consumes the supplied source → Sink.
            return (refKind == RefKind.Out && !isWrite) || (refKind == RefKind.None && isWrite)
                ? SourceName + "<" + p + ">"
                : SinkName + "<" + p + ">";
        }

        if (flowParamCount == 2)
        {
            var p1 = h.FlowParams[0].Type.ToDisplayString();
            var p2 = h.FlowParams[1].Type.ToDisplayString();
            return PipeName + "<" + p1 + ", " + p2 + ">";
        }

        return null;
    }

    /// <summary>Gets a human-readable property name for a handler view.</summary>
    private static string GetViewPropertyName(HandlerInfo h)
    {
        var flowParamCount = CountFlowParams(h);

        if (flowParamCount == 2)
        {
            // Pattern: FlowEnumToAProd (Fuse pipe: read → write)
            var p1 = GetInterfaceShortName(h.FlowParams[0].Type);
            var p2 = GetInterfaceShortName(h.FlowParams[1].Type);
            return "Flow" + p1 + "To" + p2;
        }

        if (flowParamCount == 1)
        {
            // Pattern: FlowAProd (source) / FlowEnum (sink)
            return "Flow" + GetInterfaceShortName(h.FlowParams[0].Type);
        }

        return "View";
    }

    /// <summary>
    ///     Extracts the short (abbreviated) name from a flow interface type, e.g.:
    ///     IEnumerator → "Enum", IAsyncEnumerator → "AEnum",
    ///     IConsumator → "Cons", IAsyncConsumator → "ACons",
    ///     IProducator → "Prod", IAsyncProducator → "AProd".
    /// </summary>
    private static string GetInterfaceShortName(ITypeSymbol type)
    {
        var name = type.Name;

        // Remove leading "I" if present
        if (name.StartsWith("I") && name.Length > 1 && char.IsUpper(name[1]))
            name = name.Substring(1);

        return name switch
        {
            "Enumerator" => "Enum",
            "AsyncEnumerator" => "AEnum",
            "Consumator" => "Cons",
            "AsyncConsumator" => "ACons",
            "Producator" => "Prod",
            "AsyncProducator" => "AProd",
            _ => name
        };
    }

    private static string? BuildSingleInterface(HandlerInfo h)
    {
        return BuildFuseBasedInterface(h);
    }

    private static string BuildFuseBasedInterface(HandlerInfo h)
    {
        var flowParamCount = h.FlowParams.Count;

        if (flowParamCount == 1)
        {
            var paramTypeStr = h.FlowParams[0].Type.ToDisplayString();
            var refKind = h.FlowParams[0].RefKind;
            var isWrite = IsWriteInterface(h.FlowParams[0].Type);

            // For Fuse the node's role is determined by the parameter direction and family:
            //  - out + read (IEnumerator/IConsumator): the node emits a stream → Source;
            //  - out + write (IProducator): the node hands out a target to be written into → Sink;
            //  - plain write (IProducator): the node pushes into the supplied target → Source;
            //  - plain/explicit-in read: the node consumes the supplied source → Sink.
            if ((refKind == RefKind.Out && !isWrite) || (refKind == RefKind.None && isWrite))
                return "    : " + FlowInterfaceNs + ".IFlowable<" + SourceName + "<" + paramTypeStr + ">>";
            return "    : " + FlowInterfaceNs + ".IFlowable<" + SinkName + "<" + paramTypeStr + ">>";
        }

        if (flowParamCount == 2)
        {
            var p1 = h.FlowParams[0].Type.ToDisplayString();
            var p2 = h.FlowParams[1].Type.ToDisplayString();
            return "    : " + FlowInterfaceNs + ".IFlowable<" + PipeName + "<" + p1 + ", " + p2 + ">>";
        }

        return "";
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
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;

        foreach (var fi in WriteInterfaceNames)
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;

        return false;
    }

    private static bool IsReadInterface(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol namedType)
            return false;

        var baseType = namedType.ConstructedFrom.ToDisplayString();

        foreach (var fi in ReadInterfaceNames)
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;

        return false;
    }

    private static bool IsWriteInterface(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol namedType)
            return false;

        var baseType = namedType.ConstructedFrom.ToDisplayString();

        foreach (var fi in WriteInterfaceNames)
            if (baseType == fi || baseType.StartsWith(fi + "<"))
                return true;

        return false;
    }

    private struct HandlerInfo
    {
        public IMethodSymbol Method;
        public ITypeSymbol ReturnType;
        public List<IParameterSymbol> FlowParams;
    }
}