using System.Threading.Channels;
using JetBrains.Annotations;
using Steelax.Pufflow.Abstractions;
using Steelax.Pufflow.Sdk.Test.SourceProcessors;

namespace Steelax.Pufflow.Sdk.Test;

[PublicAPI]
public static class TestSourceExtensions
{
    private static void InitialFill<T>(ChannelWriter<T> writer, IEnumerable<T>? initial)
    {
        if (initial is null)
            return;

        if (initial.Any(item => !writer.TryWrite(item)))
            throw new InvalidOperationException();
    }
    
    extension(FlowSource flowSource)
    {
        public Source<IAsyncProducator<T>> OnAsyncProducatorSource<T>(out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
        {
            var source = new FlowSourceProducator<T>();
            InitialFill(source.Writer, initial);
            
            writer = source.Writer;
            
            return flowSource.On(source.FlowAProd);
        }

        public Source<IProducator<T>> OnProducatorSource<T>(out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
        {
            var source = new FlowSourceProducator<T>();
            InitialFill(source.Writer, initial);
            
            writer = source.Writer;
            return flowSource.On(source.FlowProd);
        }

        public Source<IAsyncConsumator<T>> OnAsyncConsumatorSource<T>(out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
        {
            var source = new FlowSourceConsumator<T>();
            InitialFill(source.Writer, initial);
            
            writer = source.Writer;
            return flowSource.On(source.FlowACons);
        }

        public Source<IConsumator<T>> OnConsumatorSource<T>(out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
        {
            var source = new FlowSourceConsumator<T>();
            InitialFill(source.Writer, initial);
            
            writer = source.Writer;
            return flowSource.On(source.FlowCons);
        }

        public Source<IAsyncEnumerator<T>> OnAsyncEnumeratorSource<T>(out ChannelWriter<T> writer, IEnumerable<T>? initial = null)
        {
            var source = new FlowSourceEnumerator<T>();
            InitialFill(source.Writer, initial);
            
            writer = source.Writer;
            return flowSource.On<IAsyncEnumerator<T>>(source);
        }
    }
    
    extension(FlowSource flowSource)
    {
        public Source<IAsyncProducator<T>> OnAsyncProducatorSource<T>(IEnumerable<T> items)
        {
            ChannelWriter<T>? writer = null;
            try
            {
                return flowSource.OnAsyncProducatorSource(out writer, items);
            }
            finally
            {
                writer?.TryComplete();
            }
        }

        public Source<IProducator<T>> OnProducatorSource<T>(IEnumerable<T> items)
        {
            ChannelWriter<T>? writer = null;
            try
            {
                return flowSource.OnProducatorSource(out writer, items);
            }
            finally
            {
                writer?.TryComplete();
            }
        }

        public Source<IAsyncConsumator<T>> OnAsyncConsumatorSource<T>(IEnumerable<T> items)
        {
            ChannelWriter<T>? writer = null;
            try
            {
                return flowSource.OnAsyncConsumatorSource(out writer, items);
            }
            finally
            {
                writer?.TryComplete();
            }
        }

        public Source<IConsumator<T>> OnConsumatorSource<T>(IEnumerable<T> items)
        {
            ChannelWriter<T>? writer = null;
            try
            {
                return flowSource.OnConsumatorSource(out writer, items);
            }
            finally
            {
                writer?.TryComplete();
            }
        }

        public Source<IAsyncEnumerator<T>> OnAsyncEnumeratorSource<T>(IEnumerable<T> items)
        {
            ChannelWriter<T>? writer = null;
            try
            {
                return flowSource.OnAsyncEnumeratorSource(out writer, items);
            }
            finally
            {
                writer?.TryComplete();
            }
        }
    }
}