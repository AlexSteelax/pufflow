using Steelax.Pufflow.Operators.Abstractions;
using Steelax.Pufflow.Operators.Common;
using Steelax.Pufflow.Operators.Transforms;

namespace Steelax.Pufflow.Operators;

public static partial class OperatorExtensions
{
    
    #region producator
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IAsyncProducator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IAsyncProducator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(static (in v, s, _) => s.Invoke(v), selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IAsyncProducator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(static (in v, s, args) => s.Invoke(v, args), selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left"></param>
    /// <typeparam name="TSource"></typeparam>
    extension<TSource>(Source<IAsyncProducator<Watermarked<TSource>>> left)
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IAsyncProducator<Watermarked<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(static (in w, s, _) => new Watermarked<TTarget>(s.Invoke(w), w.Watermark), selector, default);
            return left.Next(processor.FlowAProdToAProd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IAsyncProducator<Watermarked<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(static (in w, s, args) => new Watermarked<TTarget>(s.Invoke(w, args), w.Watermark), selector, args);
            return left.Next(processor.FlowAProdToAProd);
        }
    }

    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IProducator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IProducator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(static (in w, s, _) => s.Invoke(w), selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IProducator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(static (in w, s, args) => s.Invoke(w, args), selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }

    /// <param name="left"></param>
    /// <typeparam name="TSource"></typeparam>
    extension<TSource>(Source<IProducator<Watermarked<TSource>>> left)
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IProducator<Watermarked<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(static (in w, s, _) => new Watermarked<TTarget>(s.Invoke(w.Value), w.Watermark), selector, default);
            return left.Next(processor.FlowProdToProd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IProducator<Watermarked<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(static (in w, s, args) => new Watermarked<TTarget>(s.Invoke(w.Value, args), w.Watermark), selector, args);
            return left.Next(processor.FlowProdToProd);
        }
    }
    #endregion
    
    #region consumator
    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IAsyncConsumator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IAsyncConsumator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(static (in v, s, _) => s.Invoke(v), selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IAsyncConsumator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(static (in v, s, args) => s.Invoke(v, args), selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left"></param>
    /// <typeparam name="TSource"></typeparam>
    extension<TSource>(Source<IAsyncConsumator<Watermarked<TSource>>> left)
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IAsyncConsumator<Watermarked<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(static (in w, s, _) => new Watermarked<TTarget>(s.Invoke(w), w.Watermark), selector, default);
            return left.Next(processor.FlowAConsToACons);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IAsyncConsumator<Watermarked<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(static (in w, s, args) => new Watermarked<TTarget>(s.Invoke(w, args), w.Watermark), selector, args);
            return left.Next(processor.FlowAConsToACons);
        }
    }

    /// <param name="left">The upstream push source whose elements are projected.</param>
    /// <typeparam name="TSource">The input element type.</typeparam>
    extension<TSource>(Source<IConsumator<TSource>> left)
    {
        /// <summary>
        ///     Projects each element of an async push stream through a <see cref="MapSelector{TSource,TTarget}" />,
        ///     producing a 1:1 transformed push stream.
        /// </summary>
        /// <typeparam name="TTarget">The output element type.</typeparam>
        /// <param name="selector">The pure function applied to each element to produce the output element.</param>
        /// <returns>A source emitting the projected elements downstream.</returns>
        public Source<IConsumator<TTarget>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TTarget>, Unit, TTarget>(static (in w, s, _) => s.Invoke(w), selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IConsumator<TTarget>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<TSource, MapSelector<TSource, TArgs, TTarget>, TArgs, TTarget>(static (in w, s, args) => s.Invoke(w, args), selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }

    /// <param name="left"></param>
    /// <typeparam name="TSource"></typeparam>
    extension<TSource>(Source<IConsumator<Watermarked<TSource>>> left)
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IConsumator<Watermarked<TTarget>>> Map<TTarget>(MapSelector<TSource, TTarget> selector)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TTarget>, Unit, Watermarked<TTarget>>(static (in w, s, _) => new Watermarked<TTarget>(s.Invoke(w.Value), w.Watermark), selector, default);
            return left.Next(processor.FlowConsToCons);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="args"></param>
        /// <typeparam name="TArgs"></typeparam>
        /// <typeparam name="TTarget"></typeparam>
        /// <returns></returns>
        public Source<IConsumator<Watermarked<TTarget>>> Map<TArgs, TTarget>(MapSelector<TSource, TArgs, TTarget> selector, TArgs args)
        {
            var processor = new BypassMapProcessor<Watermarked<TSource>, MapSelector<TSource, TArgs, TTarget>, TArgs, Watermarked<TTarget>>(static (in w, s, args) => new Watermarked<TTarget>(s.Invoke(w.Value, args), w.Watermark), selector, args);
            return left.Next(processor.FlowConsToCons);
        }
    }
    #endregion
}