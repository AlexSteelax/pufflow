using Steelax.Pufflow.Common;

namespace Steelax.Pufflow.Tests.Common;

public static class ReusableBufferTests
{
    public sealed class TryAdd
    {
        [Fact]
        public void ShouldAddItem_WhenBufferIsNotFull()
        {
            var buffer = new ReusableBuffer<int>(3);

            var result = buffer.TryAdd(42);

            Assert.True(result);
            Assert.Equal(1, buffer.Count);
        }

        [Fact]
        public void ShouldReturnFalse_WhenBufferIsFull()
        {
            var buffer = new ReusableBuffer<int>(2);

            Assert.True(buffer.TryAdd(1));
            Assert.True(buffer.TryAdd(2));
            var result = buffer.TryAdd(3);

            Assert.False(result);
            Assert.Equal(2, buffer.Count);
        }
    }

    public sealed class Enumeration
    {
        [Fact]
        public void ToArray_ShouldReturnAllAddedItems()
        {
            var buffer = new ReusableBuffer<int>(5);
            buffer.TryAdd(10);
            buffer.TryAdd(20);
            buffer.TryAdd(30);

            var result = buffer.ToArray();

            Assert.Equal([10, 20, 30], result);
        }

        [Fact]
        public void Foreach_ShouldIterateOverAllItems()
        {
            var buffer = new ReusableBuffer<int>(4);
            buffer.TryAdd(1);
            buffer.TryAdd(2);
            buffer.TryAdd(3);

            var items = new List<int>();
            foreach (var item in buffer)
                items.Add(item);

            Assert.Equal([1, 2, 3], items);
        }

        [Fact]
        public void EmptyBuffer_ShouldReturnEmpty()
        {
            var buffer = new ReusableBuffer<int>(3);

            var result = buffer.ToArray();

            Assert.Empty(result);
        }
    }

    public sealed class Reset
    {
        [Fact]
        public void ShouldClearBuffer_AndAllowReuse()
        {
            var buffer = new ReusableBuffer<string>(3);
            buffer.TryAdd("A");
            buffer.TryAdd("B");
            Assert.Equal(2, buffer.Count);

            buffer.Reset();
            Assert.Equal(0, buffer.Count);

            buffer.TryAdd("C");
            Assert.Equal(["C"], buffer.ToArray());
        }

        [Fact]
        public void EmptyBuffer_ShouldBeSafe()
        {
            var buffer = new ReusableBuffer<int>(3);

            buffer.Reset();

            Assert.Equal(0, buffer.Count);
            Assert.Empty(buffer.ToArray());
        }

        [Fact]
        public void MultipleCycles_ShouldWork()
        {
            var buffer = new ReusableBuffer<int>(3);

            for (var cycle = 0; cycle < 5; cycle++)
            {
                buffer.TryAdd(cycle * 10 + 1);
                buffer.TryAdd(cycle * 10 + 2);

                Assert.Equal([cycle * 10 + 1, cycle * 10 + 2], buffer.ToArray());

                buffer.Reset();
                Assert.Equal(0, buffer.Count);
            }
        }
    }

    public sealed class AsSpan
    {
        [Fact]
        public void ShouldReturnAllBufferedItems()
        {
            var buffer = new ReusableBuffer<int>(5);
            buffer.TryAdd(100);
            buffer.TryAdd(200);

            var span = buffer.AsSpan();

            Assert.Equal(2, span.Length);
            Assert.Equal(100, span[0]);
            Assert.Equal(200, span[1]);
        }

        [Fact]
        public void EmptyBuffer_ShouldReturnEmptySpan()
        {
            var buffer = new ReusableBuffer<int>(3);

            var span = buffer.AsSpan();

            Assert.True(span.IsEmpty);
        }
    }

    public sealed class Count
    {
        [Fact]
        public void ShouldReflectAddedItems()
        {
            var buffer = new ReusableBuffer<int>(10);

            Assert.Equal(0, buffer.Count);
            buffer.TryAdd(1);
            Assert.Equal(1, buffer.Count);
            buffer.TryAdd(2);
            Assert.Equal(2, buffer.Count);
        }

        [Fact]
        public void ShouldBeZero_AfterReset()
        {
            var buffer = new ReusableBuffer<int>(5);
            buffer.TryAdd(1);
            buffer.TryAdd(2);
            Assert.Equal(2, buffer.Count);

            buffer.Reset();

            Assert.Equal(0, buffer.Count);
        }
    }

    public sealed class Dispose
    {
        [Fact]
        public void MultipleCalls_ShouldBeSafe()
        {
            var buffer = new ReusableBuffer<int>(3);

            buffer.Dispose();
            buffer.Dispose();

            Assert.Equal(0, buffer.Count);
        }
    }

    public sealed class ReferenceType
    {
        [Fact]
        public void ResetShouldClearStaleReferences()
        {
            var buffer = new ReusableBuffer<string>(3);
            buffer.TryAdd("hello");
            buffer.TryAdd("world");

            buffer.Reset();

            buffer.TryAdd("new");
            Assert.Equal(["new"], buffer.ToArray());
        }
    }

    public sealed class Enumerator
    {
        [Fact]
        public void ShouldAdvanceCorrectly()
        {
            var buffer = new ReusableBuffer<int>(3);
            buffer.TryAdd(1);
            buffer.TryAdd(2);

            using var enumerator = buffer.GetEnumerator();

            Assert.True(enumerator.MoveNext());
            Assert.Equal(1, enumerator.Current);

            Assert.True(enumerator.MoveNext());
            Assert.Equal(2, enumerator.Current);

            Assert.False(enumerator.MoveNext());
        }

        [Fact]
        public void GetEnumerator_ShouldReturnSameInstance()
        {
            var buffer = new ReusableBuffer<int>(3);
            buffer.TryAdd(42);

            using var enumerator1 = buffer.GetEnumerator();
            using var enumerator2 = buffer.GetEnumerator();

            Assert.Same(enumerator1, enumerator2);
        }

        [Fact]
        public void CurrentBeforeMoveNext_ShouldThrow()
        {
            var buffer = new ReusableBuffer<int>(3);
            using var enumerator = buffer.GetEnumerator();

            Assert.Throws<IndexOutOfRangeException>(() => enumerator.Current);
        }
    }

    public sealed class LargeData
    {
        [Fact]
        public void LargeNumberOfItems_ShouldWork()
        {
            const int capacity = 1000;
            var buffer = new ReusableBuffer<int>(capacity);

            for (var i = 0; i < capacity; i++)
                Assert.True(buffer.TryAdd(i));

            Assert.Equal(capacity, buffer.Count);
            Assert.Equal(Enumerable.Range(0, capacity), buffer.ToArray());
        }
    }

    public sealed class ValueType
    {
        [Fact]
        public void ShouldNotRetainOldValues_AfterReset()
        {
            var buffer = new ReusableBuffer<int>(3);
            buffer.TryAdd(10);
            buffer.TryAdd(20);

            buffer.Reset();
            buffer.TryAdd(30);

            // Single enumeration to verify (self-enumerator reuses instance)
            var items = buffer.ToArray();
            Assert.DoesNotContain(10, items);
            Assert.Single(items);
            Assert.Equal(30, items[0]);
        }
    }
}
