// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Concurrent;

namespace MQTTnet.Internal;

#pragma warning disable CA1711

public sealed class AsyncQueue<TItem> : IDisposable
{
    readonly AsyncSignal _signal = new();
    readonly object _syncRoot = new();

    readonly ConcurrentQueue<TItem> _queue = [];

    bool _isDisposed;

    public int Count => _queue.Count;

    public void Clear()
    {
        _queue.Clear();
    }

    public void Dispose()
    {
        lock (_syncRoot)
        {
            _signal.Dispose();

            _isDisposed = true;

            if (typeof(IDisposable).IsAssignableFrom(typeof(TItem)))
            {
                while (_queue.TryDequeue(out var item))
                {
                    (item as IDisposable)?.Dispose();
                }
            }
        }
    }

    public void Enqueue(TItem item)
    {
        lock (_syncRoot)
        {
            _queue.Enqueue(item);
            _signal.Set();
        }
    }

    public AsyncQueueDequeueResult<TItem> TryDequeue()
    {
        if (_queue.TryDequeue(out var item))
        {
            return new AsyncQueueDequeueResult<TItem>(true, item);
        }

        return AsyncQueueDequeueResult<TItem>.NonSuccess;
    }

    public async Task<AsyncQueueDequeueResult<TItem>> TryDequeueAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                Task task = null;
                lock (_syncRoot)
                {
                    if (_isDisposed)
                    {
                        return AsyncQueueDequeueResult<TItem>.NonSuccess;
                    }

                    if (_queue.IsEmpty)
                    {
                        task = _signal.WaitAsync(cancellationToken);
                    }
                }

                if (task != null)
                {
                    await task.ConfigureAwait(false);
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return AsyncQueueDequeueResult<TItem>.NonSuccess;
                }

                if (_queue.TryDequeue(out var item))
                {
                    return new AsyncQueueDequeueResult<TItem>(true, item);
                }
            }
            catch (OperationCanceledException)
            {
                return AsyncQueueDequeueResult<TItem>.NonSuccess;
            }
        }

        return AsyncQueueDequeueResult<TItem>.NonSuccess;
    }

    /// <summary>
    /// Dequeues up to maxCount items into the buffer. Waits for at least one item if the queue is empty.
    /// Returns the number of items dequeued.
    /// </summary>
    public async Task<int> TryDequeueBatchAsync(TItem[] buffer, int maxCount, CancellationToken cancellationToken)
    {
        if (maxCount <= 0 || buffer == null || buffer.Length < maxCount)
        {
            return 0;
        }

        // Wait for at least one item
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                Task task = null;
                lock (_syncRoot)
                {
                    if (_isDisposed)
                    {
                        return 0;
                    }

                    if (_queue.IsEmpty)
                    {
                        task = _signal.WaitAsync(cancellationToken);
                    }
                }

                if (task != null)
                {
                    await task.ConfigureAwait(false);
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return 0;
                }

                // Dequeue as many items as available, up to maxCount
                var count = 0;
                while (count < maxCount && _queue.TryDequeue(out var item))
                {
                    buffer[count++] = item;
                }

                if (count > 0)
                {
                    return count;
                }
            }
            catch (OperationCanceledException)
            {
                return 0;
            }
        }

        return 0;
    }
}