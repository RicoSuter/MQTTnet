// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using MQTTnet.Packets;

namespace MQTTnet.Internal;

public sealed class MqttPacketBus : IDisposable
{
    readonly Queue<MqttPacketBusItem>[] _partitions =
    [
        new(),
        new(),
        new()
    ];

    readonly AsyncSignal _signal = new();
    readonly object _syncRoot = new();

    int _activePartition = (int)MqttPacketBusPartition.Health;

    public int TotalItemsCount
    {
        get
        {
            lock (_syncRoot)
            {
                return _partitions.Sum(p => p.Count);
            }
        }
    }

    public void Clear()
    {
        lock (_syncRoot)
        {
            foreach (var partition in _partitions)
            {
                partition.Clear();
            }
        }
    }

    public async Task<MqttPacketBusItem> DequeueItemAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            lock (_syncRoot)
            {
                for (var i = 0; i < 3; i++)
                {
                    // Iterate through the partitions in order to ensure processing of health packets
                    // even if lots of data packets are enqueued.

                    // Partition | Messages (left = oldest).
                    // DATA      | [#]#########################
                    // CONTROL   | [#]#############
                    // HEALTH    | [#]####

                    // In this sample the 3 oldest messages from the partitions are processed in a row.
                    // Then the next 3 from all 3 partitions.

                    MoveActivePartition();

                    var activePartition = _partitions[_activePartition];

                    if (activePartition.TryDequeue(out var item))
                    {
                        return item;
                    }
                }
            }

            // No partition contains data so that we have to wait and put
            // the worker back to the thread pool.
            try
            {
                await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // The cancelled token should "hide" the disposal of the signal.
                cancellationToken.ThrowIfCancellationRequested();
                throw;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();

        throw new InvalidOperationException("MqttPacketBus is broken.");
    }

    public async Task<int> DequeueItemsAsync(MqttPacketBusItem[] buffer, int maxCount, CancellationToken cancellationToken)
    {
        if (maxCount <= 0 || buffer == null || buffer.Length < maxCount)
        {
            throw new ArgumentException("Invalid buffer or maxCount");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var count = 0;
            lock (_syncRoot)
            {
                // Dequeue up to maxCount items, cycling through partitions to maintain fairness
                while (count < maxCount)
                {
                    var foundAny = false;
                    for (var i = 0; i < 3 && count < maxCount; i++)
                    {
                        MoveActivePartition();

                        var activePartition = _partitions[_activePartition];
                        if (activePartition.TryDequeue(out var item))
                        {
                            buffer[count++] = item;
                            foundAny = true;
                        }
                    }

                    if (!foundAny)
                    {
                        break;
                    }
                }
            }

            if (count > 0)
            {
                return count;
            }

            // No partition contains data so that we have to wait
            try
            {
                await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        return 0;
    }

    public void Dispose()
    {
        _signal.Dispose();
    }

    public MqttPacketBusItem DropFirstItem(MqttPacketBusPartition partition)
    {
        lock (_syncRoot)
        {
            var partitionInstance = _partitions[(int)partition];

            if (partitionInstance.TryDequeue(out var firstItem))
            {
                return firstItem;
            }
        }

        return null;
    }

    public void EnqueueItem(MqttPacketBusItem item, MqttPacketBusPartition partition)
    {
        ArgumentNullException.ThrowIfNull(item);

        lock (_syncRoot)
        {
            _partitions[(int)partition].Enqueue(item);
            _signal.Set();
        }
    }

    public List<MqttPacket> ExportPackets(MqttPacketBusPartition partition)
    {
        lock (_syncRoot)
        {
            return _partitions[(int)partition].Select(i => i.Packet).ToList();
        }
    }

    public int PartitionItemsCount(MqttPacketBusPartition partition)
    {
        lock (_syncRoot)
        {
            return _partitions[(int)partition].Count;
        }
    }

    void MoveActivePartition()
    {
        if (_activePartition >= _partitions.Length - 1)
        {
            _activePartition = 0;
        }
        else
        {
            _activePartition++;
        }
    }
}