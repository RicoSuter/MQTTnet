// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using MQTTnet.Packets;

namespace MQTTnet.PacketDispatcher;

public sealed class MqttPacketDispatcher : IDisposable
{
    // Dictionary for O(1) lookup by (identifier, type)
    readonly Dictionary<(ushort Identifier, Type Type), List<IMqttPacketAwaitable>> _waitersMap = new();
    readonly object _syncRoot = new();

    bool _isDisposed;

    public MqttPacketAwaitable<TResponsePacket> AddAwaitable<TResponsePacket>(ushort packetIdentifier) where TResponsePacket : MqttPacket
    {
        var awaitable = new MqttPacketAwaitable<TResponsePacket>(packetIdentifier, this);
        var key = (packetIdentifier, typeof(TResponsePacket));

        lock (_syncRoot)
        {
            if (!_waitersMap.TryGetValue(key, out var list))
            {
                list = new List<IMqttPacketAwaitable>(1);
                _waitersMap[key] = list;
            }
            list.Add(awaitable);
        }

        return awaitable;
    }

    public void CancelAll()
    {
        lock (_syncRoot)
        {
            foreach (var kvp in _waitersMap)
            {
                foreach (var awaitable in kvp.Value)
                {
                    awaitable.Cancel();
                }
            }

            _waitersMap.Clear();
        }
    }

    public void Dispose()
    {
        Dispose(new ObjectDisposedException(nameof(MqttPacketDispatcher)));
    }

    public void Dispose(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        lock (_syncRoot)
        {
            FailAll(exception);

            // Make sure that no task can start waiting after this instance is already disposed.
            // This will prevent unexpected freezes.
            _isDisposed = true;
        }
    }

    public void FailAll(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        lock (_syncRoot)
        {
            foreach (var kvp in _waitersMap)
            {
                foreach (var awaitable in kvp.Value)
                {
                    awaitable.Fail(exception);
                }
            }

            _waitersMap.Clear();
        }
    }

    public void RemoveAwaitable(IMqttPacketAwaitable awaitable)
    {
        ArgumentNullException.ThrowIfNull(awaitable);

        var key = (awaitable.Filter.Identifier, awaitable.Filter.Type);

        lock (_syncRoot)
        {
            if (_waitersMap.TryGetValue(key, out var list))
            {
                list.Remove(awaitable);
                if (list.Count == 0)
                {
                    _waitersMap.Remove(key);
                }
            }
        }
    }

    public bool TryDispatch(MqttPacket packet)
    {
        ArgumentNullException.ThrowIfNull(packet);

        ushort identifier = 0;
        if (packet is MqttPacketWithIdentifier packetWithIdentifier)
        {
            identifier = packetWithIdentifier.PacketIdentifier;
        }

        var packetType = packet.GetType();
        var key = (identifier, packetType);
        List<IMqttPacketAwaitable> matchingWaiters = null;

        lock (_syncRoot)
        {
            ThrowIfDisposed();

            // O(1) lookup by key
            if (_waitersMap.TryGetValue(key, out var list) && list.Count > 0)
            {
                // Take all matching waiters (usually just one)
                matchingWaiters = new List<IMqttPacketAwaitable>(list);
                list.Clear();
                _waitersMap.Remove(key);
            }
        }

        if (matchingWaiters != null)
        {
            foreach (var matchingEntry in matchingWaiters)
            {
                matchingEntry.Complete(packet);
            }
            return true;
        }

        return false;
    }

    void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
    }
}