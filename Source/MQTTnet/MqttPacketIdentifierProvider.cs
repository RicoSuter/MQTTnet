// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace MQTTnet;

public sealed class MqttPacketIdentifierProvider
{
    int _value;

    public ushort GetNextPacketIdentifier()
    {
        // Lock-free increment using Interlocked
        // Map to 1-65535 range (MQTT spec: packet identifier must never be 0)
        var newValue = Interlocked.Increment(ref _value);
        return (ushort)((uint)(newValue - 1) % 65535 + 1);
    }

    public void Reset()
    {
        Volatile.Write(ref _value, 0);
    }
}