﻿using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionOffsetsPartitionInfo
    {
        public readonly int PartitionId;

        public readonly long? ClientOffset;

        public readonly long? ServerOffset;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupSessionOffsetsPartitionInfo(int partitionId, long? clientOffset, long? serverOffset, DateTime timestampUtc)
        {
            PartitionId = partitionId;
            ClientOffset = clientOffset;
            ServerOffset = serverOffset;
            TimestampUtc = timestampUtc;
        }
    }
}
