﻿using System;
using NKafka.Client.Consumer.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupOffsetsDataPartition : IKafkaConsumerCoordinatorOffsetsData
    {
        public long ClientOffset;

        public long ServerOffset;

        public DateTime TimestampUtc;

        long IKafkaConsumerCoordinatorOffsetsData.ClientOffset => ClientOffset;
        long IKafkaConsumerCoordinatorOffsetsData.ServerOffset => ServerOffset;
        DateTime IKafkaConsumerCoordinatorOffsetsData.TimestampUtc => TimestampUtc;

        public KafkaCoordinatorGroupOffsetsDataPartition(long clientOffset, long serverOffset, DateTime timestampUtc)
        {
            ClientOffset = clientOffset;
            ServerOffset = serverOffset;
            TimestampUtc = timestampUtc;
        }                
    }
}
