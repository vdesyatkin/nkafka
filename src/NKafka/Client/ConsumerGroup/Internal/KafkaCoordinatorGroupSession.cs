﻿using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupSession
    {        
        public readonly int GenerationId;

        [CanBeNull] public readonly string MemberId;

        public readonly bool IsLeader;

        public readonly DateTime TimestampUtc;

        public KafkaCoordinatorGroupSession(int generationId, string memberId, bool isLeader, DateTime timestampUtc)
        {            
            GenerationId = generationId;
            MemberId = memberId;
            IsLeader = isLeader;
            TimestampUtc = timestampUtc;
        }
    }
}
