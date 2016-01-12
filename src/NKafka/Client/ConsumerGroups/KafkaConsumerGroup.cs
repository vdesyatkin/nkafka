﻿namespace NKafka.Client.ConsumerGroups
{
    public class KafkaConsumerGroup : IKafkaConsumerGroup
    {
        public string GroupName { get; }

        public KafkaConsumerGroupSettings Settings { get; }

        public KafkaConsumerGroup(string groupName, KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            Settings = settings;
        }
    }
}
