using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public KafkaClientTopicInfo TopicDiagnosticsInfo;

        [NotNull] private readonly KafkaProducerSettings _settings;

        [NotNull] private readonly IKafkaProducerTopicBuffer _buffer;
        [NotNull] private IReadOnlyList<int> _topicPartitionIds;
        [NotNull] private IReadOnlyDictionary<int, KafkaProducerTopicPartition> _topicPartitions;

        public KafkaProducerTopic([NotNull] string topicName,
            [NotNull] KafkaProducerSettings settings,
            [NotNull] IKafkaProducerTopicBuffer buffer)
        {
            TopicName = topicName;
            _settings = settings;
            _buffer = buffer;
            _topicPartitions = new Dictionary<int, KafkaProducerTopicPartition>();
            _topicPartitionIds = new int[0];
            TopicDiagnosticsInfo = new KafkaClientTopicInfo(topicName, DateTime.UtcNow, false, null, null);
        }

        [NotNull]
        public KafkaProducerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaProducerTopicPartition(partitionId, _settings);
        }
        
        public void ApplyPartitions([NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartition> partitions)
        {
            var topicPartitionIds = new List<int>(partitions.Count);
            var topicPartitions = new Dictionary<int, KafkaProducerTopicPartition>(partitions.Count);            

            foreach (var partition in partitions)
            {
                topicPartitionIds.Add(partition.PartitonId);
                topicPartitions[partition.PartitonId] = partition;
            }

            _topicPartitionIds = topicPartitionIds;
            _topicPartitions = topicPartitions;
        }
        
        public void Flush()
        {
            _buffer.Flush(_topicPartitionIds, _topicPartitions);
        }

        [NotNull]
        public KafkaProducerTopicInfo GetDiagnosticsInfo()
        {
            var partitionInfos = new List<KafkaProducerTopicPartitionInfo>(_topicPartitions.Count);

            long sentMessageCount = 0;
            var sendMessageTimestampUtc = (DateTime?) null;

            foreach (var partitionPair in _topicPartitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                var partitionBroker = partition.BrokerPartition;

                var partitionSentMessageCount = partitionBroker.SentMessageCount;
                var partitionSendMessageTimestampUtc = partitionBroker.SendMessageTimestampUtc;
                sentMessageCount += partitionSentMessageCount;
                if (sendMessageTimestampUtc == null || sendMessageTimestampUtc < partitionSendMessageTimestampUtc)
                {
                    sendMessageTimestampUtc = partitionSendMessageTimestampUtc;
                }

                var partitionMessagesInfo = new KafkaProducerTopicMessageCountInfo(
                    partition.EnqueuedCount + partitionBroker.RetryEnqueuedMessageCount, 
                    partition.EnqueueTimestampUtc,
                    partitionSentMessageCount, 
                    partitionSendMessageTimestampUtc);

                var partitionInfo = new KafkaProducerTopicPartitionInfo(partition.PartitonId,
                    partitionBroker.Status == KafkaProducerBrokerPartitionStatus.Ready,
                    partitionBroker.Error,
                    partitionMessagesInfo); 
                partitionInfos.Add(partitionInfo);
            }

            var topicInfo = TopicDiagnosticsInfo;

            var topicMessagesInfo = new KafkaProducerTopicMessageCountInfo(
                _buffer.EnqueuedCount, _buffer.EnqueueTimestampUtc,
                sentMessageCount, sendMessageTimestampUtc);

            return new KafkaProducerTopicInfo(TopicName, 
                topicInfo,
                DateTime.UtcNow,
                topicInfo.IsReady,
                topicMessagesInfo,
                partitionInfos                
                );
        }
    }
}
