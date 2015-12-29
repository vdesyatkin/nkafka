using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopic
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI, NotNull]
        public readonly KafkaProducerSettings Settings;

        [NotNull] private readonly IKafkaProducerTopicBuffer _buffer;
        [NotNull] private IReadOnlyList<int> _topicPartitionIds;
        [NotNull] private IReadOnlyDictionary<int, KafkaProducerTopicPartition> _topicPartitions;

        public KafkaProducerTopic([NotNull] string topicName, [NotNull] KafkaProducerSettings settings,
            [NotNull] IKafkaProducerTopicBuffer buffer)
        {
            TopicName = topicName;
            Settings = settings;
            _buffer = buffer;
            _topicPartitions = new Dictionary<int, KafkaProducerTopicPartition>();
            _topicPartitionIds = new int[0];            
        }

        public KafkaProducerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaProducerTopicPartition(TopicName, partitionId, Settings);
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
    }
}
