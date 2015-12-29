using System;
using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Protocol;
using NKafka.Protocol.API.Offset;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaConsumerBrokerTopic> _topics;      
        
        private readonly TimeSpan _consumeClientTimeout;

        public KafkaConsumerBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaConsumerBrokerTopic>();            
            _consumeClientTimeout = consumePeriod + TimeSpan.FromSeconds(1) + consumePeriod;
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaConsumerBrokerPartition topicPartition)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaConsumerBrokerTopic(topicName, topicPartition.Settings), (oldKey, oldValue) => oldValue);
            }
            topicPartition.Reset();            

            topic.Partitions[topicPartition.PartitionId] = topicPartition;
        }

        public void RemoveTopicPartition([NotNull] string topicName, int partitionId)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                return;
            }

            KafkaConsumerBrokerPartition partition;
            topic.Partitions.TryRemove(partitionId, out partition);
        }

        public void Consume()
        {            
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;

                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;

                    var isPartitionReady = TryPreparePartition(partition);
                    if (!isPartitionReady)
                    {
                        continue;
                    }

                    //todo (C005) use partition for fetch request
                }
            }
        }


        private bool TryPreparePartition([NotNull] KafkaConsumerBrokerPartition partition)
        {
            if (partition.Status == KafkaConsumerBrokerPartitionStatus.NeedRearrage) return false;

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.NotInitialized)
            {
                partition.Reset();
                var request = RequestOffsets(partition.TopicName, partition.PartitionId);
                if (request.HasData)
                {
                    partition.OffsetRequestId = request.Data;
                    partition.Status = KafkaConsumerBrokerPartitionStatus.OffsetRequested;
                }
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.OffsetRequested)
            {
                var offsetRequestId = partition.OffsetRequestId;
                if (offsetRequestId == null)
                {
                    partition.Status = KafkaConsumerBrokerPartitionStatus.NotInitialized;
                    return false;
                }

                var offsetResponse = GetOffsetsResponse(offsetRequestId.Value);
                if (offsetResponse.HasData)
                {
                    bool needRearrange;
                    var minPartitionOffset = ExtractMinOffset(offsetResponse.Data, out needRearrange);
                    if (needRearrange)
                    {
                        partition.Status = KafkaConsumerBrokerPartitionStatus.NeedRearrage;
                        return false;
                    }
                    if (minPartitionOffset == null)
                    {
                        partition.Status = KafkaConsumerBrokerPartitionStatus.NotInitialized;
                        return false;
                    }

                    partition.InitOffsets(minPartitionOffset.Value);
                    partition.Status = KafkaConsumerBrokerPartitionStatus.Plugged;
                }
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.Plugged)
            {                
                return true;
            }

            return false;
        }

        #region Topic offsets

        private KafkaBrokerResult<int?> RequestOffsets([NotNull] string topicName, int partitionId)
        {
            var partitionRequest = new KafkaOffsetRequestTopicPartition(partitionId, null, 1000); // 1000 is overkill, in fact there will be 2 items.
            var topicRequest = new KafkaOffsetRequestTopic(topicName, new [] { partitionRequest });
            return _broker.Send(new KafkaOffsetRequest(new[] { topicRequest }), _consumeClientTimeout);
        }

        private KafkaBrokerResult<KafkaOffsetResponse> GetOffsetsResponse(int requestId)
        {
            var response = _broker.Receive<KafkaOffsetResponse>(requestId);
            return response;
        }

        private static long? ExtractMinOffset(KafkaOffsetResponse offsetResponse, out bool needRearrange)
        {
            needRearrange = false;
            var offsetResponseTopics = offsetResponse?.Topics;

            if (offsetResponseTopics == null || offsetResponseTopics.Count == 0)
            {
                return null;
            }

            var offsetResponsePartitions = offsetResponseTopics[0]?.Partitions;
            if (offsetResponsePartitions == null || offsetResponsePartitions.Count == 0)
            {
                return null;
            }

            var offsetResponsePartition = offsetResponsePartitions[0];
            if (offsetResponsePartition == null)
            {
                return null;
            }

            var offsets = offsetResponsePartitions[0].Offsets;
            if (offsets == null || offsets.Count == 0)
            {
                return null;
            }

            needRearrange = offsetResponsePartition.ErrorCode == KafkaResponseErrorCode.NotLeaderForPartition; //todo (E009)

            long? minOffset = null;
            foreach (var offset in offsets)
            {
                if (minOffset == null || offset < minOffset.Value)
                {
                    minOffset = offset;
                }
            }

            return minOffset;
        }

        #endregion Topic offsets
    }
}