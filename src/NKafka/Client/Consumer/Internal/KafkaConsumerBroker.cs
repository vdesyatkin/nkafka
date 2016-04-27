using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Protocol;
using NKafka.Protocol.API.Fetch;
using NKafka.Protocol.API.Offset;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaConsumerBrokerTopic> _topics;
        [NotNull] private readonly Dictionary<string, FetchRequestInfo> _fetchRequests;

        private readonly TimeSpan _consumeClientTimeout;

        public KafkaConsumerBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaConsumerBrokerTopic>();         
            _fetchRequests = new Dictionary<string, FetchRequestInfo>();   
            _consumeClientTimeout = consumePeriod + TimeSpan.FromSeconds(1) + consumePeriod;
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaConsumerBrokerPartition topicPartition)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic) || topic == null)
            {
                topic = _topics.AddOrUpdate(topicName,
                    new KafkaConsumerBrokerTopic(topicName, topicPartition.Settings, topicPartition.Coordinator),
                    (oldKey, oldValue) => oldValue);
            }
            topicPartition.Reset();

            if (topic != null)
            {
                topic.Partitions[topicPartition.PartitionId] = topicPartition;
            }
        }

        public void RemoveTopicPartition([NotNull] string topicName, int partitionId)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                return;
            }

            if (topic != null)
            {
                KafkaConsumerBrokerPartition partition;
                topic.Partitions.TryRemove(partitionId, out partition);
            }
        }

        public void Close()
        {
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition == null) continue;

                    partition.Status = KafkaConsumerBrokerPartitionStatus.RearrangeRequired;
                }                
            }

            _fetchRequests.Clear();
        }

        public void Consume()
        {            
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                ConsumeTopic(topic);
            }
        }

        private void ConsumeTopic([NotNull] KafkaConsumerBrokerTopic topic)
        {
            var oldFetchBatch = new Dictionary<int, long>();
            var newFetchBatch = new Dictionary<int, long>();

            // process requests that have already sent
            FetchRequestInfo currentRequest;
            if (_fetchRequests.TryGetValue(topic.TopicName, out currentRequest) && currentRequest != null)
            {
                var response = _broker.Receive<KafkaFetchResponse>(currentRequest.RequestId);
                if (!response.HasData && !response.HasError) // has not received
                {
                    oldFetchBatch = currentRequest.PartitionOffsets;
                }
                else
                {
                    ProcessFetchResponse(topic, response);
                    _fetchRequests.Remove(topic.TopicName);
                }
            }

            var coordinator = topic.Coordinator;
            var coordinatorPartitionOffsets = coordinator.GetPartitionOffsets(topic.TopicName);
            if (coordinatorPartitionOffsets == null)
            {
                return; // coordinator is not ready or topic is not allowed for this consumer node
            }

            // prepare new fetch batch
            foreach (var partitionPair in topic.Partitions)
            {
                var partitionId = partitionPair.Key;
                var partition = partitionPair.Value;
                if (partition == null) continue;

                IKafkaConsumerCoordinatorOffsetsData coordinatorOffset;
                if (!coordinatorPartitionOffsets.TryGetValue(partitionId, out coordinatorOffset) || coordinatorOffset == null)
                {
                    continue; // partition is not allowed for this consumer node
                }

                if (oldFetchBatch.ContainsKey(partitionId)) continue;
                if (!TryPreparePartition(partition)) continue;

                var fetchOffset = partition.CurrentOffset;
                if (coordinatorOffset.ServerOffset > fetchOffset)
                {
                    fetchOffset = coordinatorOffset.ServerOffset;
                }

                newFetchBatch[partitionId] = fetchOffset + 1;
            }
            if (newFetchBatch.Count == 0) return;

            //send new fetch batch
            var fetchRequest = CreateFetchRequest(topic, newFetchBatch);
            var fetchResult = _broker.Send(fetchRequest, _consumeClientTimeout + topic.Settings.ConsumeServerWaitTime);
            if (fetchResult.HasData)
            {
                var fetchRequestId = fetchResult.Data;
                if (fetchRequestId.HasValue)
                {
                    _fetchRequests[topic.TopicName] = new FetchRequestInfo(fetchRequestId.Value, newFetchBatch);
                }
            }
        }

        private bool TryPreparePartition([NotNull] KafkaConsumerBrokerPartition partition)
        {
            if (partition.Status == KafkaConsumerBrokerPartitionStatus.RearrangeRequired) return false;

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
                        partition.Status = KafkaConsumerBrokerPartitionStatus.RearrangeRequired;
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
                return partition.CanEnqueue();
            }

            return false;
        }

        private void ProcessFetchResponse([NotNull] KafkaConsumerBrokerTopic topic,
            KafkaBrokerResult<KafkaFetchResponse> response)
        {
            if (!response.HasData && !response.HasError) return;

            if (response.HasError)
            {
                //todo (E009)
                return;
            }

            var responseTopics = response.Data?.Topics;
            if (responseTopics == null) return;

            foreach (var responseTopic in responseTopics)
            {
                var topicName = responseTopic?.TopicName;
                if (topicName != topic.TopicName) continue;

                var responsePartitions = responseTopic.Partitions;
                if (responsePartitions == null) continue;

                foreach (var partitionResponse in responsePartitions)
                {
                    if (partitionResponse == null) continue;
                    var partitionId = partitionResponse.PartitionId;                    

                    KafkaConsumerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                    {
                        continue;
                    }

                    var errorCode = partitionResponse.ErrorCode;

                    //todo (E009)
                    if (errorCode == KafkaResponseErrorCode.NotLeaderForPartition)
                    {
                        partition.Status = KafkaConsumerBrokerPartitionStatus.RearrangeRequired;                        
                        continue;
                    }

                    if (errorCode == KafkaResponseErrorCode.NoError)
                    {
                        partition.EnqueueMessages(partitionResponse.Messages ?? new KafkaMessageAndOffset[0]);
                    }
                }
            }
        }

        #region Topic offsets

            private
            KafkaBrokerResult<int?> RequestOffsets([NotNull] string topicName, int partitionId)
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

            var offsets = offsetResponsePartition?.Offsets;
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

        #region Fetch

        private KafkaFetchRequest CreateFetchRequest([NotNull] KafkaConsumerBrokerTopic topic,
            [NotNull] Dictionary<int, long> partitionBatch)
        {
            var partitionRequests = new List<KafkaFetchRequestTopicPartition>(partitionBatch.Count);
            foreach (var paritionPair in partitionBatch)
            {
                var partitionId = paritionPair.Key;
                var patitionOffset = paritionPair.Value;
                partitionRequests.Add(new KafkaFetchRequestTopicPartition(partitionId, patitionOffset, topic.Settings.ConsumeBatchMaxSizeBytes));
            }
            var topicRequest = new KafkaFetchRequestTopic(topic.TopicName, partitionRequests);
            var fetchRequest = new KafkaFetchRequest(topic.Settings.ConsumeServerWaitTime, topic.Settings.ConsumeBatchMinSizeBytes, new [] { topicRequest});
            return fetchRequest;
        }

        private class FetchRequestInfo
        {
            public readonly int RequestId;
            [NotNull]
            public readonly Dictionary<int, long> PartitionOffsets;

            public FetchRequestInfo(int requsetId, [NotNull] Dictionary<int, long> partitionOffsets)
            {
                RequestId = requsetId;
                PartitionOffsets = partitionOffsets;
            }
        }

        #endregion Fetch                
    }
}