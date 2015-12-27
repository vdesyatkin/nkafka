using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Protocol;
using NKafka.Protocol.API.Produce;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBroker
    {        
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaProducerBrokerTopic> _topics;
        [NotNull] private readonly Dictionary<int, ProduceBatch> _produceRequests;

        private readonly int _batchMaxSizeBytes;        
        private readonly KafkaConsistencyLevel _consistencyLevel;
        private readonly KafkaCodecType _codecType;
        private readonly TimeSpan _produceOnServerTimeout;
        private readonly TimeSpan _produceTotalTimeout;

        private int _produceOffset;

        public KafkaProducerBroker([NotNull] KafkaBroker broker, [NotNull] KafkaProducerSettings settings)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaProducerBrokerTopic>();
            _batchMaxSizeBytes = settings.ProduceBatchMaxSizeBytes;            
            _consistencyLevel = settings.ConsistencyLevel;
            _codecType = settings.CodecType;
            _produceOnServerTimeout = settings.ProduceTimeout;
            if (_produceOnServerTimeout < TimeSpan.FromSeconds(1))
            {
                _produceOnServerTimeout = TimeSpan.FromSeconds(1); //todo (E006) settings server-side validation
            }
            _produceTotalTimeout = _produceOnServerTimeout +
                                   TimeSpan.FromMilliseconds(settings.ProduceTimeout.TotalMilliseconds * 2) + //todo (C002) which settings should I use?
                                   TimeSpan.FromSeconds(1);

            _produceRequests = new Dictionary<int, ProduceBatch>();

            //todo (E006) settings client-side validation
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaProducerBrokerPartition topicPartition)
        {
            KafkaProducerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaProducerBrokerTopic(topicName), (oldKey, oldValue) => oldValue);
            }

            topic.Partitions[topicPartition.PartitionId] = topicPartition;
        }

        public void RemoveTopicPartition([NotNull] string topicName, int partitionId)
        {
            KafkaProducerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                return;
            }

            KafkaProducerBrokerPartition partition;
            topic.Partitions.TryRemove(partitionId, out partition);
        }

        public void Work()
        {
            if (!CheckAllRequestsAreReceived()) return;

            var produceOffset = _produceOffset;

            var partitionList = new List<KafkaProducerBrokerPartition>(100);
            if (produceOffset >= partitionList.Count)
            {
                produceOffset = 0;
            }

            foreach (var topicPair in _topics)
            {
                foreach (var partitionPair in topicPair.Value.Partitions)
                {
                    partitionList.Add(partitionPair.Value);
                }
            }

            bool isBatchFilled;

            do
            {
                isBatchFilled = false;
                var batch = new ProduceBatch();
                var batchSizeBytes = 0;                

                for (var i = 0; i < partitionList.Count; i++)
                {
                    var index = i + produceOffset;
                    if (index >= partitionList.Count)
                    {
                        index -= partitionList.Count;
                    }

                    var partition = partitionList[index];
                    if (partition.NeedRearrange)
                    {
                        continue;
                    }

                    KafkaMessage message;
                    while (partition.TryDequeueMessage(out message))
                    {                        
                        if (message.Key != null)
                        {
                            batchSizeBytes += message.Key.Length;
                        }
                        if (message.Data != null)
                        {
                            batchSizeBytes += message.Data.Length;
                        }

                        ProduceBatchTopic batcTopic;
                        if (!batch.TryGetValue(partition.TopicName, out batcTopic))
                        {
                            batcTopic = new ProduceBatchTopic();
                            batch[partition.TopicName] = batcTopic;
                        }

                        List<KafkaMessage> topicPartionMessages;
                        if (!batcTopic.TryGetValue(partition.PartitionId, out topicPartionMessages))
                        {
                            topicPartionMessages = new List<KafkaMessage>(200); //todo (C002) default capacity?
                            batcTopic[partition.PartitionId] = topicPartionMessages;
                        }
                        topicPartionMessages.Add(message);

                        if (batchSizeBytes >= _batchMaxSizeBytes)
                        {
                            isBatchFilled = true;
                            break;
                        }
                    }

                    if (isBatchFilled)
                    {
                        produceOffset = index + 1;
                        break;
                    }
                }

                var batchRequest = CreateBatchRequest(batch);
                var batchRequestResult = _broker.Send(batchRequest, _produceTotalTimeout, batchSizeBytes * 2);
                if (!batchRequestResult.HasData)
                {
                    RollbackBatch(batchRequest);
                    break;
                }

                var batchRequestId = batchRequestResult.Data;
                if (batchRequestId == null)
                {
                    RollbackBatch(batchRequest);
                    break;
                }

                if (_consistencyLevel != KafkaConsistencyLevel.None)
                {
                    _produceRequests[batchRequestId.Value] = batch;
                    break;
                }

            } while (isBatchFilled);

            _produceOffset = produceOffset;
        }

        private bool CheckAllRequestsAreReceived()
        {
            foreach (var produceRequestPair in _produceRequests)
            {
                var batch = produceRequestPair.Value;
                var response = _broker.Receive<KafkaProduceResponse>(produceRequestPair.Key);
                if (!response.HasData && !response.HasError) continue;

                _produceRequests.Remove(produceRequestPair.Key);
                if (response.HasError)
                {
                    RollbackBatch(batch);
                    continue;
                }

                var responseData = response.Data;

                var responseTopics = responseData.Topics;
                if (responseTopics == null) continue;

                foreach (var responseTopic in responseTopics)
                {
                    if (responseTopic == null) continue;
                    var topicName = responseTopic.TopicName;
                    if (string.IsNullOrEmpty(topicName)) continue;

                    var responsePartitions = responseTopic.Partitions;
                    if (responsePartitions == null) continue;

                    KafkaProducerBrokerTopic topic;
                    if (!_topics.TryGetValue(topicName, out topic)) continue;

                    ProduceBatchTopic batchTopic;
                    if (!batch.TryGetValue(topicName, out batchTopic)) continue;

                    foreach (var partitionResponse in responsePartitions)
                    {
                        if (partitionResponse == null) continue;
                        var partitionId = partitionResponse.PartitionId;

                        KafkaProducerBrokerPartition partition;
                        if (!topic.Partitions.TryGetValue(partitionId, out partition)) continue;

                        List<KafkaMessage> batchMessages;
                        if (!batchTopic.TryGetValue(partitionId, out batchMessages)) continue;

                        var error = partitionResponse.ErrorCode;

                        if (error != KafkaResponseErrorCode.NoError)
                        {
                            partition.RollbackMessags(batchMessages);

                            if (error == KafkaResponseErrorCode.NotLeaderForPartition)
                            {
                                partition.NeedRearrange = true;
                            }

                            //todo (E009) handling standard errors
                        }
                    }
                }
            }

            return _produceRequests.Count == 0;
        }

        private void RollbackBatch([NotNull] KafkaProduceRequest request)
        {
            foreach (var requestTopic in request.Topics)
            {
                KafkaProducerBrokerTopic topic;
                if (!_topics.TryGetValue(requestTopic.TopicName, out topic))
                {
                    continue;
                }
                foreach (var requestPartition in requestTopic.Partitions)
                {
                    KafkaProducerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(requestPartition.PartitionId, out partition))
                    {
                        continue;
                    }

                    partition.RollbackMessags(requestPartition.Messages);
                }
            }
        }

        private void RollbackBatch([NotNull] ProduceBatch batch)
        {
            foreach (var batchTopic in batch)
            {
                var topicName = batchTopic.Key;
                var batchPartitions = batchTopic.Value;

                KafkaProducerBrokerTopic topic;
                if (!_topics.TryGetValue(topicName, out topic))
                {
                    continue;
                }
                foreach (var batchPartition in batchPartitions)
                {
                    var partitionId = batchPartition.Key;
                    var batchMessags = batchPartition.Value;

                    KafkaProducerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition))
                    {
                        continue;
                    }
                    partition.RollbackMessags(batchMessags);
                }
            }
        }

        [NotNull]
        private KafkaProduceRequest CreateBatchRequest([NotNull] ProduceBatch batch)
        {
            var requestTopics = new List<KafkaProduceRequestTopic>(batch.Count);
            foreach (var batchTopic in batch)
            {
                var topicName = batchTopic.Key;
                var batchTopicPartitons = batchTopic.Value;
                var requestPartitions = new List<KafkaProduceRequestTopicPartition>(batchTopicPartitons.Count);
                foreach (var batchPartiton in batchTopicPartitons)
                {
                    var partitonId = batchPartiton.Key;
                    var messages = batchPartiton.Value;
                    var requestPartiton = new KafkaProduceRequestTopicPartition(partitonId, _codecType, messages);
                    requestPartitions.Add(requestPartiton);
                }
                var requestTopic = new KafkaProduceRequestTopic(topicName, requestPartitions);
                requestTopics.Add(requestTopic);
            }

            var batchRequest = new KafkaProduceRequest(_consistencyLevel, _produceOnServerTimeout, requestTopics);
            return batchRequest;
        }

        private class ProduceBatch : Dictionary<string, ProduceBatchTopic>
        {

        }

        private class ProduceBatchTopic : Dictionary<int, List<KafkaMessage>>
        {

        }
    }
}
