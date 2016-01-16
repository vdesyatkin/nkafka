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
        [NotNull] private readonly Dictionary<string, ProduceBatchSet> _produceRequests;

        private readonly TimeSpan _produceClientTimeout; 
        
        public KafkaProducerBroker([NotNull] KafkaBroker broker, TimeSpan producePeriod)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaProducerBrokerTopic>();                                    

            _produceRequests = new Dictionary<string, ProduceBatchSet>();
            _produceClientTimeout = producePeriod + TimeSpan.FromSeconds(1) + producePeriod;            
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaProducerBrokerPartition topicPartition)
        {
            KafkaProducerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaProducerBrokerTopic(topicName, topicPartition.Settings), (oldKey, oldValue) => oldValue);
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

        public void Close()
        {
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                
                var batches = CreateTopicRequests(topic, new HashSet<int>());
                foreach (var batch in batches)
                {
                    var batchRequest = CreateBatchRequest(topic, batch);
                    _broker.SendWithoutResponse(batchRequest, batch.DataSize*2);
                }

                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;

                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrageRequired;
                }
            }

            _produceRequests.Clear();
        }

        public void Produce()
        {
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;

                ProduceTopic(topic);
            }            
        }

        private void ProduceTopic([NotNull]KafkaProducerBrokerTopic topic)
        {
            ProduceBatchSet batchSet;
            if (!_produceRequests.TryGetValue(topic.TopicName, out batchSet))
            {
                batchSet = new ProduceBatchSet();
                _produceRequests[topic.TopicName] = batchSet;                
            }

            // process requests that have already sent
            var processedRequests = new List<int>();
            foreach (var request in batchSet.RequestBatches)
            {
                var requestId = request.Key;
                var response = _broker.Receive<KafkaProduceResponse>(requestId);
                if (!response.HasData && !response.HasError) continue; // has not received

                ProcessProduceResponse(topic, request.Value, response);
                processedRequests.Add(requestId);
            }
            foreach (var requestId in processedRequests)
            {
                batchSet.RequestBatches.Remove(requestId);
            }

            // send batches
            var newBatches = CreateTopicRequests(topic, batchSet.GetBusyPartitionSet());            
            foreach (var batch in newBatches)
            {
                var batchRequest = CreateBatchRequest(topic, batch);

                var batchRequestResult = topic.Settings.ConsistencyLevel == KafkaConsistencyLevel.None
                    ? _broker.SendWithoutResponse(batchRequest, batch.DataSize * 2)
                    : _broker.Send(batchRequest, _produceClientTimeout + topic.Settings.ProduceServerTimeout, batch.DataSize * 2);

                if (!batchRequestResult.HasData)
                {
                    RollbackBatch(batchRequest);
                    continue;
                }

                var batchRequestId = batchRequestResult.Data;
                if (batchRequestId == null)
                {
                    RollbackBatch(batchRequest);
                    continue;
                }

                batchSet.RequestBatches[batchRequestId.Value] = batch;
            }
        }

        private IReadOnlyList<ProduceBatch> CreateTopicRequests([NotNull]KafkaProducerBrokerTopic topic, HashSet<int> busyPartitionSet)
        {
            var partitionList = new List<KafkaProducerBrokerPartition>(100);
            foreach (var partitionPair in topic.Partitions)
            {
                var partition = partitionPair.Value;
                if (partition.Status == KafkaProducerBrokerPartitionStatus.RearrageRequired)
                {
                    continue;
                }
                if (partition.Status == KafkaProducerBrokerPartitionStatus.NotInitialized)
                {
                    partition.Status = KafkaProducerBrokerPartitionStatus.Plugged;
                }
                partitionList.Add(partition);
            }
            var produceOffset = topic.ProducePartitionIndex;
            if (produceOffset >= partitionList.Count)
            {
                produceOffset = 0;
            }

            var requests = new List<ProduceBatch>();
            var topicBatch = new ProduceBatch();
            var batchSizeBytes = 0;
            var isBatchFilled = false;

            for (var i = 0; i < partitionList.Count; i++)
            {
                if (busyPartitionSet.Contains(i)) continue;

                var index = i + produceOffset;
                if (index >= partitionList.Count)
                {
                    index -= partitionList.Count;
                }

                var partition = partitionList[index];

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

                    List<KafkaMessage> topicPartionMessages;
                    if (!topicBatch.Partitions.TryGetValue(partition.PartitionId, out topicPartionMessages))
                    {
                        topicPartionMessages = new List<KafkaMessage>(200);
                        topicBatch.Partitions[partition.PartitionId] = topicPartionMessages;
                    }
                    topicPartionMessages.Add(message);

                    if (batchSizeBytes >= topic.Settings.ProduceBatchMaxSizeBytes)
                    {
                        isBatchFilled = true;
                        break;
                    }
                }

                if (isBatchFilled)
                {
                    produceOffset = index + 1;
                    topicBatch.DataSize = batchSizeBytes;
                    requests.Add(topicBatch);
                    topicBatch = new ProduceBatch();
                    batchSizeBytes = 0;
                    isBatchFilled = false;
                }
            }

            if (topicBatch.Partitions.Count > 0)
            {
                topicBatch.DataSize = batchSizeBytes;
                requests.Add(topicBatch);
            }

            topic.ProducePartitionIndex = produceOffset;

            return requests;
        }

        private void ProcessProduceResponse([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] ProduceBatch topicBatch, KafkaBrokerResult<KafkaProduceResponse> response)
        {
            if (!response.HasData && !response.HasError) return;

            if (response.HasError)
            {
                RollbackBatch(topic, topicBatch);
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

                    KafkaProducerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition)) continue;
                   
                    List<KafkaMessage> batchMessages;
                    if (!topicBatch.Partitions.TryGetValue(partitionId, out batchMessages)) continue;

                    var error = partitionResponse.ErrorCode;

                    if (error != KafkaResponseErrorCode.NoError)
                    {
                        partition.RollbackMessags(batchMessages);

                        if (error == KafkaResponseErrorCode.NotLeaderForPartition)
                        {
                            partition.Status = KafkaProducerBrokerPartitionStatus.RearrageRequired;
                        }

                        //todo (E009) handling standard errors
                    }
                }
            }
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

        private void RollbackBatch([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch)
        {            
            foreach (var batchPartition in batch.Partitions)
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

        [NotNull]
        private KafkaProduceRequest CreateBatchRequest([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch)
        {            
            var requestPartitions = new List<KafkaProduceRequestTopicPartition>(batch.Partitions.Count);
            foreach (var batchPartiton in batch.Partitions)
            {
                var partitonId = batchPartiton.Key;
                var messages = batchPartiton.Value;
                var requestPartiton = new KafkaProduceRequestTopicPartition(partitonId, topic.Settings.CodecType, messages);
                requestPartitions.Add(requestPartiton);
            }
            var requestTopic = new KafkaProduceRequestTopic(topic.TopicName, requestPartitions);
            
            var batchRequest = new KafkaProduceRequest(topic.Settings.ConsistencyLevel, topic.Settings.ProduceServerTimeout, new [] { requestTopic});
            return batchRequest;
        }

        private class ProduceBatch
        {
            public int DataSize;

            [NotNull]
            public readonly Dictionary<int, List<KafkaMessage>> Partitions;

            public ProduceBatch()
            {
                Partitions = new Dictionary<int, List<KafkaMessage>>();
            }
        }

        private class ProduceBatchSet
        {
            [NotNull]
            public readonly Dictionary<int, ProduceBatch> RequestBatches;

            public ProduceBatchSet()
            {
                RequestBatches = new Dictionary<int, ProduceBatch>();
            }

            public HashSet<int> GetBusyPartitionSet()
            {
                var result = new HashSet<int>();
                foreach (var request in RequestBatches)
                {
                    foreach (var partition in request.Value.Partitions)
                    {
                        result.Add(partition.Key);
                    }
                }
                return result;
            }
        }        
    }
}