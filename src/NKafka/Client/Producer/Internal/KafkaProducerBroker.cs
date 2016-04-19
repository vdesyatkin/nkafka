using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Producer.Diagnostics;
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
            if (!_topics.TryGetValue(topicName, out topic) || topic == null)
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaProducerBrokerTopic(topicName, topicPartition.Settings), (oldKey, oldValue) => oldValue);
            }

            if (topic != null)
            {
                topic.Partitions[topicPartition.PartitionId] = topicPartition;
            }
        }

        public void RemoveTopicPartition([NotNull] string topicName, int partitionId)
        {
            KafkaProducerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic) || topic == null)
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
                if (topic == null) continue;
                
                var batches = CreateTopicRequests(topic, new HashSet<int>());
                foreach (var batch in batches)
                {
                    var batchRequest = CreateBatchRequest(topic, batch);
                    _broker.SendWithoutResponse(batchRequest, batch.ByteCount*2);
                }

                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition == null) continue;

                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                }
            }

            _produceRequests.Clear();
        }

        public void Produce()
        {
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                ProduceTopic(topic);
            }            
        }

        private void ProduceTopic([NotNull]KafkaProducerBrokerTopic topic)
        {
            ProduceBatchSet batchSet;
            if (!_produceRequests.TryGetValue(topic.TopicName, out batchSet) || batchSet == null)
            {
                batchSet = new ProduceBatchSet();
                _produceRequests[topic.TopicName] = batchSet;                
            }

            // process requests that have already sent
            var processedRequests = new List<int>();
            foreach (var request in batchSet.RequestBatches)
            {
                var requestId = request.Key;
                if (request.Value == null) continue;

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
                    ? _broker.SendWithoutResponse(batchRequest, batch.ByteCount * 2)
                    : _broker.Send(batchRequest, _produceClientTimeout + topic.Settings.ProduceServerTimeout, batch.ByteCount * 2);
                
                var batchRequestId = batchRequestResult.Data;
                if (batchRequestId == null)
                {
                    RollbackBatch(batchRequest, batchRequestResult.Error);
                    continue;
                }

                batchSet.RequestBatches[batchRequestId.Value] = batch;
            }
        }

        [NotNull, ItemNotNull]
        private IReadOnlyList<ProduceBatch> CreateTopicRequests([NotNull]KafkaProducerBrokerTopic topic, [NotNull] HashSet<int> busyPartitionSet)
        {
            var partitionList = new List<KafkaProducerBrokerPartition>(100);
            foreach (var partitionPair in topic.Partitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                if (partition.Status == KafkaProducerBrokerPartitionStatus.RearrangeRequired)
                {
                    continue;
                }
                if (partition.Status == KafkaProducerBrokerPartitionStatus.NotInitialized)
                {
                    partition.Status = KafkaProducerBrokerPartitionStatus.Ready;
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
            var batchByteCount = 0;
            var batchMessageCount = 0;
            var isBatchFilled = false;

            var batchMaxByteCount = topic.Settings.ProduceBatchMaxByteCount;
            var batchMaxMessageCount = topic.Settings.ProduceBatchMaxMessageCount;

            for (var i = 0; i < partitionList.Count; i++)
            {
                if (busyPartitionSet.Contains(i)) continue;

                var index = i + produceOffset;
                if (index >= partitionList.Count)
                {
                    index -= partitionList.Count;
                }

                var partition = partitionList[index];
                if (partition == null) continue;

                KafkaMessage message;
                while (partition.TryDequeueMessage(out message))
                {
                    if (message == null)
                    {
                        continue;
                    }

                    if (message.Key != null)
                    {
                        batchByteCount += message.Key.Length;
                    }
                    if (message.Data != null)
                    {
                        batchByteCount += message.Data.Length;
                    }
                    batchMessageCount++;

                    List<KafkaMessage> topicPartionMessages;
                    if (!topicBatch.Partitions.TryGetValue(partition.PartitionId, out topicPartionMessages) || topicPartionMessages == null)
                    {
                        topicPartionMessages = new List<KafkaMessage>(200);
                        topicBatch.Partitions[partition.PartitionId] = topicPartionMessages;
                    }
                    topicPartionMessages.Add(message);

                    if (batchByteCount >= batchMaxByteCount || 
                        (batchMaxMessageCount.HasValue && batchMessageCount >= batchMaxMessageCount.Value))
                    {
                        isBatchFilled = true;
                        break;
                    }
                }

                if (isBatchFilled)
                {
                    produceOffset = index + 1;
                    topicBatch.ByteCount = batchByteCount;
                    topicBatch.MessageCount = batchMessageCount;
                    requests.Add(topicBatch);
                    topicBatch = new ProduceBatch();
                    batchByteCount = 0;
                    isBatchFilled = false;
                }
            }

            if (topicBatch.Partitions.Count > 0)
            {
                topicBatch.ByteCount = batchByteCount;
                topicBatch.MessageCount = batchMessageCount;
                requests.Add(topicBatch);
            }

            topic.ProducePartitionIndex = produceOffset;

            return requests;
        }

        private void ProcessProduceResponse([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] ProduceBatch topicBatch, KafkaBrokerResult<KafkaProduceResponse> response)
        {
            if (!response.HasData && !response.HasError) return;

            if (response.HasError || !response.HasData || response.Data == null)
            {
                RollbackBatch(topic, topicBatch, response.Error);                
                return;
            }

            var responseTopics = response.Data.Topics;
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
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null) continue;
                   
                    List<KafkaMessage> batchMessages;
                    if (!topicBatch.Partitions.TryGetValue(partitionId, out batchMessages)) continue;

                    var error = partitionResponse.ErrorCode;

                    if (error != KafkaResponseErrorCode.NoError)
                    {
                        partition.RollbackMessags(batchMessages);
                        HandlePartitionError(partition, error);
                        
                        continue;
                    }

                    partition.ConfirmMessags(batchMessages);
                    partition.Error = null;
                }
            }
        }

        private void HandlePartitionError([NotNull] KafkaProducerBrokerPartition partition, KafkaResponseErrorCode responsError)
        {
            KafkaProducerTopicPartitionErrorCode error = KafkaProducerTopicPartitionErrorCode.UnknownError;
            var isRearrangeRequired = false;
            switch (responsError)
            {
                case KafkaResponseErrorCode.InvalidMessage:
                    error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaResponseErrorCode.UnknownTopicOrPartition:
                    error = KafkaProducerTopicPartitionErrorCode.UnknownTopicOrPartition;
                    isRearrangeRequired = true;
                    break;
                case KafkaResponseErrorCode.InvalidMessageSize:
                    error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaResponseErrorCode.LeaderNotAvailable:
                    error = KafkaProducerTopicPartitionErrorCode.ServerMaintenance;                    
                    break;
                case KafkaResponseErrorCode.NotLeaderForPartition:
                    error = KafkaProducerTopicPartitionErrorCode.ClientMaintenance;
                    isRearrangeRequired = true;
                    break;
                case KafkaResponseErrorCode.RequestTimedOut:
                    error = KafkaProducerTopicPartitionErrorCode.ServerTimetout;
                    break;
                case KafkaResponseErrorCode.BrokerNotAvailable:
                    error = KafkaProducerTopicPartitionErrorCode.ClientMaintenance;
                    isRearrangeRequired = true;
                    break;
                case KafkaResponseErrorCode.ReplicaNotAvailable:
                    error = KafkaProducerTopicPartitionErrorCode.ClientMaintenance;
                    isRearrangeRequired = true;
                    break;
                case KafkaResponseErrorCode.MessageSizeTooLarge:
                    error = KafkaProducerTopicPartitionErrorCode.UnknownError; //todo change limit, return message to user
                    break;
                case KafkaResponseErrorCode.InvalidTopic:
                    error = KafkaProducerTopicPartitionErrorCode.InvalidTopic;
                    isRearrangeRequired = true;
                    break;
                case KafkaResponseErrorCode.RecordListTooLarge:
                    error = KafkaProducerTopicPartitionErrorCode.ClientMaintenance; //todo change limit
                    break;
                case KafkaResponseErrorCode.NotEnoughReplicas:
                    error = KafkaProducerTopicPartitionErrorCode.NotEnoughReplicas;
                    break;
                case KafkaResponseErrorCode.NotEnoughReplicasAfterAppend:
                    error = KafkaProducerTopicPartitionErrorCode.NotEnoughReplicasAfterAppend;
                    break;
                case KafkaResponseErrorCode.InvalidRequiredAcks:
                    error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    isRearrangeRequired = true;
                    break;
                case KafkaResponseErrorCode.TopicAuthorizationFailed:
                    error = KafkaProducerTopicPartitionErrorCode.TopicAuthorizationFailed;
                    isRearrangeRequired = true;
                    break;
            }

            partition.Error = error;
            if (isRearrangeRequired)
            {
                partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
            }
        }

        private KafkaProducerTopicPartitionErrorCode? ConvertBrokerErrorCode(KafkaBrokerErrorCode? brokerError)
        {
            if (brokerError == null) return null;

            var error = KafkaProducerTopicPartitionErrorCode.UnknownError;
            switch (brokerError.Value)
            {                
                case KafkaBrokerErrorCode.Closed:
                    error = KafkaProducerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.Maintenance:
                    error = KafkaProducerTopicPartitionErrorCode.ClientMaintenance;
                    break;
                case KafkaBrokerErrorCode.BadRequest:
                    error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.ProtocolError:
                    error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TransportError:
                    error = KafkaProducerTopicPartitionErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.Timeout:
                    error = KafkaProducerTopicPartitionErrorCode.ClientTimeout;
                    break;

            }

            return error;
        }

        private void RollbackBatch([NotNull] KafkaProduceRequest request, KafkaBrokerErrorCode? brokerError)
        {
            if (request.Topics == null) return;
            var error = ConvertBrokerErrorCode(brokerError);

            foreach (var requestTopic in request.Topics)
            {
                if (requestTopic?.TopicName == null || requestTopic.Partitions == null) continue;

                KafkaProducerBrokerTopic topic;
                if (!_topics.TryGetValue(requestTopic.TopicName, out topic) || topic == null)
                {
                    continue;
                }

                foreach (var requestPartition in requestTopic.Partitions)
                {
                    if (requestPartition == null) continue;

                    KafkaProducerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(requestPartition.PartitionId, out partition) || partition == null)
                    {
                        continue;
                    }

                    partition.RollbackMessags(requestPartition.Messages);
                    if (error != null)
                    {
                        partition.Error = error;
                    }
                }
            }
        }

        private void RollbackBatch([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch, KafkaBrokerErrorCode? brokerError)
        {
            var error = ConvertBrokerErrorCode(brokerError);

            foreach (var batchPartition in batch.Partitions)
            {
                var partitionId = batchPartition.Key;
                var batchMessags = batchPartition.Value;

                KafkaProducerBrokerPartition partition;
                if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    continue;
                }

                partition.RollbackMessags(batchMessags);
                if (error != null)
                {
                    partition.Error = error;
                }
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
            public int ByteCount;
            public int MessageCount;

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

            [NotNull]
            public HashSet<int> GetBusyPartitionSet()
            {
                var result = new HashSet<int>();
                foreach (var request in RequestBatches)
                {
                    if (request.Value == null) continue;

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