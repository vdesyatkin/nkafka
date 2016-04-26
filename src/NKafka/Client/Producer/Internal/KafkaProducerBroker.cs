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
                    : _broker.Send(batchRequest, _produceClientTimeout + topic.Settings.BatchServerTimeout, batch.ByteCount * 2);
                
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
                    partition.ResetLimits();
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

            var batchMaxByteCount = topic.Settings.BatchSizeByteCount;
            var batchMaxMessageCount = topic.Settings.BatchMaxMessageCount;

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

                if (partition.Error != null)
                {
                    if (DateTime.UtcNow - partition.ErrorTimestampUtc > partition.Settings.ErrorReplyPeriod)
                    {
                        continue;
                    }
                }                

                List<KafkaMessage> topicPartionMessages;
                topicBatch.Partitions.TryGetValue(partition.PartitionId, out topicPartionMessages);                

                if (topicPartionMessages?.Count >= partition.LimitInfo.MaxMessageCount)
                {
                    continue;
                }

                KafkaMessage message;
                while (partition.TryDequeueMessage(out message))
                {
                    if (message == null)
                    {
                        continue;
                    }

                    var messageSize = (message.Key?.Length ?? 0) + (message.Data?.Length ?? 0);
                    if (messageSize > partition.LimitInfo.MaxMessageSizeByteCount)
                    {                        
                        partition.FallbackMessage(message, DateTime.UtcNow, KafkaProdcuerFallbackErrorCode.TooLargeSize);                        
                        
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
                    
                    if (topicPartionMessages == null)
                    {
                        topicPartionMessages = new List<KafkaMessage>(partition.LimitInfo.MaxMessageCount ?? batchMaxMessageCount ?? 200);
                        topicBatch.Partitions[partition.PartitionId] = topicPartionMessages;
                    }
                    topicPartionMessages.Add(message);

                    if (batchByteCount >= batchMaxByteCount || batchMessageCount >= batchMaxMessageCount)
                    {
                        isBatchFilled = true;
                        break;
                    }

                    if (topicPartionMessages.Count >= partition.LimitInfo.MaxMessageCount)
                    {
                        break;
                    }
                }

                if (isBatchFilled)
                {
                    produceOffset = index + 1;
                    topicBatch.ByteCount = batchByteCount;                    
                    requests.Add(topicBatch);
                    topicBatch = new ProduceBatch();
                    batchByteCount = 0;
                    isBatchFilled = false;
                }
            }

            if (topicBatch.Partitions.Count > 0)
            {
                topicBatch.ByteCount = batchByteCount;                
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
                    if (!topicBatch.Partitions.TryGetValue(partitionId, out batchMessages) || batchMessages == null) continue;

                    HandlePartitionResponse(topic, partition, partitionResponse, batchMessages);                    
                }
            }
        }

        private void HandlePartitionResponse([NotNull] KafkaProducerBrokerTopic topic, [NotNull] KafkaProducerBrokerPartition partition, 
            [NotNull] KafkaProduceResponseTopicPartition response, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> batchMessages)
        {            
            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaProducerTopicPartitionErrorCode error;
                var isRearrangeRequired = false;

                switch (response.ErrorCode)
                {
                    //todo validate                    
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
                        error = KafkaProducerTopicPartitionErrorCode.LeaderNotAvailable;
                        break;
                    case KafkaResponseErrorCode.NotLeaderForPartition:
                        error = KafkaProducerTopicPartitionErrorCode.NotLeaderForPartition;
                        isRearrangeRequired = true;
                        break;
                    case KafkaResponseErrorCode.RequestTimedOut:
                        error = KafkaProducerTopicPartitionErrorCode.ServerTimeout;
                        break;
                    case KafkaResponseErrorCode.BrokerNotAvailable: //todo ?
                        error = KafkaProducerTopicPartitionErrorCode.LeaderNotAvailable;
                        isRearrangeRequired = true;
                        break;
                    case KafkaResponseErrorCode.ReplicaNotAvailable: //todo ?
                        error = KafkaProducerTopicPartitionErrorCode.LeaderNotAvailable;
                        isRearrangeRequired = true;
                        break;
                    case KafkaResponseErrorCode.MessageSizeTooLarge:
                        error = KafkaProducerTopicPartitionErrorCode.MessageSizeTooLarge;
                        var maxMessageSize = 2;
                        foreach (var message in batchMessages)
                        {
                            var messageSize = (message.Key?.Length ?? 0) + (message.Data?.Length ?? 0);
                            if (messageSize > maxMessageSize)
                            {
                                maxMessageSize = messageSize;
                            }
                        }
                        partition.SetMaxMessageSizeByteCount(maxMessageSize - 1);
                        break;
                    case KafkaResponseErrorCode.InvalidTopic:
                        error = KafkaProducerTopicPartitionErrorCode.InvalidTopic;
                        isRearrangeRequired = true;
                        break;
                    case KafkaResponseErrorCode.RecordListTooLarge:
                        error = KafkaProducerTopicPartitionErrorCode.RecordListTooLarge;
                        var newMessageCountLimit = Math.Max(1, (int)Math.Round(batchMessages.Count * 0.66));
                        partition.SetMaxMessageCount(newMessageCountLimit);
                        break;
                    case KafkaResponseErrorCode.NotEnoughReplicas:
                        error = KafkaProducerTopicPartitionErrorCode.NotEnoughReplicas;
                        break;
                    case KafkaResponseErrorCode.NotEnoughReplicasAfterAppend:
                        error = KafkaProducerTopicPartitionErrorCode.NotEnoughReplicasAfterAppend;
                        break;
                    case KafkaResponseErrorCode.InvalidRequiredAcks:
                        error = KafkaProducerTopicPartitionErrorCode.InvalidRequiredAcks;
                        topic.ConsistencyLevel = KafkaConsistencyLevel.OneReplica;                        
                        break;
                    case KafkaResponseErrorCode.TopicAuthorizationFailed:
                        error = KafkaProducerTopicPartitionErrorCode.TopicAuthorizationFailed;
                        isRearrangeRequired = true;
                        break;
                    default:
                        error = KafkaProducerTopicPartitionErrorCode.UnknownError;
                        isRearrangeRequired = true;
                        break;
                }
                partition.SetError(error);
                partition.RollbackMessags(batchMessages);
                if (isRearrangeRequired)
                {
                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                }
                return;
            }

            partition.ResetError();
            partition.ConfirmMessags(batchMessages);                                  
        }

        private void HandleBrokerError(
            [NotNull] KafkaProducerBrokerPartition partition,
            KafkaBrokerErrorCode? brokerError,
            [CanBeNull] IReadOnlyList<KafkaMessage> batchMessages)
        {
            if (batchMessages != null)
            {
                partition.RollbackMessags(batchMessages);
            }

            if (brokerError != null)
            {
                KafkaProducerTopicPartitionErrorCode error;
                var isRearrangeRequired = false;

                switch (brokerError.Value)
                {
                    case KafkaBrokerErrorCode.Closed:
                        error = KafkaProducerTopicPartitionErrorCode.ConnectionClosed;
                        isRearrangeRequired = true;
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
                        isRearrangeRequired = true;
                        break;
                    case KafkaBrokerErrorCode.Timeout:
                        error = KafkaProducerTopicPartitionErrorCode.ClientTimeout;
                        isRearrangeRequired = true;
                        break;
                    default:
                        error = KafkaProducerTopicPartitionErrorCode.UnknownError;
                        isRearrangeRequired = true;
                        break;
                }
                partition.SetError(error);
                
                if (isRearrangeRequired)
                {
                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                }
            }            
        }

        private void RollbackBatch([NotNull] KafkaProduceRequest request, KafkaBrokerErrorCode? brokerError)
        {
            if (request.Topics == null) return;            

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

                    HandleBrokerError(partition, brokerError, requestPartition.Messages);
                }
            }
        }

        private void RollbackBatch([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch, KafkaBrokerErrorCode? brokerError)
        {            
            foreach (var batchPartition in batch.Partitions)
            {
                var partitionId = batchPartition.Key;
                var batchMessags = batchPartition.Value;

                KafkaProducerBrokerPartition partition;
                if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    continue;
                }

                HandleBrokerError(partition, brokerError, batchMessags);
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
            
            var batchRequest = new KafkaProduceRequest(topic.ConsistencyLevel, topic.Settings.BatchServerTimeout, new [] { requestTopic});
            return batchRequest;
        }

        private class ProduceBatch
        {
            public int ByteCount;            

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