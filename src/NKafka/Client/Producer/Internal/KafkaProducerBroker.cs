using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Producer.Diagnostics;
using NKafka.Client.Producer.Logging;
using NKafka.Connection;
using NKafka.Connection.Diagnostics;
using NKafka.Protocol;
using NKafka.Protocol.API.Produce;

namespace NKafka.Client.Producer.Internal
{    
    //todo (E013) log ready and fix reset error (by partitions?)
    internal sealed class KafkaProducerBroker
    {        
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly IKafkaClientBroker _clientBroker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaProducerBrokerTopic> _topics;
        [NotNull] private readonly Dictionary<string, ProduceBatchSet> _produceRequests;

        private readonly TimeSpan _produceClientTimeout;       
        
        public KafkaProducerBroker([NotNull] KafkaBroker broker, [NotNull] IKafkaClientBroker clientBroker, TimeSpan producePeriod)
        {
            _broker = broker;
            _clientBroker = clientBroker;
            _topics = new ConcurrentDictionary<string, KafkaProducerBrokerTopic>();                                    

            _produceRequests = new Dictionary<string, ProduceBatchSet>();
            _produceClientTimeout = producePeriod + TimeSpan.FromSeconds(1) + producePeriod;            
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaProducerBrokerPartition topicPartition)
        {
            KafkaProducerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic) || topic == null)
            {
                var topicProducerName = $"topic(producer)[{topicName}]";
                var brokerTopic = new KafkaProducerBrokerTopic(topicName, topicProducerName, topicPartition.Settings);
                topic = _topics.AddOrUpdate(topicName, brokerTopic, (oldKey, oldValue) => oldValue);
            }

            if (topic != null)
            {
                topic.Partitions[topicPartition.PartitionId] = topicPartition;
                if (topic.Logger == null)
                {
                    topic.Logger = topicPartition.Logger;
                }
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

        public void Start()
        {            
        }

        public void Stop()
        {
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;
                
                var batches = CreateTopicRequests(topic, new HashSet<int>());
                foreach (var batch in batches)
                {
                    var batchRequest = CreateBatchRequest(topic, batch);
                    _broker.SendWithoutResponse(batchRequest, topic.TopicProducerName, batch.ByteCount*2);
                }

                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition == null) continue;

                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                    partition.Clear();
                }
            }

            _produceRequests.Clear();
        }

        public void Produce(CancellationToken cancellation)
        {            
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                if (cancellation.IsCancellationRequested) return;
                ProduceTopic(topic, cancellation);
            }            
        }

        private void ProduceTopic([NotNull]KafkaProducerBrokerTopic topic, CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;

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

                HandleProduceResponse(topic, request.Value, response);
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
                var batchTimeout = _produceClientTimeout + topic.Settings.BatchServerTimeout;

                var batchRequestResult = topic.Settings.ConsistencyLevel == KafkaConsistencyLevel.None
                    ? _broker.SendWithoutResponse(batchRequest, topic.TopicProducerName, batch.ByteCount * 2)
                    : _broker.Send(batchRequest, topic.TopicProducerName, batchTimeout, batch.ByteCount * 2);
                
                var batchRequestId = batchRequestResult.Data;
                if (batchRequestId == null)
                {
                    RollbackBatch(topic, batch, batchRequestResult.Error ?? KafkaBrokerErrorCode.TransportError, "SendProduceRequest");
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
                    partition.ResetData();
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

            var batchMaxByteCount = topic.BatchSizeByteCount;
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

                if (partition.Status == KafkaProducerBrokerPartitionStatus.Error)
                {
                    if (DateTime.UtcNow - partition.ErrorTimestampUtc > partition.Settings.ErrorRetryPeriod)
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

        private void HandleProduceResponse([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] ProduceBatch topicBatch, KafkaBrokerResult<KafkaProduceResponse> response)
        {
            if (!response.HasData && !response.HasError) return;

            if (response.HasError || !response.HasData || response.Data == null)
            {
                var brokerError = response.Error ?? KafkaBrokerErrorCode.TransportError;
                RollbackBatch(topic, topicBatch, brokerError, "ReceiveProduceResponse");                
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

                    if (!TryHandlePartitionResponse(topic, partition, partitionResponse, batchMessages))
                    {
                        continue;
                    }

                    ResetPartitionError(partition);
                    partition.Status = KafkaProducerBrokerPartitionStatus.Ready;
                }
            }
        }

        private bool TryHandlePartitionResponse([NotNull] KafkaProducerBrokerTopic topic, [NotNull] KafkaProducerBrokerPartition partition, 
            [NotNull] KafkaProduceResponseTopicPartition response, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> batchMessages)
        {            
            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaProducerTopicPartitionErrorCode error;
                ProducerErrorType errorType;

                switch (response.ErrorCode)
                {
                    case KafkaResponseErrorCode.InvalidMessage:
                        error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                        errorType = ProducerErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.UnknownTopicOrPartition:
                        error = KafkaProducerTopicPartitionErrorCode.UnknownTopicOrPartition;
                        errorType = ProducerErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.InvalidMessageSize:
                        error = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                        errorType = ProducerErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.LeaderNotAvailable:
                        error = KafkaProducerTopicPartitionErrorCode.LeaderNotAvailable;
                        errorType = ProducerErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.NotLeaderForPartition:
                        error = KafkaProducerTopicPartitionErrorCode.NotLeaderForPartition;
                        errorType = ProducerErrorType.Rebalance;
                        break;
                    case KafkaResponseErrorCode.RequestTimedOut:
                        error = KafkaProducerTopicPartitionErrorCode.ServerTimeout;
                        errorType = ProducerErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.BrokerNotAvailable:
                        error = KafkaProducerTopicPartitionErrorCode.LeaderNotAvailable;
                        errorType = ProducerErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.ReplicaNotAvailable:
                        error = KafkaProducerTopicPartitionErrorCode.ReplicaNotAvailable;
                        errorType = ProducerErrorType.Warning;
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
                        errorType = ProducerErrorType.Warning;
                        break;
                    case KafkaResponseErrorCode.InvalidTopic:
                        error = KafkaProducerTopicPartitionErrorCode.InvalidTopic;
                        errorType = ProducerErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.RecordListTooLarge:
                        error = KafkaProducerTopicPartitionErrorCode.RecordListTooLarge;
                        var newMessageCountLimit = Math.Max(1, (int)Math.Round(batchMessages.Count * 0.66));
                        partition.SetMaxMessageCount(newMessageCountLimit);
                        errorType = ProducerErrorType.Warning;
                        break;
                    case KafkaResponseErrorCode.NotEnoughReplicas:
                        error = KafkaProducerTopicPartitionErrorCode.NotEnoughReplicas;
                        errorType = ProducerErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.NotEnoughReplicasAfterAppend:
                        error = KafkaProducerTopicPartitionErrorCode.NotEnoughReplicasAfterAppend;
                        errorType = ProducerErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.InvalidRequiredAcks:
                        error = KafkaProducerTopicPartitionErrorCode.InvalidRequiredAcks;
                        if (topic.ConsistencyLevel == KafkaConsistencyLevel.None ||
                            topic.ConsistencyLevel == KafkaConsistencyLevel.OneReplica ||
                            topic.ConsistencyLevel == KafkaConsistencyLevel.AllReplicas)
                        {
                            errorType = ProducerErrorType.Error;
                        }
                        else
                        {
                            topic.ConsistencyLevel = KafkaConsistencyLevel.OneReplica;
                            errorType = ProducerErrorType.Warning;
                        }                        
                        break;
                    case KafkaResponseErrorCode.TopicAuthorizationFailed:
                        error = KafkaProducerTopicPartitionErrorCode.TopicAuthorizationFailed;
                        errorType = ProducerErrorType.Rearrange;
                        break;
                    default:
                        error = KafkaProducerTopicPartitionErrorCode.UnknownError;
                        errorType = ProducerErrorType.Rearrange;
                        break;
                }
                
                SetPartitionError(partition, error, errorType);
                partition.RollbackMessags(batchMessages);

                var logger = partition.Logger;
                if (logger != null)
                {
                    var errorInfo = new KafkaProducerTopicProtocolErrorInfo(partition.PartitionId, error, "ProduceResponse", _clientBroker, batchMessages.Count);
                    if (errorType == ProducerErrorType.Warning)
                    {
                        logger.OnProtocolWarning(errorInfo);
                        return false;
                    }

                    if (errorType == ProducerErrorType.Rebalance)
                    {
                        logger.OnServerRebalance(errorInfo);
                        return false;
                    }

                    logger.OnProtocolError(errorInfo);
                }
                return false;
            }
                   
            partition.ConfirmMessags(batchMessages);
            return true;
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

        #region Error handling

        private void ResetPartitionError([NotNull] KafkaProducerBrokerPartition partition)
        {
            var error = partition.Error;
            var errorTimestamp = partition.ErrorTimestampUtc;
            partition.ResetError();
            if (error == null) return;

            var errorInfo = new KafkaProducerTopicErrorResetInfo(partition.PartitionId, error.Value, errorTimestamp);
            partition.Logger?.OnPartitionErrorReset(errorInfo);
        }

        private void HandleBrokerError(
           [NotNull] KafkaProducerBrokerTopic topic,
           [NotNull] KafkaProducerBrokerPartition partition,
           KafkaBrokerErrorCode brokerError)
        {            
            KafkaProducerTopicPartitionErrorCode partitionErrorCode;

            var errorType = ProducerErrorType.Rearrange;

            switch (brokerError)
            {
                case KafkaBrokerErrorCode.ConnectionClosed:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionMaintenance:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ClientMaintenance;
                    break;
                case KafkaBrokerErrorCode.BadRequest:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.ProtocolError:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TransportError:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.ClientTimeout:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ClientTimeout;
                    break;
                case KafkaBrokerErrorCode.Cancelled:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionRefused:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.HostUnreachable:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.HostUnreachable;
                    break;
                case KafkaBrokerErrorCode.HostNotAvailable:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.NotAuthorized:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.NotAuthorized;
                    break;
                case KafkaBrokerErrorCode.UnsupportedOperation:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.OperationRefused:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.TooBigMessage:
                    if (topic.BatchSizeByteCount > 100) //empiric small size
                    {
                        partitionErrorCode = KafkaProducerTopicPartitionErrorCode.TransportError;
                        errorType = ProducerErrorType.Rearrange;
                    }
                    else
                    {
                        partitionErrorCode = KafkaProducerTopicPartitionErrorCode.MessageBatchSizeTooLarge;
                        topic.BatchSizeByteCount = (int)Math.Round(topic.BatchSizeByteCount * 0.66);
                        errorType = ProducerErrorType.Warning;
                    }                    
                    break;
                case KafkaBrokerErrorCode.UnknownError:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.UnknownError;
                    break;
                default:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.UnknownError;
                    break;
            }

            SetPartitionError(partition, partitionErrorCode, errorType);
        }

        private void SetPartitionError([NotNull] KafkaProducerBrokerPartition partition,
            KafkaProducerTopicPartitionErrorCode errorCode,
            ProducerErrorType errorType)
        {
            partition.SetError(errorCode);
            switch (errorType)
            {
                case ProducerErrorType.Warning:
                    break;
                case ProducerErrorType.Error:                    
                    partition.Status = KafkaProducerBrokerPartitionStatus.Error;
                    break;
                case ProducerErrorType.Rebalance:
                    partition.ResetData();
                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                    break;
                case ProducerErrorType.Rearrange:
                    partition.ResetData();
                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                    break;
            }
        }

        private void RollbackBatch([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch, KafkaBrokerErrorCode brokerError, string errorDescription)
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

                if (batchMessags != null)
                {
                    partition.RollbackMessags(batchMessags);
                }

                HandleBrokerError(topic, partition, brokerError);
            }

            var logger = topic.Logger;
            if (logger != null)
            {
                var errorInfo = new KafkaProducerTopicTransportErrorInfo(brokerError, errorDescription,
                    _clientBroker, batch.ByteCount, batch.MessageCount);
                logger.OnTransportError(errorInfo);
            }
        }

        private enum ProducerErrorType
        {
            Warning = 0,
            Error = 1,
            Rebalance = 2,
            Rearrange = 3
        }

        #endregion Error handling

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