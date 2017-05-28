using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
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
    internal sealed class KafkaProducerBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly IKafkaClientBroker _clientBroker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaProducerBrokerTopic> _topics;
        [NotNull] private readonly Dictionary<string, ProduceBatchSet> _produceRequests;

        public KafkaProducerBroker([NotNull] KafkaBroker broker, [NotNull] IKafkaClientBroker clientBroker)
        {
            _broker = broker;
            _clientBroker = clientBroker;
            _topics = new ConcurrentDictionary<string, KafkaProducerBrokerTopic>();

            _produceRequests = new Dictionary<string, ProduceBatchSet>();
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaProducerBrokerPartition topicPartition)
        {
            KafkaClientTrace.Trace($"[producer({topicName}, {topicPartition.PartitionId})] Partition added");

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
            KafkaClientTrace.Trace($"[producer({topicName}, {partitionId})] Partition removed");

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

                KafkaClientTrace.Trace($"[producer({topic.TopicName})] Stop");

                var batches = CreateTopicBatches(topic, new HashSet<int>());
                foreach (var batch in batches)
                {
                    var batchRequest = CreateProduceRequest(topic, batch);
                    _broker.SendWithoutResponse(batchRequest, topic.TopicProducerName, batch.ByteCount * 2);
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
            var newBatches = CreateTopicBatches(topic, batchSet.GetBusyPartitionSet());
            foreach (var batch in newBatches)
            {
                var batchRequest = CreateProduceRequest(topic, batch);
                var batchTimeout = topic.Settings.ProduceRequestClientTimeout;

                KafkaClientTrace.Trace($"[producer({topic.TopicName}] Produce batch sending {batch.GetPartititonText()}, {batch.MessageCount} messages");

                var batchRequestResult = _broker.Send(batchRequest, topic.TopicProducerName, batchTimeout, batch.ByteCount * 2);

                var batchRequestId = batchRequestResult.Data;
                if (batchRequestId == null)
                {
                    RollbackBatch(topic, batch, batchRequestResult.Error ?? KafkaBrokerErrorCode.TransportError, "SendProduceRequest");
                    continue;
                }

                KafkaClientTrace.Trace($"[producer({topic.TopicName}] Produce batch sent {batch.GetPartititonText()} partitions, {batch.MessageCount} messages");

                batchSet.RequestBatches[batchRequestId.Value] = batch;
            }
        }

        [NotNull, ItemNotNull]
        private IReadOnlyList<ProduceBatch> CreateTopicBatches([NotNull]KafkaProducerBrokerTopic topic, [NotNull] HashSet<int> busyPartitionSet)
        {
            var partitionList = new List<KafkaProducerBrokerPartition>(100);
            foreach (var partitionPair in topic.Partitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

                partitionList.Add(partition);
            }
            var produceOffset = topic.ProducePartitionIndex;
            if (produceOffset >= partitionList.Count)
            {
                produceOffset = 0;
            }

            var requests = new List<ProduceBatch>();
            var topicBatch = new ProduceBatch(partitionList.Count);
            var topicBatchSize = 0;
            var topicBatchMessageCount = 0;
            var topicBatchIsFilled = false;

            var topicBatchMaxSize = topic.Settings.ProduceRequestMaxSizeByteCount;
            var partitionBatchPreferredSize = topic.Settings.PartitionBatchPreferredSizeByteCount;

            for (var i = 0; i < partitionList.Count; i++)
            {
                var index = i + produceOffset;
                if (index >= partitionList.Count)
                {
                    index -= partitionList.Count;
                }

                var partition = partitionList[index];
                if (partition == null) continue;

                if (partition.Status == KafkaProducerBrokerPartitionStatus.RearrangeRequired)
                {
                    continue;
                }

                if (partition.Status == KafkaProducerBrokerPartitionStatus.Error)
                {
                    if (DateTime.UtcNow - partition.ErrorTimestampUtc < partition.Settings.ErrorRetryPeriod)
                    {
                        continue;
                    }

                    KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] Restore");
                }

                if (partition.Status == KafkaProducerBrokerPartitionStatus.NotInitialized)
                {
                    partition.ResetData();
                    partition.Status = KafkaProducerBrokerPartitionStatus.Ready;
                }

                if (busyPartitionSet.Contains(partition.PartitionId)) continue;

                var partitionBatchSize = 0;
                var partitionBatchMaxSize = partition.LimitInfo.MaxBatchSizeByteCount ?? topicBatchMaxSize;

                List<KafkaMessage> topicPartionMessages;
                topicBatch.Partitions.TryGetValue(partition.PartitionId, out topicPartionMessages);

                KafkaMessage message;
                while (partition.TryPeekMessage(out message))
                {
                    if (message == null)
                    {
                        partition.DequeueMessage();
                        continue;
                    }

                    var messageSize = _broker.Protocol.GetMessageSize(message);
                    if (messageSize > partition.LimitInfo.MaxMessageSizeByteCount)
                    {
                        partition.DequeueMessage();
                        partition.FallbackMessage(message, DateTime.UtcNow, KafkaProducerFallbackErrorCode.MessageSizeTooLarge);
                        continue;
                    }

                    var messageSizeInBatch = _broker.Protocol.GetMessageSizeInBatch(message);
                    if (messageSizeInBatch > partition.LimitInfo.MaxBatchSizeByteCount ||
                        messageSizeInBatch > topicBatchMaxSize)
                    {
                        partition.DequeueMessage();
                        partition.FallbackMessage(message, DateTime.UtcNow, KafkaProducerFallbackErrorCode.MessageSizeLargerThanBatchMaxSize);
                        continue;
                    }

                    if (partitionBatchSize + messageSizeInBatch > partitionBatchMaxSize ||
                        topicBatchSize + messageSizeInBatch > topicBatchMaxSize)
                    {
                        // not enough space in batch
                        break;
                    }

                    // message is accepted to batch
                    partition.DequeueMessage();
                    partitionBatchSize += messageSizeInBatch;
                    topicBatchSize += messageSizeInBatch;
                    topicBatchMessageCount++;

                    if (topicPartionMessages == null)
                    {
                        topicPartionMessages = new List<KafkaMessage>(1024); // empiric capacity value, may be need to save max message count for partition
                        topicBatch.Partitions[partition.PartitionId] = topicPartionMessages;
                    }
                    topicPartionMessages.Add(message);

                    if (topicBatchSize >= topicBatchMaxSize)
                    {
                        topicBatchIsFilled = true;
                        break;
                    }

                    if (partitionBatchSize >= partitionBatchPreferredSize)
                    {
                        break;
                    }
                }

                if (topicBatchIsFilled)
                {
                    produceOffset = index + 1;
                    topicBatch.ByteCount = topicBatchSize;
                    topicBatch.MessageCount = topicBatchMessageCount;
                    requests.Add(topicBatch);
                    topicBatch = new ProduceBatch(partitionList.Count);
                    topicBatchSize = 0;
                    topicBatchMessageCount = 0;
                    topicBatchIsFilled = false;
                }
            }

            if (topicBatch.Partitions.Count > 0)
            {
                topicBatch.ByteCount = topicBatchSize;
                topicBatch.MessageCount = topicBatchMessageCount;
                requests.Add(topicBatch);
            }

            topic.ProducePartitionIndex = produceOffset;

            return requests;
        }

        private void HandleProduceResponse([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] ProduceBatch topicBatch, KafkaBrokerResult<KafkaProduceResponse> response)
        {
            if (!response.HasData && !response.HasError)
            {
                // has not received
                return;
            }

            if (response.HasError || !response.HasData || response.Data == null)
            {
                var brokerError = response.Error ?? KafkaBrokerErrorCode.TransportError;
                RollbackBatch(topic, topicBatch, brokerError, "ReceiveProduceResponse.BrokerError");
                return;
            }

            var responseTopics = response.Data.Topics;
            if (responseTopics == null || responseTopics.Count == 0)
            {
                RollbackBatch(topic, topicBatch, KafkaBrokerErrorCode.ProtocolError, "ReceiveProduceResponse.NoTopicsInResponse");
                return;
            }

            foreach (var responseTopic in responseTopics)
            {
                var topicName = responseTopic?.TopicName;
                if (topicName != topic.TopicName)
                {
                    RollbackBatch(topic, topicBatch, KafkaBrokerErrorCode.ProtocolError, "ReceiveProduceResponse.UnexpectedTopicName");
                    continue;
                }

                var responsePartitions = responseTopic.Partitions;
                if (responsePartitions == null || responsePartitions.Count == 0)
                {
                    RollbackBatch(topic, topicBatch, KafkaBrokerErrorCode.ProtocolError, "ReceiveProduceResponse.NoPartitionsInResponse");
                    continue;
                }

                // check protocol violation (it's important to rollback batch in this case even though the case is almost impossible)
                foreach (var partitionResponse in responsePartitions)
                {
                    if (partitionResponse == null)
                    {
                        RollbackBatch(topic, topicBatch, KafkaBrokerErrorCode.ProtocolError, "ReceiveProduceResponse.EmptyPartition");
                        continue;
                    }
                    var partitionId = partitionResponse.PartitionId;

                    List<KafkaMessage> batchMessages;
                    KafkaProducerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null ||
                        !topicBatch.Partitions.TryGetValue(partitionId, out batchMessages) || batchMessages == null)
                    {
                        RollbackBatch(topic, topicBatch, KafkaBrokerErrorCode.ProtocolError, $"ReceiveProduceResponse.UnexpectedPartition({partitionId})");
                    }
                }

                foreach (var partitionResponse in responsePartitions)
                {
                    if (partitionResponse == null) continue;
                    var partitionId = partitionResponse.PartitionId;

                    KafkaProducerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                    {
                        continue;
                    }

                    List<KafkaMessage> batchMessages;
                    if (!topicBatch.Partitions.TryGetValue(partitionId, out batchMessages) || batchMessages == null)
                    {
                        continue;
                    }

                    if (!TryHandlePartitionResponse(topic, partition, partitionResponse, batchMessages))
                    {
                        continue;
                    }

                    ResetPartitionError(topic, partition);
                    partition.Status = KafkaProducerBrokerPartitionStatus.Ready;
                }
            }
        }

        private bool TryHandlePartitionResponse([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] KafkaProducerBrokerPartition partition,
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
                            var messageSize = _broker.Protocol.GetMessageSize(message);
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
                        error = KafkaProducerTopicPartitionErrorCode.MessageBatchTooLarge;
                        var batchSize = 0;
                        foreach (var message in batchMessages)
                        {
                            batchSize += _broker.Protocol.GetMessageSize(message);
                        }
                        partition.SetMaxBatchSizeByteCount(batchSize - 1);
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
                        errorType = ProducerErrorType.Error;
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

                KafkaClientTrace.Trace(errorType == ProducerErrorType.Rebalance
                    ? $"[producer({topic.TopicName}, {partition.PartitionId})] [ERROR] ProtocolError {response.ErrorCode} type={errorType}"
                    : $"[producer({topic.TopicName}, {partition.PartitionId})] [REBALANCE] {response.ErrorCode}");

                SetPartitionError(topic, partition, error, errorType, "HandlePartitionResponse");

                partition.RollbackMessages(batchMessages);
                KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] Rollback batch {batchMessages.Count}, Retry queue {partition.RetrySendPendingMessageCount}");

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
            KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] Produced {batchMessages.Count} Pending {partition.SendPendingMessageCount}");

            return true;
        }

        [NotNull]
        private KafkaProduceRequest CreateProduceRequest([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch)
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

            var batchRequest = new KafkaProduceRequest(topic.Settings.ConsistencyLevel, topic.Settings.ProduceRequestServerTimeout, new[] { requestTopic });
            return batchRequest;
        }

        #region Error handling

        private void ResetPartitionError([NotNull] KafkaProducerBrokerTopic topic, [NotNull] KafkaProducerBrokerPartition partition)
        {
            var error = partition.Error;
            var errorTimestamp = partition.ErrorTimestampUtc;
            partition.ResetError();
            if (error == null) return;

            KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] Restore");

            var errorInfo = new KafkaProducerTopicErrorResetInfo(partition.PartitionId, error.Value, errorTimestamp, _clientBroker);
            partition.Logger?.OnPartitionErrorReset(errorInfo);
        }

        private void HandleBrokerError([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] KafkaProducerBrokerPartition partition,
            KafkaBrokerErrorCode brokerError,
            [NotNull] string description)
        {
            KafkaProducerTopicPartitionErrorCode? partitionErrorCode;
            var errorType = ProducerErrorType.Rearrange;

            switch (brokerError)
            {
                case KafkaBrokerErrorCode.ConnectionClosed:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionMaintenance:
                    partitionErrorCode = null;
                    errorType = ProducerErrorType.Error;
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
                case KafkaBrokerErrorCode.OperationRefused:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.TooBigMessage:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.TransportRequestTooLarge;
                    break;
                case KafkaBrokerErrorCode.UnknownError:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.UnknownError;
                    break;
                default:
                    partitionErrorCode = KafkaProducerTopicPartitionErrorCode.UnknownError;
                    break;
            }

            if (partitionErrorCode != null)
            {
                KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] [ERROR] TransportError Code={brokerError} Description={description}");
            }

            SetPartitionError(topic, partition, partitionErrorCode, errorType, description);
        }

        private void SetPartitionError([NotNull] KafkaProducerBrokerTopic topic,
            [NotNull] KafkaProducerBrokerPartition partition,
            KafkaProducerTopicPartitionErrorCode? errorCode,
            ProducerErrorType errorType,
            [NotNull] string description)
        {
            if (errorCode != null)
            {
                partition.SetError(errorCode.Value);
            }
            switch (errorType)
            {
                case ProducerErrorType.Warning:
                    KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] [ERROR] Warning Code={errorCode} Description={description}");
                    break;
                case ProducerErrorType.Error:
                    partition.Status = KafkaProducerBrokerPartitionStatus.Error;
                    KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] [ERROR] Error Code={errorCode} Description={description}");
                    break;
                case ProducerErrorType.Rebalance:
                    KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] [REBALANCE] Code={errorCode} Description={description}");
                    partition.ResetData();
                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                    break;
                case ProducerErrorType.Rearrange:
                    KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] [ERROR] Rearrange Code={errorCode} Description={description}");
                    partition.ResetData();
                    partition.Status = KafkaProducerBrokerPartitionStatus.RearrangeRequired;
                    break;
            }
        }

        private void RollbackBatch([NotNull] KafkaProducerBrokerTopic topic, [NotNull] ProduceBatch batch, KafkaBrokerErrorCode brokerError, string description)
        {
            if (brokerError != KafkaBrokerErrorCode.ConnectionMaintenance)
            {
                KafkaClientTrace.Trace($"[producer({topic.TopicName}] [ERROR] TransportError {brokerError} Description={description}");
            }

            KafkaClientTrace.Trace($"[producer({topic.TopicName})] Rollback batch {batch.GetPartititonText()}");

            foreach (var batchPartition in batch.Partitions)
            {
                var partitionId = batchPartition.Key;
                var batchMessages = batchPartition.Value;

                KafkaProducerBrokerPartition partition;
                if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    continue;
                }

                if (batchMessages != null)
                {
                    partition.RollbackMessages(batchMessages);
                    KafkaClientTrace.Trace($"[producer({topic.TopicName}, {partition.PartitionId})] Rollback {batchMessages.Count} messages, Pending {partition.SendPendingMessageCount}, Retry queue {partition.RetrySendPendingMessageCount}");
                }

                HandleBrokerError(topic, partition, brokerError, description);
            }

            var logger = topic.Logger;
            if (logger != null && brokerError != KafkaBrokerErrorCode.ConnectionMaintenance)
            {
                var errorInfo = new KafkaProducerTopicTransportErrorInfo(brokerError, description,
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

            private string _partitionText;

            public ProduceBatch(int partitionCount)
            {
                Partitions = new Dictionary<int, List<KafkaMessage>>(partitionCount);
            }

            public string GetPartititonText()
            {
                if (_partitionText != null) return _partitionText;

                var builder = new StringBuilder(10);
                builder.Append('(');
                foreach (var partitionPair in Partitions)
                {
                    if (builder.Length > 1)
                    {
                        builder.Append(',');
                    }
                    builder.Append(partitionPair.Key);
                    builder.Append(':');
                    builder.Append(partitionPair.Value.Count);
                }

                _partitionText = builder.ToString();
                return _partitionText;
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