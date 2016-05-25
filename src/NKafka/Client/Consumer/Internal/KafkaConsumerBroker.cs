using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Consumer.Diagnostics;
using NKafka.Client.Consumer.Logging;
using NKafka.Connection;
using NKafka.Connection.Diagnostics;
using NKafka.Protocol;
using NKafka.Protocol.API.Fetch;
using NKafka.Protocol.API.Offset;

namespace NKafka.Client.Consumer.Internal
{    
    internal sealed class KafkaConsumerBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly IKafkaClientBroker _clientBroker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaConsumerBrokerTopic> _topics;
        [NotNull] private readonly Dictionary<string, FetchRequestInfo> _fetchRequests;

        public bool IsConsumeEnabled;

        private readonly TimeSpan _consumeClientTimeout;

        public KafkaConsumerBroker([NotNull] KafkaBroker broker, [NotNull] IKafkaClientBroker clientBroker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _clientBroker = clientBroker;
            _topics = new ConcurrentDictionary<string, KafkaConsumerBrokerTopic>();         
            _fetchRequests = new Dictionary<string, FetchRequestInfo>();   
            _consumeClientTimeout = consumePeriod + TimeSpan.FromSeconds(1) + consumePeriod;        
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaConsumerBrokerPartition topicPartition)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic) || topic == null)
            {
                var groupName = topicPartition.Group.GroupCoordinator.GroupName;
                var topicConsumerName = $"topic(consumer)[{topicName}][{groupName}]";
                var brokerTopic = new KafkaConsumerBrokerTopic(topicName, topicConsumerName, topicPartition.Group, topicPartition.Settings);
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

        public void Start()
        {
            IsConsumeEnabled = true;
        }

        public void Stop()
        {
            IsConsumeEnabled = false;

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                var coordinator = topic.Group.GroupCoordinator;
                var coordinatorPartitionOffsets = coordinator.GetPartitionOffsets(topic.TopicName);

                foreach (var partitionPair in topic.Partitions)
                {                    
                    var partition = partitionPair.Value;
                    if (partition == null) continue;
                    
                    IKafkaConsumerCoordinatorOffsetsData coordinatorOffset = null;
                    coordinatorPartitionOffsets?.TryGetValue(partition.PartitionId, out coordinatorOffset);                  
                    if (coordinatorPartitionOffsets?.TryGetValue(partition.PartitionId, out coordinatorOffset) == true && coordinatorOffset != null)
                    {
                        partition.IsAssigned = true;
                    }
                    else
                    {
                        partition.IsAssigned = false;
                    }

                    if (coordinatorOffset != null)
                    {
                        partition.SetCommitServerOffset(coordinatorOffset.GroupServerOffset, coordinatorOffset.TimestampUtc);
                    }

                    // check uncommited offsets for unassigned partitions
                    var commitClientOffset = partition.GetCommitClientOffset();
                    var commitServerOffset = partition.GetCommitServerOffset();
                    if (commitClientOffset.HasValue &&
                        (commitServerOffset == null || (commitClientOffset > commitServerOffset)))
                    {
                        var fallbackHandler = partition.FallbackHandler;
                        if (fallbackHandler != null)
                        {
                            var fallbackInfo = new KafkaConsumerFallbackInfo(topic.TopicName, partition.PartitionId,
                                KafkaConsumerFallbackErrorCode.ClientStopped, commitClientOffset.Value, commitServerOffset);
                            try
                            {
                                fallbackHandler.HandleСommitFallback(fallbackInfo);
                            }
                            catch (Exception)
                            {
                                //ignored
                            }
                        }
                    }
                    partition.ResetCommitClientOffset();

                    partition.Status = KafkaConsumerBrokerPartitionStatus.RearrangeRequired;
                    partition.Clear();
                }              
            }

            _fetchRequests.Clear();
        }

        public void Consume(CancellationToken cancellation)
        {            
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                if (cancellation.IsCancellationRequested) return;
                ConsumeTopic(topic, cancellation);
            }
        }

        private void ConsumeTopic([NotNull] KafkaConsumerBrokerTopic topic, CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;

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
                    if (IsConsumeEnabled)
                    {
                        HandleFetchResponse(topic, currentRequest, response);
                    }
                    _fetchRequests.Remove(topic.TopicName);
                }
            }

            var coordinator = topic.Group.GroupCoordinator;
            var coordinatorPartitionOffsets = coordinator.GetPartitionOffsets(topic.TopicName);

            var catchUpCoordinator = topic.Group.CatchUpGroupCoordinator;
            var catchUpPartitionOffsets = catchUpCoordinator?.GetPartitionOffsets(topic.TopicName);

            // prepare new fetch batch
            foreach (var partitionPair in topic.Partitions)
            {                
                var partition = partitionPair.Value;
                if (partition == null) continue;
                var partitionId = partition.PartitionId;

                var coordinatorOffset = SyncPartitionWithCoordinator(partition, coordinatorPartitionOffsets);

                IKafkaConsumerCoordinatorOffsetsData catchUpOffset = null;
                if (catchUpCoordinator != null)
                {
                    // uses catch-up group
                    catchUpOffset = SyncPartitionWithCatchUpCoordinator(partition, catchUpPartitionOffsets);
                }

                if (!IsConsumeEnabled || coordinatorOffset == null) continue;                

                if (oldFetchBatch.ContainsKey(partitionId)) continue;
                if (!TryPreparePartition(topic, partition)) continue;

                var currentReceivedOffset = partition.GetReceivedClientOffset();
                var minAvailableOffset = partition.GetMinAvailableServerOffset();

                var clientOffset = currentReceivedOffset ?? coordinatorOffset.GroupServerOffset;
                if (clientOffset == null)
                {
                    if (partition.Settings.BeginBehavior == KafkaConsumerBeginBehavior.BeginFromMinAvailableOffset)
                    {
                        clientOffset = partition.GetMinAvailableServerOffset() - 1;
                    }
                    if (partition.Settings.BeginBehavior == KafkaConsumerBeginBehavior.BeginFromMaxAvailableOffset)
                    {
                        clientOffset = partition.GetMaxAvailableServerOffset();
                    }
                }

                if (clientOffset == null) continue;

                if (clientOffset < minAvailableOffset)
                {
                    clientOffset = minAvailableOffset - 1;
                }

                if (clientOffset < coordinatorOffset.GroupServerOffset)
                {
                    clientOffset = coordinatorOffset.GroupServerOffset;
                }

                // uses catch-up group
                if (clientOffset >= catchUpOffset?.GroupServerOffset) continue;

                if (clientOffset == null) continue;

                newFetchBatch[partitionId] = clientOffset.Value + 1;
            }

            if (newFetchBatch.Count == 0) return;

            SendFetchRequest(topic, newFetchBatch);
        }

        private IKafkaConsumerCoordinatorOffsetsData SyncPartitionWithCoordinator([NotNull] KafkaConsumerBrokerPartition partition, 
            [CanBeNull] IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> coordinatorPartitionOffsets)
        {
            if (coordinatorPartitionOffsets == null)
            {
                partition.IsAssigned = false; // coordinator is not ready or topic is not allowed for this consumer node
                return null;
            }
            IKafkaConsumerCoordinatorOffsetsData coordinatorOffset;
            if (!coordinatorPartitionOffsets.TryGetValue(partition.PartitionId, out coordinatorOffset) || coordinatorOffset == null)
            {
                if (partition.IsAssigned)
                {
                    partition.IsAssigned = false; // partition is not allowed for this consumer node

                    // check uncommited offsets for unassigned partitions
                    var unassignedClientOffset = partition.GetCommitClientOffset();
                    var unassignedServerOffset = partition.GetCommitServerOffset();
                    if (unassignedClientOffset.HasValue &&
                        (unassignedServerOffset == null || (unassignedClientOffset > unassignedServerOffset)))
                    {
                        var fallbackHandler = partition.FallbackHandler;
                        if (fallbackHandler != null)
                        {
                            var fallbackInfo = new KafkaConsumerFallbackInfo(partition.TopicName, partition.PartitionId,
                                KafkaConsumerFallbackErrorCode.UnassignedBeforeCommit, unassignedClientOffset.Value, unassignedServerOffset);
                            try
                            {
                                fallbackHandler.HandleСommitFallback(fallbackInfo);
                            }
                            catch (Exception)
                            {
                                //ignored
                            }
                        }
                    }

                    partition.ResetCommitClientOffset();
                }

                return null;
            }

            if (!partition.IsAssigned)
            {
                partition.ResetCommitClientOffset();
                partition.IsAssigned = true;
            }
            partition.SetCommitServerOffset(coordinatorOffset.GroupServerOffset, coordinatorOffset.TimestampUtc);
            return coordinatorOffset;
        }

        private IKafkaConsumerCoordinatorOffsetsData SyncPartitionWithCatchUpCoordinator(
            [NotNull] KafkaConsumerBrokerPartition partition,
            [CanBeNull] IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> catchUpPartitionOffsets)
        {                        
            if (catchUpPartitionOffsets == null)
            {
                // catch-up group coordinator is not ready
                return null;
            }
            IKafkaConsumerCoordinatorOffsetsData catchUpOffset;

            if (!catchUpPartitionOffsets.TryGetValue(partition.PartitionId, out catchUpOffset) || catchUpOffset == null)
            {
                // catch-up group coordinator has not received partition offset
                return null;
            }
            partition.SetCatchUpGroupServerOffset(catchUpOffset.GroupServerOffset);
            return catchUpOffset;
        }

        private bool TryPreparePartition([NotNull] KafkaConsumerBrokerTopic topic, [NotNull] KafkaConsumerBrokerPartition partition)
        {
            if (partition.Status == KafkaConsumerBrokerPartitionStatus.RearrangeRequired) return false;
                        
            if (partition.Status == KafkaConsumerBrokerPartitionStatus.NotInitialized)
            {
                partition.ResetData();

                if (!TrySendRequestOffsets(topic, partition))
                {
                    return false;
                }
                
                partition.Status = KafkaConsumerBrokerPartitionStatus.OffsetRequested;                
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.NotInitialized ||
                partition.Status == KafkaConsumerBrokerPartitionStatus.Error)
            {
                if (DateTime.UtcNow - partition.ErrorTimestampUtc > partition.Settings.ErrorRetryPeriod)
                {
                    return false;
                }

                if (!TrySendRequestOffsets(topic, partition))
                {
                    return false;
                }

                partition.Status = KafkaConsumerBrokerPartitionStatus.OffsetRequested;
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.OffsetRequested)
            {
                if (!TryHandleOffsetResponse(topic, partition))
                {
                    return false;
                }

                partition.Status = KafkaConsumerBrokerPartitionStatus.Ready;
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.Ready)
            {
                return partition.CanEnqueueForConsume();
            }

            return false;
        }        

        #region Topic offsets

        private bool TrySendRequestOffsets([NotNull] KafkaConsumerBrokerTopic topic, [NotNull] KafkaConsumerBrokerPartition partition)
        {
            var partitionRequest = new KafkaOffsetRequestTopicPartition(partition.PartitionId, null, 2);
            var topicRequest = new KafkaOffsetRequestTopic(partition.TopicName, new [] { partitionRequest });
            var request = new KafkaOffsetRequest(new[] {topicRequest});
            var requestResult = _broker.Send(request, topic.TopicConsumerName, _consumeClientTimeout);
            
            if (requestResult.HasError || requestResult.Data == null)
            {
                var brokerError = requestResult.Error ?? KafkaBrokerErrorCode.TransportError;
                HandleBrokerError(partition, brokerError);
                LogBrokerError(topic, brokerError, "SendOffsetRequest");
                return false;
            }

            var requestId = requestResult.Data.Value;
            partition.OffsetRequestId = requestId;
            return true;
        }

        private bool TryHandleOffsetResponse([NotNull] KafkaConsumerBrokerTopic topic, [NotNull] KafkaConsumerBrokerPartition partition)
        {
            var requestId = partition.OffsetRequestId;
            if (requestId == null)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ClientError, ConsumerErrorType.Error);
                return false;
            }

            var response = _broker.Receive<KafkaOffsetResponse>(requestId.Value);

            if (!response.HasData && !response.HasError) return false;

            partition.OffsetRequestId = null;

            if (response.Error != null || response.Data == null)
            {
                var brokerError = response.Error ?? KafkaBrokerErrorCode.TransportError;                
                HandleBrokerError(partition, brokerError);
                LogBrokerError(topic, brokerError, "ReceiveOffsetRequest");
                return false;
            }

            partition.OffsetRequestId = null;
                        
            var offsetResponseTopics = response.Data.Topics;

            if (offsetResponseTopics == null || offsetResponseTopics.Count == 0)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
                LogProtocolError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error, "OffsetResponse(no topics)");
                return false;
            }

            var offsetResponsePartitions = offsetResponseTopics[0]?.Partitions;
            if (offsetResponsePartitions == null || offsetResponsePartitions.Count == 0)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
                LogProtocolError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error, "OffsetResponse(no partitions)");
                return false;
            }

            var offsetResponsePartition = offsetResponsePartitions[0];
            if (offsetResponsePartition == null)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
                return false;
            }

            if (offsetResponsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaConsumerTopicPartitionErrorCode errorCode;
                ConsumerErrorType errorType;
                switch (offsetResponsePartition.ErrorCode)
                {
                    case KafkaResponseErrorCode.UnknownTopicOrPartition:
                        errorCode = KafkaConsumerTopicPartitionErrorCode.UnknownTopicOrPartition;
                        errorType = ConsumerErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.NotLeaderForPartition:
                        errorCode = KafkaConsumerTopicPartitionErrorCode.NotLeaderForPartition;                        
                        errorType = ConsumerErrorType.Rebalance;
                        break;
                    default:
                        errorCode = KafkaConsumerTopicPartitionErrorCode.UnknownError;
                        errorType = ConsumerErrorType.Rearrange;
                        break;
                }

                SetPartitionError(partition, errorCode, errorType);
                LogProtocolError(partition, errorCode, errorType, "OffsetResponse");
                return false;
            }

            var offsets = offsetResponsePartition.Offsets;
            if (offsets == null)
            {
                LogProtocolError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error, "OffsetResponse(no offsets)");
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
                return false;
            }
                        
            long? minOffset = null;
            long? maxOffset = null;
            foreach (var offset in offsets)
            {
                if (minOffset == null || offset < minOffset.Value)
                {
                    minOffset = offset;
                }
                if (maxOffset == null || offset > maxOffset.Value)
                {
                    maxOffset = offset;
                }
            }

            if (minOffset == null)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
                LogProtocolError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error, "OffsetResponse(no offsets)");
                return false;
            }

            partition.SetMinAvailableServerOffset(minOffset.Value - 1);
            partition.SetMaxAvailableServerOffset(maxOffset.Value - 1);
            return true;
        }

        #endregion Topic offsets

        #region Fetch

        private void SendFetchRequest([NotNull] KafkaConsumerBrokerTopic topic,
            [NotNull] Dictionary<int, long> fetchBatch)
        {
            var fetchRequest = CreateFetchRequest(topic, fetchBatch);
            var fetchTimeout = _consumeClientTimeout + topic.Settings.FetchServerWaitTime;
            var fetchResult = _broker.Send(fetchRequest, topic.TopicConsumerName, fetchTimeout);
            if (fetchResult.HasError || fetchResult.Data == null)
            {
                foreach (var partitionPair in fetchBatch)
                {
                    var partitionId = partitionPair.Key;
                    KafkaConsumerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                    {
                        continue;
                    }
                    var brokerError = fetchResult.Error ?? KafkaBrokerErrorCode.TransportError;
                    HandleBrokerError(partition, brokerError);
                    LogBrokerError(topic, brokerError, "SendFetchRequest");
                }
                return;
            }

            var fetchRequestId = fetchResult.Data;
            _fetchRequests[topic.TopicName] = new FetchRequestInfo(fetchRequestId.Value, fetchBatch);            
        }

        [NotNull]
        private KafkaFetchRequest CreateFetchRequest([NotNull] KafkaConsumerBrokerTopic topic,
            [NotNull] Dictionary<int, long> partitionBatch)
        {
            var partitionRequests = new List<KafkaFetchRequestTopicPartition>(partitionBatch.Count);
            foreach (var paritionPair in partitionBatch)
            {
                var partitionId = paritionPair.Key;
                var patitionOffset = paritionPair.Value;
                partitionRequests.Add(new KafkaFetchRequestTopicPartition(partitionId, patitionOffset, topic.Settings.PartitionBatchMaxSizeBytes));
            }
            var topicRequest = new KafkaFetchRequestTopic(topic.TopicName, partitionRequests);
            var fetchRequest = new KafkaFetchRequest(topic.Settings.FetchServerWaitTime, topic.Settings.TopicBatchMinSizeBytes, new[] { topicRequest });

            return fetchRequest;
        }

        private void HandleFetchResponse([NotNull] KafkaConsumerBrokerTopic topic, [NotNull] FetchRequestInfo request, KafkaBrokerResult<KafkaFetchResponse> response)
        {
            if (!response.HasData && !response.HasError) return;

            if (response.Error != null || response.Data == null)
            {
                var brokerError = response.Error ?? KafkaBrokerErrorCode.TransportError;
                foreach (var partitionPair in request.PartitionOffsets)
                {
                    var partitionId = partitionPair.Key;
                    KafkaConsumerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                    {
                        continue;
                    }
                    HandleBrokerError(partition, brokerError);                    
                }
                LogBrokerError(topic, brokerError, "ReceiveFetchResponse");
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

                foreach (var responsePartition in responsePartitions)
                {
                    if (responsePartition == null) continue;
                    var partitionId = responsePartition.PartitionId;

                    KafkaConsumerBrokerPartition partition;
                    if (!topic.Partitions.TryGetValue(partitionId, out partition) || partition == null)
                    {
                        continue;
                    }

                    if (responsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
                    {
                        KafkaConsumerTopicPartitionErrorCode errorCode;
                        ConsumerErrorType errorType;
                        switch (responsePartition.ErrorCode)
                        {
                            case KafkaResponseErrorCode.OffsetOutOfRange: // jast no messages
                                continue;
                            case KafkaResponseErrorCode.UnknownTopicOrPartition:
                                errorCode = KafkaConsumerTopicPartitionErrorCode.UnknownTopicOrPartition;
                                errorType = ConsumerErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.NotLeaderForPartition:
                                errorCode = KafkaConsumerTopicPartitionErrorCode.NotLeaderForPartition;
                                errorType = ConsumerErrorType.Rebalance;
                                break;
                            case KafkaResponseErrorCode.ReplicaNotAvailable:
                                errorCode = KafkaConsumerTopicPartitionErrorCode.ReplicaNotAvailable;
                                errorType = ConsumerErrorType.Warning;
                                break;
                            default:
                                errorCode = KafkaConsumerTopicPartitionErrorCode.UnknownError;
                                errorType = ConsumerErrorType.Rearrange;
                                break;
                        }
                        
                        SetPartitionError(partition, errorCode, errorType);
                        LogProtocolError(partition, errorCode, errorType, "FetchResponse");
                        continue;
                    }

                    ResetPartitionError(partition);
                    if (responsePartition.Messages != null && responsePartition.Messages.Count > 0)
                    {
                        partition.SetMaxAvailableServerOffset(responsePartition.HighwaterMarkOffset - 1);
                        partition.EnqueueMessagesForConsume(responsePartition.Messages);
                    }
                }
            }
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

        #region Error handling        

        private void LogBrokerError([NotNull] KafkaConsumerBrokerTopic topic, KafkaBrokerErrorCode brokerError, string errorDescription)
        {
            var logger = topic.Logger;
            if (logger == null) return;
            var errorInfo = new KafkaConsumerTopicTransportErrorInfo(brokerError, errorDescription, _clientBroker);
            logger.OnTransportError(errorInfo);
        }
        
        private void LogProtocolError([NotNull] KafkaConsumerBrokerPartition partition, KafkaConsumerTopicPartitionErrorCode error, ConsumerErrorType erorrType, string errorDescription)
        {
            var logger = partition.Logger;
            if (logger != null)
            {
                var errorInfo = new KafkaConsumerTopicProtocolErrorInfo(partition.PartitionId, error, errorDescription, _clientBroker);
                if (erorrType == ConsumerErrorType.Warning)
                {
                    logger.OnProtocolWarning(errorInfo);
                    return;
                }

                if (erorrType == ConsumerErrorType.Rebalance)
                {
                    logger.OnServerRebalance(errorInfo);
                    return;
                }

                logger.OnProtocolError(errorInfo);
            }
        }

        private void ResetPartitionError([NotNull] KafkaConsumerBrokerPartition partition)
        {
            var error = partition.Error;
            var errorTimestamp = partition.ErrorTimestampUtc;
            partition.ResetError();
            if (error == null) return;

            var errorInfo = new KafkaConsumerTopicErrorResetInfo(partition.PartitionId, error.Value, errorTimestamp, _clientBroker);
            partition.Logger?.OnPartitionErrorReset(errorInfo);
        }

        private void HandleBrokerError([NotNull] KafkaConsumerBrokerPartition partition, KafkaBrokerErrorCode errorCode)
        {
            KafkaConsumerTopicPartitionErrorCode partitionErrorCode;

            switch (errorCode)
            {
                case KafkaBrokerErrorCode.ConnectionClosed:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionMaintenance:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ClientMaintenance;
                    break;
                case KafkaBrokerErrorCode.BadRequest:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.ProtocolError:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TransportError:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.ClientTimeout:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ClientTimeout;
                    break;
                case KafkaBrokerErrorCode.Cancelled:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionRefused:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.HostUnreachable:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.HostUnreachable;
                    break;
                case KafkaBrokerErrorCode.HostNotAvailable:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.NotAuthorized:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.NotAuthorized;
                    break;
                case KafkaBrokerErrorCode.UnsupportedOperation:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.OperationRefused:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TooBigMessage: // there are only command requests w/o data - network problem.
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.UnknownError:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.UnknownError;
                    break;
                default:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.UnknownError;
                    break;
            }

            SetPartitionError(partition, partitionErrorCode, ConsumerErrorType.Rearrange);
        }

        private void SetPartitionError([NotNull] KafkaConsumerBrokerPartition partition, 
            KafkaConsumerTopicPartitionErrorCode errorCode,
            ConsumerErrorType errorType)
        {
            partition.SetError(errorCode);
            switch (errorType)
            {
                case ConsumerErrorType.Warning:
                    break;                
                case ConsumerErrorType.Error:     
                    partition.ResetData();               
                    partition.Status = KafkaConsumerBrokerPartitionStatus.Error;
                    break;
                case ConsumerErrorType.Rebalance:
                    partition.ResetData();
                    partition.Status = KafkaConsumerBrokerPartitionStatus.RearrangeRequired;
                    break;
                case ConsumerErrorType.Rearrange:
                    partition.ResetData();
                    partition.Status = KafkaConsumerBrokerPartitionStatus.RearrangeRequired;
                    break;
            }
        }

        private enum ConsumerErrorType
        {
            Warning = 0,
            Error = 1,
            Rebalance = 2,
            Rearrange = 3
        }

        #endregion Error handling           
    }
}