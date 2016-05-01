using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;
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
                    HandleFetchResponse(topic, currentRequest, response);
                    _fetchRequests.Remove(topic.TopicName);
                }
            }            

            var coordinator = topic.Coordinator;
            var coordinatorPartitionOffsets = coordinator.GetPartitionOffsets(topic.TopicName);            

            // prepare new fetch batch
            foreach (var partitionPair in topic.Partitions)
            {
                var partitionId = partitionPair.Key;
                var partition = partitionPair.Value;
                if (partition == null) continue;

                if (coordinatorPartitionOffsets == null)
                {
                    partition.IsAssigned = false; // coordinator is not ready or topic is not allowed for this consumer node
                    continue; 
                }
                IKafkaConsumerCoordinatorOffsetsData coordinatorOffset;
                if (!coordinatorPartitionOffsets.TryGetValue(partitionId, out coordinatorOffset) || coordinatorOffset == null)
                {
                    partition.IsAssigned = false; // partition is not allowed for this consumer node
                    continue; 
                }
                partition.IsAssigned = true;

                if (oldFetchBatch.ContainsKey(partitionId)) continue;
                if (!TryPreparePartition(partition)) continue;

                var clientOffset = partition.GetReceivedClientOffset() ?? coordinatorOffset.GroupServerOffset ?? partition.GetMaxAvailableServerOffset();
                var minAvailableOffset = partition.GetMinAvailableServerOffset();
                
                if (clientOffset < minAvailableOffset)
                {
                    clientOffset = minAvailableOffset;
                }

                if (clientOffset < coordinatorOffset.GroupServerOffset)
                {
                    clientOffset = coordinatorOffset.GroupServerOffset;
                }

                newFetchBatch[partitionId] = (clientOffset ?? -1) + 1;
            }

            if (newFetchBatch.Count == 0) return;

            SendFetchRequest(topic, newFetchBatch);
        }

        private bool TryPreparePartition([NotNull] KafkaConsumerBrokerPartition partition)
        {
            if (partition.Status == KafkaConsumerBrokerPartitionStatus.RearrangeRequired) return false;
                        
            if (partition.Status == KafkaConsumerBrokerPartitionStatus.NotInitialized)
            {
                partition.ResetData();

                if (!TrySendRequestOffsets(partition))
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

                if (!TrySendRequestOffsets(partition))
                {
                    return false;
                }

                partition.Status = KafkaConsumerBrokerPartitionStatus.OffsetRequested;
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.OffsetRequested)
            {
                if (!TryHandleOffsetResponse(partition))
                {
                    return false;
                }

                partition.Status = KafkaConsumerBrokerPartitionStatus.Ready;
            }

            if (partition.Status == KafkaConsumerBrokerPartitionStatus.Ready)
            {
                return partition.CanEnqueue();
            }

            return false;
        }

        

        #region Topic offsets

        private bool TrySendRequestOffsets([NotNull] KafkaConsumerBrokerPartition partition)
        {
            var partitionRequest = new KafkaOffsetRequestTopicPartition(partition.PartitionId, null, 2);
            var topicRequest = new KafkaOffsetRequestTopic(partition.TopicName, new [] { partitionRequest });
            var requestResult = _broker.Send(new KafkaOffsetRequest(new[] { topicRequest }), _consumeClientTimeout);
            
            if (requestResult.HasError || requestResult.Data == null)
            {
                HandleBrokerError(partition, requestResult.Error ?? KafkaBrokerErrorCode.TransportError);
                return false;
            }

            var requestId = requestResult.Data.Value;
            partition.OffsetRequestId = requestId;
            return true;
        }

        private bool TryHandleOffsetResponse([NotNull] KafkaConsumerBrokerPartition partition)
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
                HandleBrokerError(partition, response.Error ?? KafkaBrokerErrorCode.TransportError);
                return false;
            }

            partition.OffsetRequestId = null;
                        
            var offsetResponseTopics = response.Data.Topics;

            if (offsetResponseTopics == null || offsetResponseTopics.Count == 0)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
                return false;
            }

            var offsetResponsePartitions = offsetResponseTopics[0]?.Partitions;
            if (offsetResponsePartitions == null || offsetResponsePartitions.Count == 0)
            {
                SetPartitionError(partition, KafkaConsumerTopicPartitionErrorCode.ProtocolError, ConsumerErrorType.Error);
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
                        errorType = ConsumerErrorType.Rearrange;
                        break;
                    default:
                        errorCode = KafkaConsumerTopicPartitionErrorCode.UnknownError;
                        errorType = ConsumerErrorType.Rearrange;
                        break;
                }

                SetPartitionError(partition, errorCode, errorType);
                return false;
            }

            var offsets = offsetResponsePartition.Offsets;
            if (offsets == null)
            {
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
            var fetchResult = _broker.Send(fetchRequest, _consumeClientTimeout + topic.Settings.ConsumeServerWaitTime);
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
                    HandleBrokerError(partition, fetchResult.Error ?? KafkaBrokerErrorCode.TransportError);
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
                partitionRequests.Add(new KafkaFetchRequestTopicPartition(partitionId, patitionOffset, topic.Settings.ConsumeBatchMaxSizeBytes));
            }
            var topicRequest = new KafkaFetchRequestTopic(topic.TopicName, partitionRequests);
            var fetchRequest = new KafkaFetchRequest(topic.Settings.ConsumeServerWaitTime, topic.Settings.ConsumeBatchMinSizeBytes, new[] { topicRequest });

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
                                errorType = ConsumerErrorType.Rearrange;
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
                        continue;
                    }

                    partition.ResetError();
                    partition.SetMaxAvailableServerOffset(responsePartition.HighwaterMarkOffset);
                    partition.EnqueueMessages(responsePartition.Messages ?? new KafkaMessageAndOffset[0]);
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

        private void HandleBrokerError([NotNull] KafkaConsumerBrokerPartition partition, KafkaBrokerErrorCode errorCode)
        {
            KafkaConsumerTopicPartitionErrorCode partitionErrorCode;

            switch (errorCode)
            {
                case KafkaBrokerErrorCode.Closed:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.Maintenance:
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
                case KafkaBrokerErrorCode.Timeout:
                    partitionErrorCode = KafkaConsumerTopicPartitionErrorCode.ClientTimeout;
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
            Rearrange = 2
        }

        #endregion Error handling           
    }
}