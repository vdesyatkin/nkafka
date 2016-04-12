using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Client.Internal.Broker;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientWorker
    {
        [NotNull] private readonly KafkaProtocol _protocol;
        [NotNull] private readonly KafkaClientSettings _settings;

        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientTopic> _topics;
        [NotNull] private readonly ConcurrentDictionary<string, MetadataRequestInfo> _topicMetadataRequests;        

        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientGroup> _groups;
        [NotNull] private readonly ConcurrentDictionary<string, MetadataRequestInfo> _groupMetadataRequests;

        [NotNull] private readonly ConcurrentDictionary<int, KafkaClientBroker> _brokers;
        [NotNull, ItemNotNull] private readonly IReadOnlyCollection<KafkaClientBroker> _metadataBrokers;

        [NotNull] private Timer _workerTimer;
        private readonly TimeSpan _workerPeriod;
        [NotNull] private CancellationTokenSource _workerCancellation;

        public delegate void ArrangeTopicDelegate([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaClientBrokerPartition> partitions);
        public event ArrangeTopicDelegate ArrangeTopic;

        public delegate void ArrangeGroupDelegate([NotNull] string groupName, [NotNull] KafkaClientBrokerGroup groupCoordinator);
        public event ArrangeGroupDelegate ArrangeGroup;

        private readonly TimeSpan _retryMetadataRequestPeriod = TimeSpan.FromMinutes(1);
        
        public KafkaClientWorker([NotNull] KafkaClientSettings settings)
        {
            _settings = settings;
            _protocol = new KafkaProtocol(_settings.KafkaVersion, _settings.ClientId);
            _workerPeriod = settings.WorkerPeriod;
            if (_workerPeriod < TimeSpan.FromMilliseconds(100))
            {
                _workerPeriod = TimeSpan.FromMilliseconds(100);
            }

            _brokers = new ConcurrentDictionary<int, KafkaClientBroker>();

            _topics = new ConcurrentDictionary<string, KafkaClientTopic>();
            _topicMetadataRequests = new ConcurrentDictionary<string, MetadataRequestInfo>();            

            _groups = new ConcurrentDictionary<string, KafkaClientGroup>();
            _groupMetadataRequests = new ConcurrentDictionary<string, MetadataRequestInfo>();

            var metadataBrokerInfos = _settings.MetadataBrokers ?? new KafkaBrokerInfo[0];
            var metadataBrokers = new List<KafkaClientBroker>(metadataBrokerInfos.Count);
            foreach (var metadataBrokerInfo in metadataBrokerInfos)
            {
                if (metadataBrokerInfo == null) continue;
                metadataBrokers.Add(CreateMetadataBroker(metadataBrokerInfo));
            }

            _metadataBrokers = metadataBrokers;
            _workerCancellation = new CancellationTokenSource();
            _workerTimer = new Timer(Work);
        }        

        public void AssignTopic([NotNull] KafkaClientTopic topic)
        {
            _topics[topic.TopicName] = topic;            
        }

        public void AssignTopicPartition([NotNull] string topicName, [NotNull] KafkaClientBrokerPartition topicPartition)
        {
            var brokerId = topicPartition.BrokerMetadata.BrokerId;
            KafkaClientBroker broker;
            if (!_brokers.TryGetValue(brokerId, out broker) || broker == null)
            {
                broker = CreateBroker(topicPartition.BrokerMetadata);
                _brokers[brokerId] = broker;
            }

            broker.AddTopicPartition(topicName, topicPartition);
        }

        public void AssignGroup([NotNull] KafkaClientGroup group)
        {
            _groups[group.GroupName] = group;
        }

        public void AssignGroupCoordinator([NotNull] string groupName, [NotNull] KafkaClientBrokerGroup groupCoordinator)
        {
            var brokerId = groupCoordinator.BrokerMetadata.BrokerId;
            KafkaClientBroker broker;
            if (!_brokers.TryGetValue(brokerId, out broker) || broker == null)
            {
                broker = CreateBroker(groupCoordinator.BrokerMetadata);
                _brokers[brokerId] = broker;
            }

            broker.AddGroupCoordinator(groupName, groupCoordinator);
        }

        public void Start()
        {
            _workerCancellation = new CancellationTokenSource();
            var produceTimer = new Timer(Work);
            _workerTimer = produceTimer;
            produceTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }

        public void Stop()
        {
            try
            {
                _workerCancellation.Cancel();
            }
            catch (Exception)
            {
                //ignored
            }

            try
            {
                var workerTimer = _workerTimer;
                lock (workerTimer)
                {
                    workerTimer.Dispose();
                }
            }
            catch (Exception)
            {
                //ignored
            }            

            foreach (var broker in _brokers)
            {
                broker.Value?.Close();                
            }
            foreach (var broker in _metadataBrokers)
            {
                broker.Close();                
            }

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;                
                if (topic == null) continue;

                topic.Status = KafkaClientTopicStatus.RearrangeRequired;
            }

            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                group.Status = KafkaClientGroupStatus.RearrangeRequired;
            }

            _topicMetadataRequests.Clear();
            _groupMetadataRequests.Clear();
        }

        private void Work(object state)
        {
            if (_workerCancellation.IsCancellationRequested) return;
            var workerTimer = _workerTimer;
            lock (workerTimer)
            {
                var hasGroups = false;
                foreach (var groupPair in _groups)
                {
                    var group = groupPair.Value;
                    if (group == null) continue;
                    if (_workerCancellation.IsCancellationRequested) return;

                    ProcessGroup(group);
                    hasGroups = true;
                }

                var hasTopics = false;
                foreach (var topicPair in _topics)
                {
                    var topic = topicPair.Value;
                    if (topic == null) continue;
                    if (_workerCancellation.IsCancellationRequested) return;

                    ProcessTopic(topic);
                    hasTopics = true;
                }

                var isBrokersRequired = hasTopics;
                bool isRegularBrokerAvailable = false;
                foreach (var brokerPair in _brokers)
                {
                    var broker = brokerPair.Value;
                    if (broker == null) continue;
                    if (_workerCancellation.IsCancellationRequested) return;

                    ProcessBroker(broker, isBrokersRequired);
                    if (broker.IsEnabled)
                    {
                        isRegularBrokerAvailable = true;
                    }
                }

                var isMetadataBrokerRequired = (hasTopics || hasGroups) && !isRegularBrokerAvailable;
                foreach (var metadataBroker in _metadataBrokers)
                {
                    if (_workerCancellation.IsCancellationRequested) return;
                    ProcessMetadataBroker(metadataBroker, isMetadataBrokerRequired);
                }                       
            
                if (_workerCancellation.IsCancellationRequested) return;

                try
                {
                    workerTimer.Change(_workerPeriod, Timeout.InfiniteTimeSpan);
                }
                catch (Exception)
                {
                    //ignored
                }
            }
        }

        private void ProcessTopic([NotNull] KafkaClientTopic topic)
        {
            if (topic.Status == KafkaClientTopicStatus.MetadataError)
            {
                if (topic.DiagnosticsInfo.TimestampUtc - DateTime.UtcNow < _retryMetadataRequestPeriod) return;                
            }

            if (topic.Status == KafkaClientTopicStatus.NotInitialized || topic.Status == KafkaClientTopicStatus.MetadataError)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataRequest = CreateTopicMetadataRequest(topic.TopicName);
                    var metadataRequestResult =  metadataBroker.SendRequest(metadataRequest);
                    if (metadataRequestResult.HasError)
                    {
                        topic.Status = KafkaClientTopicStatus.MetadataError;
                        topic.ChangeMetadataState(false, ConvertMetadataRequestError(metadataRequestResult.Error), null);                        
                        return;
                    }
                    
                    var metadataRequestId = metadataRequestResult.HasData ? metadataRequestResult.Data : null;
                    if (metadataRequestId.HasValue)
                    {

                        _topicMetadataRequests[topic.TopicName] = new MetadataRequestInfo(metadataRequestId.Value, metadataBroker);
                        topic.Status = KafkaClientTopicStatus.MetadataRequested;
                    }
                }
            }

            if (topic.Status == KafkaClientTopicStatus.MetadataRequested)
            {
                MetadataRequestInfo metadataRequest;
                if (!_topicMetadataRequests.TryGetValue(topic.TopicName, out metadataRequest) || metadataRequest == null)
                {
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    return;
                }

                var topicMetadataResponse = metadataRequest.Broker.GetResponse<KafkaTopicMetadataResponse>(metadataRequest.RequestId);
                if (topicMetadataResponse.HasError)
                {
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, ConvertMetadataRequestError(topicMetadataResponse.Error), null);
                    return;
                }

                if (!topicMetadataResponse.HasData || topicMetadataResponse.Data == null)
                {
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, KafkaClientTopicErrorCode.ProtocolError, null);
                    return;
                }

                bool hasMetadataError;
                var metadata = ConvertMetadata(topic.TopicName, topicMetadataResponse.Data, out hasMetadataError);                               

                if (hasMetadataError)
                {                 
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, KafkaClientTopicErrorCode.MetadataError, metadata);
                    return;
                }

                topic.ChangeMetadataState(true, null, metadata);

                var brokerPartitions = new List<KafkaClientBrokerPartition>(topic.Partitions.Count);
                foreach (var topicPartition in topic.Partitions)
                {
                    brokerPartitions.Add(topicPartition.BrokerPartition);
                }

                try
                {
                    ArrangeTopic?.Invoke(topic.TopicName, brokerPartitions);
                    topic.Status = KafkaClientTopicStatus.Ready;
                }
                catch (Exception)
                {
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, KafkaClientTopicErrorCode.InternalError, null);
                    return;
                }
            }

            if (topic.Status == KafkaClientTopicStatus.Ready)
            {
                topic.Producer?.Flush();

                foreach (var paritition in topic.Partitions)
                {
                    if (paritition.BrokerPartition.Status == KafkaClientBrokerPartitionStatus.RearrangeRequired)
                    {
                        topic.Status = KafkaClientTopicStatus.RearrangeRequired;
                        break;
                    }
                }
            }

            if (topic.Status == KafkaClientTopicStatus.RearrangeRequired)
            {
                var areAllUnplugged = true;
                foreach (var paritition in topic.Partitions)
                {
                    paritition.BrokerPartition.IsUnplugRequired = true;
                    if (paritition.BrokerPartition.Status != KafkaClientBrokerPartitionStatus.Unplugged)
                    {
                        areAllUnplugged = false;
                    }
                }

                if (areAllUnplugged)
                {
                    topic.Status = KafkaClientTopicStatus.NotInitialized;
                }
            }
        }

        private void ProcessGroup([NotNull] KafkaClientGroup group)
        {
            if (group.Status == KafkaClientGroupStatus.NotInitialized)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataResponse = metadataBroker.RequestGroupCoordinator(group.GroupName);
                    var metadataRequestId = metadataResponse.HasData ? metadataResponse.Data : null;
                    if (metadataRequestId.HasValue)
                    {
                        _groupMetadataRequests[group.GroupName] = new MetadataRequestInfo(metadataRequestId.Value, metadataBroker);
                        group.Status = KafkaClientGroupStatus.MetadataRequested;
                    }
                }

            }

            if (group.Status == KafkaClientGroupStatus.MetadataRequested)
            {
                MetadataRequestInfo metadataRequest;
                if (!_groupMetadataRequests.TryGetValue(group.GroupName, out metadataRequest) || metadataRequest == null)
                {
                    group.Status = KafkaClientGroupStatus.NotInitialized;
                    return;
                }

                var groupCoordinatorMetadata = metadataRequest.Broker.GetGroupCoordinator(metadataRequest.RequestId);
                if (groupCoordinatorMetadata.HasData && groupCoordinatorMetadata.Data != null)
                {
                    group.ApplyCoordinator(groupCoordinatorMetadata.Data);
                    var groupCoordinator = group.BrokerGroup;
                    if (groupCoordinator == null)
                    {
                        group.Status = KafkaClientGroupStatus.NotInitialized;
                        return;
                    }

                    try
                    {
                        ArrangeGroup?.Invoke(group.GroupName, groupCoordinator);
                        group.Status = KafkaClientGroupStatus.Ready;
                    }
                    catch (Exception)
                    {
                        group.Status = KafkaClientGroupStatus.NotInitialized;
                        return;
                    }
                }
                else
                {
                    if (groupCoordinatorMetadata.HasError)
                    {
                        group.Status = KafkaClientGroupStatus.NotInitialized;
                        return;
                    }
                }
            }

            if (group.Status == KafkaClientGroupStatus.Ready)
            {
                if (group.BrokerGroup?.Status == KafkaClientBrokerGroupStatus.RearrangeRequired)
                {
                    group.Status = KafkaClientGroupStatus.RearrangeRequired;
                }
            }

            if (group.Status == KafkaClientGroupStatus.RearrangeRequired)
            {
                var brokerGroup = group.BrokerGroup;
                if (brokerGroup != null)
                {
                    brokerGroup.IsUnplugRequired = true;                    
                }

                if (group.BrokerGroup?.Status == KafkaClientBrokerGroupStatus.Unplugged)
                {
                    group.Status = KafkaClientGroupStatus.NotInitialized;
                }
            }
        }

        private void ProcessBroker([NotNull]KafkaClientBroker broker, bool isBrokerRequired)
        {
            if (!isBrokerRequired)
            {
                if (broker.IsOpenned)
                {
                    broker.Close();
                }
                return;
            }

            if (!broker.IsOpenned)
            {
                broker.Open();
            }

            broker.Work();            
        }

        private void ProcessMetadataBroker([NotNull] KafkaClientBroker metadataBroker, bool isMetadataBrokerRequired)
        {
            if (!isMetadataBrokerRequired)
            {
                if (metadataBroker.IsOpenned)
                {
                    metadataBroker.Close();
                }
                return;
            }            

            if (!metadataBroker.IsOpenned)
            {
                metadataBroker.Open();
            }

            metadataBroker.Work();
        }

        [CanBeNull]
        private KafkaClientBroker GetMetadataBroker()
        {
            foreach (var brokerPair in _brokers)
            {
                var broker = brokerPair.Value;
                if (broker == null) continue;

                if (broker.IsEnabled)
                {
                    return broker;
                }
            }

            foreach (var broker in _metadataBrokers)
            {
                if (broker.IsEnabled)
                {
                    return broker;
                }
            }

            return null;
        }

        [NotNull]
        private KafkaClientBroker CreateBroker([NotNull]KafkaBrokerMetadata brokerMetadata)
        {
            var brokerId = brokerMetadata.BrokerId;
            var host = brokerMetadata.Host ?? string.Empty;
            var port = brokerMetadata.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{brokerId} ({host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaClientBroker(broker, _settings);
        }

        [NotNull]
        private KafkaClientBroker CreateMetadataBroker([NotNull]KafkaBrokerInfo brokerInfo)
        {
            var host = brokerInfo.Host ?? string.Empty;
            var port = brokerInfo.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaClientBroker(broker, _settings);
        }

        #region Topic metadata

        [NotNull]
        private static KafkaTopicMetadataRequest CreateTopicMetadataRequest([NotNull] string topicName)
        {
            return new KafkaTopicMetadataRequest(new[] { topicName });
        }

        private static KafkaClientTopicErrorCode? ConvertMetadataRequestError(KafkaBrokerErrorCode? errorCode)
        {           
            KafkaClientTopicErrorCode? topicErrorCode = null;
            if (errorCode.HasValue)
            {
                switch (errorCode.Value)
                {
                    case KafkaBrokerErrorCode.BadRequest:
                        topicErrorCode = KafkaClientTopicErrorCode.ProtocolError;
                        break;
                    case KafkaBrokerErrorCode.InvalidState:
                        topicErrorCode = KafkaClientTopicErrorCode.InvalidState;
                        break;
                    case KafkaBrokerErrorCode.DataError:
                        topicErrorCode = KafkaClientTopicErrorCode.ProtocolError;
                        break;
                    case KafkaBrokerErrorCode.TransportError:
                        topicErrorCode = KafkaClientTopicErrorCode.TransportError;
                        break;                                        
                    case KafkaBrokerErrorCode.Timeout:
                        topicErrorCode = KafkaClientTopicErrorCode.TransportError;
                        break;
                    default:
                        topicErrorCode = KafkaClientTopicErrorCode.UnknownError;
                        break;
                }
            }

            return topicErrorCode;            
        }

        [NotNull]
        private static KafkaTopicMetadata ConvertMetadata([NotNull] string topicName, KafkaTopicMetadataResponse responseData, out bool hasError)
        {
            hasError = false;     
            var responseBrokers = responseData?.Brokers ?? new KafkaTopicMetadataResponseBroker[0];
            var responseTopics = responseData?.Topics ?? new KafkaTopicMetadataResponseTopic[0];
            
            var responseTopic = responseTopics.Count >= 1 ? responseTopics[0] : null;
            if (string.IsNullOrEmpty(responseTopic?.TopicName))
            {
                hasError = true;
                return new KafkaTopicMetadata(topicName, KafkaTopicMetadataError.InvalidTopic, new KafkaBrokerMetadata[0], new KafkaTopicPartitionMetadata[0]);
            }

            KafkaTopicMetadataError? topicError = null;
            if (responseTopic.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                hasError = true;
                switch (responseTopic.ErrorCode)
                {
                    case KafkaResponseErrorCode.UnknownTopicOrPartition:
                        topicError = KafkaTopicMetadataError.UnknownTopic;
                        break;
                    case KafkaResponseErrorCode.InvalidTopic:
                        topicError = KafkaTopicMetadataError.InvalidTopic;
                        break;
                    case KafkaResponseErrorCode.TopicAuthorizationFailed:
                        topicError = KafkaTopicMetadataError.TopicAuthorizationFailed;
                        break;
                    default:
                        topicError = KafkaTopicMetadataError.UnknownError;
                        break;
                }
            }
            
            var responsePartitons = responseTopic.Partitions ?? new KafkaTopicMetadataResponseTopicPartition[0];

            var brokers = new List<KafkaBrokerMetadata>(responseBrokers.Count);
            foreach (var responseBroker in responseBrokers)
            {
                if (responseBroker == null) continue;                
                brokers.Add(new KafkaBrokerMetadata(responseBroker.BrokerId, responseBroker.Host, responseBroker.Port, responseBroker.Rack));
            }

            var partitions = new List<KafkaTopicPartitionMetadata>(responsePartitons.Count);
            foreach (var responsePartition in responsePartitons)
            {                
                if (responsePartition == null) continue;
                KafkaTopicPartitionMetadataError? partitionError = null;
                if (responsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
                {
                    hasError = true;
                    switch (responsePartition.ErrorCode)
                    {
                        case KafkaResponseErrorCode.UnknownTopicOrPartition:
                            partitionError = KafkaTopicPartitionMetadataError.UnknownPartition;
                            break;
                        case KafkaResponseErrorCode.LeaderNotAvailable:
                            partitionError = KafkaTopicPartitionMetadataError.LeaderNotAvailable;
                            break;                        
                        default:
                            partitionError = KafkaTopicPartitionMetadataError.UnknownError;
                            break;
                    }
                }                
                partitions.Add(new KafkaTopicPartitionMetadata(responsePartition.PartitionId, partitionError, responsePartition.LeaderId));
            }

            return new KafkaTopicMetadata(responseTopic.TopicName, topicError, brokers, partitions);
        }

        private sealed class MetadataRequestInfo
        {
            public readonly int RequestId;
            [NotNull]
            public readonly KafkaClientBroker Broker;

            public MetadataRequestInfo(int requestId, [NotNull] KafkaClientBroker broker)
            {
                RequestId = requestId;
                Broker = broker;
            }
        }

        #endregion Topic metadata
    }
}
