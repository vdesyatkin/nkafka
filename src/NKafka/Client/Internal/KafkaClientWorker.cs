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
using NKafka.Protocol.API.GroupCoordinator;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientWorker
    {
        private readonly int _workerId;
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
        
        public KafkaClientWorker(int workerId, [NotNull] KafkaClientSettings settings)
        {
            _workerId = workerId;
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

        [NotNull]
        public KafkaClientWorkerInfo GetDiagnosticsInfo()
        {
            var topicInfos = new List<KafkaClientTopicMetadataInfo>();
            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;                
                if (topic == null) continue;

                var topicInfo = topic.MetadataInfo;
                topicInfos.Add(topicInfo);
            }

            var groupInfos = new List<KafkaClientGroupMetadataInfo>();
            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                var groupInfo = group.MetadataInfo;
                groupInfos.Add(groupInfo);
            }

            var brokerInfos = new List<KafkaClientBrokerInfo>();
            foreach (var brokerPair in _brokers)
            {
                var broker = brokerPair.Value;
                if (broker == null) continue;

                var brokerInfo = broker.GetDiagnosticsInfo();
                brokerInfos.Add(brokerInfo);
            }

            var metadataBrokerInfos = new List<KafkaClientBrokerInfo>();
            foreach (var metadataBroker in _metadataBrokers)
            {                                
                var metadataBrokerInfo = metadataBroker.GetDiagnosticsInfo();
                metadataBrokerInfos.Add(metadataBrokerInfo);
            }

            return new KafkaClientWorkerInfo(_workerId, topicInfos, groupInfos, brokerInfos, metadataBrokerInfos, DateTime.UtcNow);
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

        public void DisableConsume()
        {
            foreach (var broker in _brokers)
            {
                broker.Value?.DisableConsume();
            }
        }

        public void EnableConsume()
        {
            foreach (var broker in _brokers)
            {
                broker.Value?.EnableConsume();
            }
        }

        public bool IsAllTopicsSynchronized()
        {            
            foreach (var topic in _topics)
            {
                if (topic.Value?.Producer?.IsSynchronized == false) return false;
                if (topic.Value?.Consumer?.IsSynchronized == false) return false;
            }
            return true;
        }

        public void Start()
        {
            EnableConsume();
            _workerCancellation = new CancellationTokenSource();
            var produceTimer = new Timer(Work);
            // ReSharper disable once InconsistentlySynchronizedField
            _workerTimer = produceTimer;
            produceTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }

        public void BeginStop()
        {
            try
            {
                _workerCancellation.Cancel();
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void EndStop()
        {            
            try
            {
                var workerTimer = _workerTimer;
                lock (workerTimer)
                {
                    _workerTimer.Dispose();
                }
            }
            catch (Exception)
            {
                //ignored
            }
            foreach (var topic in _topics)
            {
                topic.Value?.Flush();
            }
            foreach (var broker in _brokers)
            {
                broker.Value?.Stop();
            }
            foreach (var broker in _metadataBrokers)
            {
                broker.Stop();
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
            var cancellation = _workerCancellation.Token;
            if (cancellation.IsCancellationRequested) return;
            var workerTimer = _workerTimer;
            lock (workerTimer)
            {

                var hasGroups = false;
                foreach (var groupPair in _groups)
                {
                    var group = groupPair.Value;
                    if (group == null) continue;
                    if (cancellation.IsCancellationRequested) return;

                    ProcessGroup(group, cancellation);
                    hasGroups = true;
                }

                var hasTopics = false;
                foreach (var topicPair in _topics)
                {
                    var topic = topicPair.Value;
                    if (topic == null) continue;
                    if (cancellation.IsCancellationRequested) return;

                    ProcessTopic(topic);
                    hasTopics = true;
                }

                var isBrokersRequired = hasTopics;
                bool isRegularBrokerAvailable = false;
                foreach (var brokerPair in _brokers)
                {
                    var broker = brokerPair.Value;
                    if (broker == null) continue;
                    if (cancellation.IsCancellationRequested) return;

                    ProcessBroker(broker, isBrokersRequired, cancellation);
                    if (broker.IsEnabled)
                    {
                        isRegularBrokerAvailable = true;
                    }
                }

                var isMetadataBrokerRequired = (hasTopics || hasGroups) && !isRegularBrokerAvailable;
                foreach (var metadataBroker in _metadataBrokers)
                {
                    if (cancellation.IsCancellationRequested) return;
                    ProcessMetadataBroker(metadataBroker, isMetadataBrokerRequired, cancellation);
                }

                if (cancellation.IsCancellationRequested) return;

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
                if (DateTime.UtcNow - topic.MetadataInfo.TimestampUtc < _settings.MetadataErrorRetryPeriod) return;                
            }

            if (topic.Status == KafkaClientTopicStatus.NotInitialized || 
                topic.Status == KafkaClientTopicStatus.MetadataError ||
                topic.Status == KafkaClientTopicStatus.Rearrange)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataRequest = CreateTopicMetadataRequest(topic.TopicName);
                    var metadataRequestResult =  metadataBroker.SendRequest(metadataRequest);
                    if (metadataRequestResult.HasError)
                    {
                        topic.Status = KafkaClientTopicStatus.MetadataError;
                        topic.ChangeMetadataState(false, ConvertTopicMetadataRequestError(metadataRequestResult.Error), null);                        
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

                if (!topicMetadataResponse.HasData && !topicMetadataResponse.HasError)
                {
                    // has not received
                    return;
                }

                if (topicMetadataResponse.HasError)
                {
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, ConvertTopicMetadataRequestError(topicMetadataResponse.Error), null);
                    return;
                }

                if (!topicMetadataResponse.HasData || topicMetadataResponse.Data == null)
                {                    
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, KafkaClientTopicMetadataErrorCode.ProtocolError, null);
                    return;
                }

                bool hasMetadataError;
                var metadata = ConvertTopicMetadata(topic.TopicName, topicMetadataResponse.Data, out hasMetadataError);                               

                if (hasMetadataError)
                {                 
                    topic.Status = KafkaClientTopicStatus.MetadataError;
                    topic.ChangeMetadataState(false, KafkaClientTopicMetadataErrorCode.MetadataError, metadata);
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
                    topic.ChangeMetadataState(false, KafkaClientTopicMetadataErrorCode.ClientError, null);
                    return;
                }
            }

            if (topic.Status == KafkaClientTopicStatus.Ready)
            {
                topic.Flush();

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
                    topic.Status = KafkaClientTopicStatus.Rearrange;
                }
            }
        }

        private void ProcessGroup([NotNull] KafkaClientGroup group, CancellationToken cancellation)
        {
            if (group.Status == KafkaClientGroupStatus.MetadataError)
            {
                if (DateTime.UtcNow - group.MetadataInfo.TimestampUtc < _settings.MetadataErrorRetryPeriod) return;
            }

            if (cancellation.IsCancellationRequested) return;

            if (group.Status == KafkaClientGroupStatus.NotInitialized || 
                group.Status == KafkaClientGroupStatus.MetadataError ||
                group.Status == KafkaClientGroupStatus.Rearrange)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataRequest = CreateGroupMetadataRequest(group.GroupName);
                    var metadataRequestResult = metadataBroker.SendRequest(metadataRequest);
                    if (metadataRequestResult.HasError)
                    {
                        group.Status = KafkaClientGroupStatus.MetadataError;
                        group.ChangeMetadataState(false, ConvertGroupMetadataRequestError(metadataRequestResult.Error), null);
                        return;
                    }

                    var metadataRequestId = metadataRequestResult.HasData ? metadataRequestResult.Data : null;
                    if (metadataRequestId.HasValue)
                    {
                        _groupMetadataRequests[group.GroupName] = new MetadataRequestInfo(metadataRequestId.Value, metadataBroker);
                        group.Status = KafkaClientGroupStatus.MetadataRequested;
                    }
                }
            }

            if (cancellation.IsCancellationRequested) return;

            if (group.Status == KafkaClientGroupStatus.MetadataRequested)
            {

                MetadataRequestInfo metadataRequest;
                if (!_groupMetadataRequests.TryGetValue(group.GroupName, out metadataRequest) || metadataRequest == null)
                {
                    group.Status = KafkaClientGroupStatus.MetadataError;
                    return;
                }

                var groupMetadataResponse = metadataRequest.Broker.GetResponse<KafkaGroupCoordinatorResponse>(metadataRequest.RequestId);

                if (!groupMetadataResponse.HasData && !groupMetadataResponse.HasError)
                {
                    // has not received
                    return;
                }

                if (groupMetadataResponse.HasError)
                {
                    group.Status = KafkaClientGroupStatus.MetadataError;
                    group.ChangeMetadataState(false, ConvertGroupMetadataRequestError(groupMetadataResponse.Error), null);
                    return;
                }

                if (!groupMetadataResponse.HasData || groupMetadataResponse.Data == null)
                {                    
                    group.Status = KafkaClientGroupStatus.MetadataError;
                    group.ChangeMetadataState(false, KafkaClientGroupMetadataErrorCode.ProtocolError, null);
                    return;
                }

                bool hasMetadataError;
                var metadata = ConvertGroupMetadata(group.GroupName, groupMetadataResponse.Data, out hasMetadataError);

                if (hasMetadataError)
                {
                    group.Status = KafkaClientGroupStatus.MetadataError;
                    group.ChangeMetadataState(false, KafkaClientGroupMetadataErrorCode.MetadataError, metadata);
                    return;
                }

                group.ChangeMetadataState(true, null, metadata);

                var groupCoordinator = group.BrokerGroup;
                if (groupCoordinator == null)
                {
                    group.Status = KafkaClientGroupStatus.MetadataError;
                    group.ChangeMetadataState(false, KafkaClientGroupMetadataErrorCode.MetadataError, null);
                    return;
                }
               
                try
                {
                    ArrangeGroup?.Invoke(group.GroupName, groupCoordinator);
                    group.Status = KafkaClientGroupStatus.Ready;
                }
                catch (Exception)
                {
                    group.Status = KafkaClientGroupStatus.MetadataError;
                    group.ChangeMetadataState(false, KafkaClientGroupMetadataErrorCode.ClientError, null);
                    return;
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

        private void ProcessBroker([NotNull]KafkaClientBroker broker, bool isBrokerRequired, CancellationToken cancellation)
        {
            if (!isBrokerRequired)
            {
                if (broker.IsStarted)
                {
                    broker.Stop();
                }
                return;
            }

            if (!broker.IsStarted)
            {
                broker.Start(cancellation);
            }

            broker.Work(cancellation);            
        }

        private void ProcessMetadataBroker([NotNull] KafkaClientBroker metadataBroker, bool isMetadataBrokerRequired, CancellationToken cancellation)
        {
            if (!isMetadataBrokerRequired)
            {
                if (metadataBroker.IsStarted)
                {
                    metadataBroker.Stop();
                }
                return;
            }            

            if (!metadataBroker.IsStarted)
            {
                metadataBroker.Start(cancellation);
            }

            metadataBroker.Work(cancellation);
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
            return new KafkaClientBroker(broker,  brokerMetadata, _settings);
        }

        [NotNull]
        private KafkaClientBroker CreateMetadataBroker([NotNull]KafkaBrokerInfo brokerInfo)
        {
            var host = brokerInfo.Host ?? string.Empty;
            var port = brokerInfo.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaClientBroker(broker, new KafkaBrokerMetadata(0, host, port, null), _settings);
        }

        #region Topic metadata

        [NotNull]
        private static KafkaTopicMetadataRequest CreateTopicMetadataRequest([NotNull] string topicName)
        {
            return new KafkaTopicMetadataRequest(new[] { topicName });
        }

        private static KafkaClientTopicMetadataErrorCode? ConvertTopicMetadataRequestError(KafkaBrokerErrorCode? errorCode)
        {           
            KafkaClientTopicMetadataErrorCode? topicErrorCode = null;
            if (errorCode.HasValue)
            {
                switch (errorCode.Value)
                {
                    case KafkaBrokerErrorCode.ConnectionClosed:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.ConnectionClosed;
                        break;
                    case KafkaBrokerErrorCode.ConnectionMaintenance:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.ClientMaintenance;
                        break;
                    case KafkaBrokerErrorCode.BadRequest:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.ProtocolError;
                        break;                    
                    case KafkaBrokerErrorCode.ProtocolError:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.ProtocolError;
                        break;
                    case KafkaBrokerErrorCode.TransportError:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.TransportError;
                        break;                                        
                    case KafkaBrokerErrorCode.ClientTimeout:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.ClientTimeout;
                        break;
                    case KafkaBrokerErrorCode.UnknownError:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.UnknownError;
                        break;
                    default:
                        topicErrorCode = KafkaClientTopicMetadataErrorCode.UnknownError;
                        break;
                }
            }

            return topicErrorCode;            
        }

        [NotNull]
        private static KafkaTopicMetadata ConvertTopicMetadata([NotNull] string topicName, [NotNull] KafkaTopicMetadataResponse responseData, out bool hasError)
        {
            hasError = false;     
            var responseBrokers = responseData.Brokers ?? new KafkaTopicMetadataResponseBroker[0];
            var responseTopics = responseData.Topics ?? new KafkaTopicMetadataResponseTopic[0];
            
            var responseTopic = responseTopics.Count >= 1 ? responseTopics[0] : null;
            if (string.IsNullOrEmpty(responseTopic?.TopicName))
            {
                hasError = true;
                return new KafkaTopicMetadata(topicName, KafkaTopicMetadataErrorCode.InvalidTopic, new KafkaBrokerMetadata[0], new KafkaTopicPartitionMetadata[0]);
            }

            KafkaTopicMetadataErrorCode? topicError = null;
            if (responseTopic.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                hasError = true;
                switch (responseTopic.ErrorCode)
                {
                    case KafkaResponseErrorCode.UnknownTopicOrPartition:
                        topicError = KafkaTopicMetadataErrorCode.UnknownTopic;
                        break;
                    case KafkaResponseErrorCode.InvalidTopic:
                        topicError = KafkaTopicMetadataErrorCode.InvalidTopic;
                        break;
                    case KafkaResponseErrorCode.TopicAuthorizationFailed:
                        topicError = KafkaTopicMetadataErrorCode.TopicAuthorizationFailed;
                        break;                    
                    default:
                        topicError = KafkaTopicMetadataErrorCode.UnknownError;
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
                KafkaTopicPartitionMetadataErrorCode? partitionError = null;
                if (responsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
                {
                    hasError = true;
                    switch (responsePartition.ErrorCode)
                    {
                        case KafkaResponseErrorCode.UnknownTopicOrPartition:
                            partitionError = KafkaTopicPartitionMetadataErrorCode.UnknownPartition;
                            break;
                        case KafkaResponseErrorCode.LeaderNotAvailable:
                            partitionError = KafkaTopicPartitionMetadataErrorCode.LeaderNotAvailable;
                            break;                        
                        default:
                            partitionError = KafkaTopicPartitionMetadataErrorCode.UnknownError;
                            break;
                    }
                }                
                partitions.Add(new KafkaTopicPartitionMetadata(responsePartition.PartitionId, partitionError, responsePartition.LeaderId));
            }

            return new KafkaTopicMetadata(responseTopic.TopicName, topicError, brokers, partitions);
        }

        #endregion Topic metadata

        #region Group metadata

        [NotNull]
        private static KafkaGroupCoordinatorRequest CreateGroupMetadataRequest([NotNull] string groupName)
        {
            return new KafkaGroupCoordinatorRequest(groupName);
        }

        private static KafkaClientGroupMetadataErrorCode? ConvertGroupMetadataRequestError(KafkaBrokerErrorCode? errorCode)
        {
            KafkaClientGroupMetadataErrorCode? groupErrorCode = null;
            if (errorCode.HasValue)
            {
                switch (errorCode.Value)
                {                    
                    case KafkaBrokerErrorCode.ConnectionClosed:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.ConnectionClosed;
                        break;
                    case KafkaBrokerErrorCode.ConnectionMaintenance:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.ClientMaintenance;
                        break;
                    case KafkaBrokerErrorCode.BadRequest:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.ProtocolError;
                        break;
                    case KafkaBrokerErrorCode.ProtocolError:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.ProtocolError;
                        break;
                    case KafkaBrokerErrorCode.TransportError:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.TransportError;
                        break;                    
                    case KafkaBrokerErrorCode.ClientTimeout:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.ClientTimeout;
                        break;
                    case KafkaBrokerErrorCode.UnknownError:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.UnknownError;
                        break;
                    default:
                        groupErrorCode = KafkaClientGroupMetadataErrorCode.UnknownError;
                        break;
                }
            }

            return groupErrorCode;
        }

        [NotNull]
        private static KafkaGroupMetadata ConvertGroupMetadata([NotNull] string groupName, [NotNull] KafkaGroupCoordinatorResponse responseData, out bool hasError)
        {
            hasError = false;
            
            KafkaGroupMetadataErrorCode? groupError = null;
            if (responseData.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                hasError = true;
                switch (responseData.ErrorCode)
                {
                    case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                        groupError = KafkaGroupMetadataErrorCode.CoordinatorNotAvailable;
                        break;
                    case KafkaResponseErrorCode.GroupAuthorizationFailed:
                        groupError = KafkaGroupMetadataErrorCode.GroupAuthorizationFailed;
                        break;
                    default:
                        groupError = KafkaGroupMetadataErrorCode.UnknownError;
                        break;
                }
            }
            
            var groupCoordinator = new KafkaBrokerMetadata(responseData.BrokerId, responseData.Host, responseData.Port, null);

            return new KafkaGroupMetadata(groupName, groupError, groupCoordinator);
        }

        #endregion Group metadata

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
    }
}
