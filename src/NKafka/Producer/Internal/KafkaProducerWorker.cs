using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;

namespace NKafka.Producer.Internal
{
    internal sealed class KafkaProducerWorker
    {
        [NotNull] private readonly KafkaProtocol _protocol;
        [NotNull] private readonly KafkaProducerSettings _settings;        

        [NotNull] private readonly ConcurrentDictionary<string, KafkaProducerTopic> _topics;
        [NotNull] private readonly ConcurrentDictionary<string, TopicMetadataRequest> _topicMetadataRequests;
        [NotNull] private readonly ConcurrentDictionary<int, KafkaProducerBroker> _brokers;
        [NotNull, ItemNotNull] private readonly IReadOnlyCollection<KafkaProducerBroker> _metadataBrokers;
        
        [NotNull] private Timer _produceTimer;
        private readonly TimeSpan _producePeriod;
        [NotNull] private CancellationTokenSource _produceCancellation;      
        
        public delegate void ArrangeTopicDelegate([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaProducerBrokerPartition> partitions);
        public event ArrangeTopicDelegate ArrangeTopic;

        public KafkaProducerWorker([NotNull] KafkaProducerSettings settings)
        {
            _settings = settings;
            _protocol = new KafkaProtocol(_settings.KafkaVersion, _settings.ClientId);
            _producePeriod = settings.ProducePeriod;
            if (_producePeriod < TimeSpan.FromMilliseconds(100))
            {
                _producePeriod = TimeSpan.FromMilliseconds(100);
            }

            _brokers = new ConcurrentDictionary<int, KafkaProducerBroker>();
            _topics = new ConcurrentDictionary<string, KafkaProducerTopic>();
            _topicMetadataRequests = new ConcurrentDictionary<string, TopicMetadataRequest>();

            var metadataBrokerInfos = _settings.MetadataBrokers ?? new KafkaBrokerInfo[0];
            var metadataBrokers = new List<KafkaProducerBroker>(metadataBrokerInfos.Count);
            foreach (var metadataBrokerInfo in metadataBrokerInfos)
            {
                if (metadataBrokerInfo == null) continue;
                metadataBrokers.Add(CreateMetadataBroker(metadataBrokerInfo));
            }

            _metadataBrokers = metadataBrokers;
            _produceCancellation = new CancellationTokenSource();            
            _produceTimer = new Timer(Work);
        }
        
        public void AssignTopic([NotNull] KafkaProducerTopic topic)
        {
            _topics[topic.TopicName] = topic;
        }

        public void AssignTopicPartition([NotNull] string topicName, [NotNull] KafkaProducerBrokerPartition topicPartition)
        {
            var brokerId = topicPartition.BrokerMetadata.BrokerId;
            KafkaProducerBroker broker;
            if (!_brokers.TryGetValue(brokerId, out broker))
            {
                broker = CreateBroker(topicPartition.BrokerMetadata);
                _brokers[brokerId] = broker;
            }

            broker.AddTopicPartition(topicName, topicPartition);
        }

        public void Start()
        {            
            _produceCancellation = new CancellationTokenSource();
            var produceTimer = new Timer(Work);
            _produceTimer = produceTimer;
            produceTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }

        public void Stop()
        {
            try
            {
                _produceCancellation.Cancel();
            }
            catch (Exception)
            {
                //ignored
            }            
            
            try
            {
                //todo sync and close brokers
                var produceTimer = _produceTimer;
                lock (produceTimer)
                {
                    produceTimer.Dispose();
                }
            }
            catch (Exception)
            {
                //ignored
            }
            foreach (var broker in _brokers)
            {                
                broker.Value.Close();
            }            
        }

        private void Work(object state)
        {
            var hasTopics = false;
            foreach (var topic in _topics)
            {
                if (_produceCancellation.IsCancellationRequested) return;
                ProcessTopic(topic.Value);
                hasTopics = true;
            }

            var isBrokersRequired = hasTopics;
            bool isAvailableRegularBrokers = false;
            foreach (var brokerPair in _brokers)
            {
                if (_produceCancellation.IsCancellationRequested) return;
                ProcessBroker(brokerPair.Value, isBrokersRequired);
                if (brokerPair.Value.IsEnabled)
                {
                    isAvailableRegularBrokers = true;
                }
            }

            var isMetadataBrokersRequired = hasTopics && !isAvailableRegularBrokers;
            foreach (var metadataBroker in _metadataBrokers)
            {
                if (_produceCancellation.IsCancellationRequested) return;
                ProcessMetadataBroker(metadataBroker, isMetadataBrokersRequired);
            }

            if (_produceCancellation.IsCancellationRequested) return;
            var produceTimer = _produceTimer;
            lock (produceTimer)
            {
                if (_produceCancellation.IsCancellationRequested) return;

                try
                {
                    produceTimer.Change(_producePeriod, Timeout.InfiniteTimeSpan);
                }
                catch (Exception)
                {
                    //ignored
                }
            }
        }

        private void ProcessTopic([NotNull] KafkaProducerTopic topic)
        {            
            if (topic.Status == KafkaProducerTopicStatus.NotInitialized)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataRequestId = metadataBroker.RequestTopicMetadta(topic.TopicName);
                    if (metadataRequestId.HasData)
                    {
                        _topicMetadataRequests[topic.TopicName] = new TopicMetadataRequest(metadataRequestId.Data, metadataBroker);
                        topic.Status = KafkaProducerTopicStatus.MetadataRequested;
                    }
                }
                                
            }

            if (topic.Status == KafkaProducerTopicStatus.MetadataRequested)
            {
                TopicMetadataRequest metadataRequest;
                if (!_topicMetadataRequests.TryGetValue(topic.TopicName, out metadataRequest))
                {
                    topic.Status = KafkaProducerTopicStatus.NotInitialized;
                    return;
                }

                var topicMetadata = metadataRequest.Broker.GetTopicMetadata(metadataRequest.RequestId);
                if (topicMetadata.HasData)
                {
                    var topicPartitions = CreateTopicPartitions(topicMetadata.Data);
                    var brokerPartitions = CreateBrokerPartitions(topicPartitions);
                    topic.Partitions = topicPartitions;

                    try
                    {
                        ArrangeTopic?.Invoke(topic.TopicName, brokerPartitions);
                        topic.Status = KafkaProducerTopicStatus.Ready;
                    }
                    catch (Exception)
                    {                                             
                        topic.Status = KafkaProducerTopicStatus.NotInitialized;
                        return;
                    }
                }
                else
                {
                    if (topicMetadata.HasError)
                    {
                        //todo
                        topic.Status = KafkaProducerTopicStatus.NotInitialized;
                        return;
                    }                    
                }
            }

            if (topic.Status == KafkaProducerTopicStatus.Ready)
            {        
                topic.Flush();
                        
                foreach (var paritition in topic.Partitions)
                {
                    if (paritition.BrokerPartition.Status == KafkaProducerBrokerPartitionStatus.NeedRearrange)
                    {
                        topic.Status = KafkaProducerTopicStatus.RearrangeRequired;                            
                        break;
                    }                    
                }
            }

            if (topic.Status == KafkaProducerTopicStatus.RearrangeRequired)
            {                
                var areAllUnplugged = true;
                foreach (var paritition in topic.Partitions)
                {
                    paritition.BrokerPartition.IsUnplugRequired = true;
                    if (paritition.BrokerPartition.Status != KafkaProducerBrokerPartitionStatus.Unplugged)
                    {
                        areAllUnplugged = false;
                    }
                }

                if (areAllUnplugged)
                {
                    topic.Status = KafkaProducerTopicStatus.NotInitialized;
                }
            }
        }

        private void ProcessBroker([NotNull]KafkaProducerBroker broker, bool isBrokerRequired)
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

            broker.Maintenance();
            broker.PerformProduce();            
        }

        private void ProcessMetadataBroker([NotNull] KafkaProducerBroker metadataBroker, bool isMetadataBrokerRequired)
        {
            if (!isMetadataBrokerRequired)
            {
                if (metadataBroker.IsOpenned)
                {
                    metadataBroker.Close();
                }
            }
            
            if (!metadataBroker.IsOpenned)
            {
                metadataBroker.Open();
            }

            metadataBroker.Maintenance();            
        }

        [CanBeNull]
        private KafkaProducerBroker GetMetadataBroker()
        {
            foreach (var broker in _brokers)
            {
                if (broker.Value.IsEnabled)
                {
                    return broker.Value;
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
        private static IReadOnlyList<KafkaProducerTopicPartition> CreateTopicPartitions([NotNull] KafkaTopicMetadata topicMetadata)
        {
            var partitionBrokers = new Dictionary<int, KafkaBrokerMetadata>(topicMetadata.Brokers.Count);
            foreach (var brokerMetadata in topicMetadata.Brokers)
            {
                partitionBrokers[brokerMetadata.BrokerId] = brokerMetadata;
            }

            var topicName = topicMetadata.TopicName;

            var topicPartitions = new List<KafkaProducerTopicPartition>(topicMetadata.Partitions.Count);            
            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                var brokerId = partitionMetadata.LeaderBrokerId;
                KafkaBrokerMetadata brokerMetadata;
                if (!partitionBrokers.TryGetValue(brokerId, out brokerMetadata))
                {                    
                    continue;
                }
                var partition = new KafkaProducerTopicPartition(topicName, partitionMetadata.PartitionId, brokerMetadata);
                topicPartitions.Add(partition);             
            }
            return topicPartitions;
        }

        [NotNull]
        private static IReadOnlyList<KafkaProducerBrokerPartition> CreateBrokerPartitions([NotNull] IReadOnlyList<KafkaProducerTopicPartition> topicPartitions)
        {           
            var brokerPartitions = new List<KafkaProducerBrokerPartition>(topicPartitions.Count);
            foreach (var topicPartition in topicPartitions)
            {                
                brokerPartitions.Add(topicPartition.BrokerPartition);
            }
            return brokerPartitions;
        }

        [NotNull]
        private KafkaProducerBroker CreateBroker([NotNull]KafkaBrokerMetadata brokerMetadata)
        {
            var brokerId = brokerMetadata.BrokerId;
            var host = brokerMetadata.Host ?? string.Empty;
            var port = brokerMetadata.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{brokerId} ({host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName);
            return new KafkaProducerBroker(broker, _settings);
        }

        [NotNull]
        private KafkaProducerBroker CreateMetadataBroker([NotNull]KafkaBrokerInfo brokerInfo)
        {
            var host = brokerInfo.Host ?? string.Empty;
            var port = brokerInfo.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName);
            return new KafkaProducerBroker(broker, _settings);
        }

        private sealed class TopicMetadataRequest
        {
            public readonly int RequestId;
            [NotNull] public readonly KafkaProducerBroker Broker;

            public TopicMetadataRequest(int requestId, KafkaProducerBroker broker)
            {
                RequestId = requestId;
                Broker = broker;
            }
        }
    }
}
