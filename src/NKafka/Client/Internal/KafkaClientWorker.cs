using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientWorker
    {
        [NotNull]
        private readonly KafkaProtocol _protocol;
        [NotNull]
        private readonly KafkaClientSettings _settings;

        [NotNull]
        private readonly ConcurrentDictionary<string, KafkaClientTopic> _topics;
        [NotNull]
        private readonly ConcurrentDictionary<string, TopicMetadataRequest> _topicMetadataRequests;
        [NotNull]
        private readonly ConcurrentDictionary<int, KafkaClientBroker> _brokers;
        [NotNull, ItemNotNull]
        private readonly IReadOnlyCollection<KafkaClientBroker> _metadataBrokers;

        [NotNull]
        private Timer _workerTimer;
        private readonly TimeSpan _workerPeriod;
        [NotNull]
        private CancellationTokenSource _workerCancellation;

        public delegate void ArrangeTopicDelegate([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaClientBrokerPartition> partitions);
        public event ArrangeTopicDelegate ArrangeTopic;

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
            _topicMetadataRequests = new ConcurrentDictionary<string, TopicMetadataRequest>();

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
            if (!_brokers.TryGetValue(brokerId, out broker))
            {
                broker = CreateBroker(topicPartition.BrokerMetadata, topicPartition.Producer != null);
                _brokers[brokerId] = broker;
            }

            broker.AddTopicPartition(topicName, topicPartition);
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
                var produceTimer = _workerTimer;
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
                if (_workerCancellation.IsCancellationRequested) return;
                ProcessTopic(topic.Value);
                hasTopics = true;
            }

            var isBrokersRequired = hasTopics;
            bool isAvailableRegularBrokers = false;
            foreach (var brokerPair in _brokers)
            {
                if (_workerCancellation.IsCancellationRequested) return;
                ProcessBroker(brokerPair.Value, isBrokersRequired);
                if (brokerPair.Value.IsEnabled)
                {
                    isAvailableRegularBrokers = true;
                }
            }

            var isMetadataBrokersRequired = hasTopics && !isAvailableRegularBrokers;
            foreach (var metadataBroker in _metadataBrokers)
            {
                if (_workerCancellation.IsCancellationRequested) return;
                ProcessMetadataBroker(metadataBroker, isMetadataBrokersRequired);
            }

            if (_workerCancellation.IsCancellationRequested) return;
            var produceTimer = _workerTimer;
            lock (produceTimer)
            {
                if (_workerCancellation.IsCancellationRequested) return;

                try
                {
                    produceTimer.Change(_workerPeriod, Timeout.InfiniteTimeSpan);
                }
                catch (Exception)
                {
                    //ignored
                }
            }
        }

        private void ProcessTopic([NotNull] KafkaClientTopic topic)
        {
            if (topic.Status == KafkaClientTopicStatus.NotInitialized)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataResponse = metadataBroker.RequestTopicMetadata(topic.TopicName);
                    var metadataRequestId = metadataResponse.HasData ? metadataResponse.Data : null;
                    if (metadataRequestId.HasValue)
                    {
                        _topicMetadataRequests[topic.TopicName] = new TopicMetadataRequest(metadataRequestId.Value, metadataBroker);
                        topic.Status = KafkaClientTopicStatus.MetadataRequested;
                    }
                }

            }

            if (topic.Status == KafkaClientTopicStatus.MetadataRequested)
            {
                TopicMetadataRequest metadataRequest;
                if (!_topicMetadataRequests.TryGetValue(topic.TopicName, out metadataRequest))
                {
                    topic.Status = KafkaClientTopicStatus.NotInitialized;
                    return;
                }

                var topicMetadata = metadataRequest.Broker.GetTopicMetadata(metadataRequest.RequestId);
                if (topicMetadata.HasData)
                {
                    topic.ApplyMetadata(topicMetadata.Data);
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
                        topic.Status = KafkaClientTopicStatus.NotInitialized;
                        return;
                    }
                }
                else
                {
                    if (topicMetadata.HasError)
                    {
                        topic.Status = KafkaClientTopicStatus.NotInitialized;
                        return;
                    }
                }
            }

            if (topic.Status == KafkaClientTopicStatus.Ready)
            {
                topic.Work();

                foreach (var paritition in topic.Partitions)
                {
                    if (paritition.BrokerPartition.Status == KafkaClientBrokerPartitionStatus.NeedRearrange)
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
        private KafkaClientBroker CreateBroker([NotNull]KafkaBrokerMetadata brokerMetadata, bool hasProducer)
        {
            var brokerId = brokerMetadata.BrokerId;
            var host = brokerMetadata.Host ?? string.Empty;
            var port = brokerMetadata.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{brokerId} ({host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaClientBroker(broker, _settings, hasProducer);
        }

        [NotNull]
        private KafkaClientBroker CreateMetadataBroker([NotNull]KafkaBrokerInfo brokerInfo)
        {
            var host = brokerInfo.Host ?? string.Empty;
            var port = brokerInfo.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaClientBroker(broker, _settings, false);
        }

        private sealed class TopicMetadataRequest
        {
            public readonly int RequestId;
            [NotNull]
            public readonly KafkaClientBroker Broker;

            public TopicMetadataRequest(int requestId, KafkaClientBroker broker)
            {
                RequestId = requestId;
                Broker = broker;
            }
        }
    }
}
