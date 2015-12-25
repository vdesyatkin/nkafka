using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;

namespace NKafka.Consumer.Internal
{
    internal sealed class KafkaConsumerWorker
    {
        [NotNull]
        private readonly KafkaProtocol _protocol;
        [NotNull]
        private readonly KafkaConsumerSettings _settings;

        [NotNull]
        private readonly ConcurrentDictionary<string, KafkaConsumerTopic> _topics;
        [NotNull]
        private readonly ConcurrentDictionary<string, TopicMetadataRequest> _topicMetadataRequests;
        [NotNull]
        private readonly ConcurrentDictionary<int, KafkaConsumerBroker> _brokers;
        [NotNull, ItemNotNull]
        private readonly IReadOnlyCollection<KafkaConsumerBroker> _metadataBrokers;

        [NotNull]
        private Timer _consumeTimer;
        private readonly TimeSpan _consumePeriod;
        [NotNull]
        private CancellationTokenSource _consumeCancellation;

        public delegate void ArrangeTopicDelegate([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaConsumerBrokerPartition> partitions);
        public event ArrangeTopicDelegate ArrangeTopic;

        public KafkaConsumerWorker([NotNull] KafkaConsumerSettings settings)
        {
            _settings = settings;
            _protocol = new KafkaProtocol(_settings.KafkaVersion, _settings.ClientId);
            _consumePeriod = settings.ConsumePeriod;
            if (_consumePeriod < TimeSpan.FromMilliseconds(100))
            {
                _consumePeriod = TimeSpan.FromMilliseconds(100);
            }

            _brokers = new ConcurrentDictionary<int, KafkaConsumerBroker>();
            _topics = new ConcurrentDictionary<string, KafkaConsumerTopic>();
            _topicMetadataRequests = new ConcurrentDictionary<string, TopicMetadataRequest>();

            var metadataBrokerInfos = _settings.MetadataBrokers ?? new KafkaBrokerInfo[0];
            var metadataBrokers = new List<KafkaConsumerBroker>(metadataBrokerInfos.Count);
            foreach (var metadataBrokerInfo in metadataBrokerInfos)
            {
                if (metadataBrokerInfo == null) continue;
                metadataBrokers.Add(CreateMetadataBroker(metadataBrokerInfo));
            }

            _metadataBrokers = metadataBrokers;
            _consumeCancellation = new CancellationTokenSource();
            _consumeTimer = new Timer(Work);
        }

        public void AssignTopic([NotNull] KafkaConsumerTopic topic)
        {
            _topics[topic.TopicName] = topic;
        }

        public void AssignTopicPartition([NotNull] string topicName, [NotNull] KafkaConsumerBrokerPartition topicPartition)
        {
            var brokerId = topicPartition.BrokerMetadata.BrokerId;
            KafkaConsumerBroker broker;
            if (!_brokers.TryGetValue(brokerId, out broker))
            {
                broker = CreateBroker(topicPartition.BrokerMetadata);
                _brokers[brokerId] = broker;
            }

            broker.AddTopicPartition(topicName, topicPartition);
        }

        public void Start()
        {
            _consumeCancellation = new CancellationTokenSource();
            var consumeTimer = new Timer(Work);
            _consumeTimer = consumeTimer;
            consumeTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }

        public void Stop()
        {
            try
            {
                _consumeCancellation.Cancel();
            }
            catch (Exception)
            {
                //ignored
            }

            try
            {
                var consumeTimer = _consumeTimer;
                lock (consumeTimer)
                {
                    consumeTimer.Dispose();
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
                if (_consumeCancellation.IsCancellationRequested) return;
                ProcessTopic(topic.Value);
                hasTopics = true;
            }

            var isBrokersRequired = hasTopics;
            bool isAvailableRegularBrokers = false;
            foreach (var brokerPair in _brokers)
            {
                if (_consumeCancellation.IsCancellationRequested) return;
                ProcessBroker(brokerPair.Value, isBrokersRequired);
                if (brokerPair.Value.IsEnabled)
                {
                    isAvailableRegularBrokers = true;
                }
            }

            var isMetadataBrokersRequired = hasTopics && !isAvailableRegularBrokers;
            foreach (var metadataBroker in _metadataBrokers)
            {
                if (_consumeCancellation.IsCancellationRequested) return;
                ProcessMetadataBroker(metadataBroker, isMetadataBrokersRequired);
            }

            if (_consumeCancellation.IsCancellationRequested) return;
            var consumeTimer = _consumeTimer;
            lock (consumeTimer)
            {
                if (_consumeCancellation.IsCancellationRequested) return;

                try
                {
                    consumeTimer.Change(_consumePeriod, Timeout.InfiniteTimeSpan);
                }
                catch (Exception)
                {
                    //ignored
                }
            }
        }

        private void ProcessTopic([NotNull] KafkaConsumerTopic topic)
        {
            if (topic.Status == KafkaConsumerTopicStatus.NotInitialized)
            {
                var metadataBroker = GetMetadataBroker();
                if (metadataBroker != null)
                {
                    var metadataResponse = metadataBroker.RequestTopicMetadata(topic.TopicName);
                    var metadataRequestId = metadataResponse.HasData ? metadataResponse.Data : null;
                    if (metadataRequestId.HasValue)
                    {
                        _topicMetadataRequests[topic.TopicName] = new TopicMetadataRequest(metadataRequestId.Value, metadataBroker);
                        topic.Status = KafkaConsumerTopicStatus.MetadataRequested;
                    }
                }

            }

            if (topic.Status == KafkaConsumerTopicStatus.MetadataRequested)
            {
                TopicMetadataRequest metadataRequest;
                if (!_topicMetadataRequests.TryGetValue(topic.TopicName, out metadataRequest))
                {
                    topic.Status = KafkaConsumerTopicStatus.NotInitialized;
                    return;
                }

                var topicMetadata = metadataRequest.Broker.GetTopicMetadata(metadataRequest.RequestId);
                if (topicMetadata.HasData)
                {
                    topic.ApplyMetadata(topicMetadata.Data);
                    var brokerPartitions = new List<KafkaConsumerBrokerPartition>(topic.Partitions.Count);
                    foreach (var topicPartition in topic.Partitions)
                    {
                        brokerPartitions.Add(topicPartition.BrokerPartition);
                    }

                    try
                    {
                        ArrangeTopic?.Invoke(topic.TopicName, brokerPartitions);
                        topic.Status = KafkaConsumerTopicStatus.Ready;
                    }
                    catch (Exception)
                    {
                        topic.Status = KafkaConsumerTopicStatus.NotInitialized;
                        return;
                    }
                }
                else
                {
                    if (topicMetadata.HasError)
                    {
                        topic.Status = KafkaConsumerTopicStatus.NotInitialized;
                        return;
                    }
                }
            }

            if (topic.Status == KafkaConsumerTopicStatus.Ready)
            {                
                foreach (var paritition in topic.Partitions)
                {
                    if (paritition.BrokerPartition.Status == KafkaConsumerBrokerPartitionStatus.NeedRearrange)
                    {
                        topic.Status = KafkaConsumerTopicStatus.RearrangeRequired;
                        break;
                    }
                }
            }

            if (topic.Status == KafkaConsumerTopicStatus.RearrangeRequired)
            {
                var areAllUnplugged = true;
                foreach (var paritition in topic.Partitions)
                {
                    paritition.BrokerPartition.IsUnplugRequired = true;
                    if (paritition.BrokerPartition.Status != KafkaConsumerBrokerPartitionStatus.Unplugged)
                    {
                        areAllUnplugged = false;
                    }
                }

                if (areAllUnplugged)
                {
                    topic.Status = KafkaConsumerTopicStatus.NotInitialized;
                }
            }
        }

        private void ProcessBroker([NotNull]KafkaConsumerBroker broker, bool isBrokerRequired)
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
            broker.PerformConsume();
        }

        private void ProcessMetadataBroker([NotNull] KafkaConsumerBroker metadataBroker, bool isMetadataBrokerRequired)
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
        private KafkaConsumerBroker GetMetadataBroker()
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
        private KafkaConsumerBroker CreateBroker([NotNull]KafkaBrokerMetadata brokerMetadata)
        {
            var brokerId = brokerMetadata.BrokerId;
            var host = brokerMetadata.Host ?? string.Empty;
            var port = brokerMetadata.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{brokerId} ({host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaConsumerBroker(broker, _settings);
        }

        [NotNull]
        private KafkaConsumerBroker CreateMetadataBroker([NotNull]KafkaBrokerInfo brokerInfo)
        {
            var host = brokerInfo.Host ?? string.Empty;
            var port = brokerInfo.Port;
            var connection = new KafkaConnection(host, port);
            var brokerName = $"{host}:{port})";
            var broker = new KafkaBroker(connection, _protocol, brokerName, _settings.ConnectionSettings);
            return new KafkaConsumerBroker(broker, _settings);
        }

        private sealed class TopicMetadataRequest
        {
            public readonly int RequestId;
            [NotNull]
            public readonly KafkaConsumerBroker Broker;

            public TopicMetadataRequest(int requestId, KafkaConsumerBroker broker)
            {
                RequestId = requestId;
                Broker = broker;
            }
        }
    }
}