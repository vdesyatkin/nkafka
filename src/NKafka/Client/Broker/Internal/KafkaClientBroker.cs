using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Broker.Diagnostics;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;

namespace NKafka.Client.Broker.Internal
{
    internal sealed class KafkaClientBroker : IKafkaClientBroker
    {
        public string Name { get; }
        public int WorkerId { get; }
        public KafkaClientBrokerType BrokerType { get; }
        public KafkaBrokerMetadata BrokerMetadata { get; }

        public bool IsEnabled => _broker.IsOpenned && _broker.Error == null;
        public bool IsStarted => _broker.IsOpenned;

        [CanBeNull] public IKafkaClientLogger Logger { get; }

        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerTopic> _topics;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerGroup> _groups;
        [NotNull] private readonly KafkaProducerBroker _producer;
        [NotNull] private readonly KafkaConsumerBroker _consumer;
        [NotNull] private readonly KafkaCoordinatorBroker _coordinator;

        private readonly TimeSpan _clientTimeout;

        public KafkaClientBroker([NotNull] KafkaProtocol protocol, int workerId,
            KafkaClientBrokerType brokerType, [NotNull] KafkaBrokerMetadata metadata,
            [NotNull] KafkaClientSettings settings,
            [CanBeNull] IKafkaClientLogger logger)
        {
            BrokerType = brokerType;
            BrokerMetadata = metadata;
            Logger = logger;
            WorkerId = workerId;

            var brokerId = metadata.BrokerId;
            var host = metadata.Host ?? string.Empty;
            var port = metadata.Port;
            var brokerName = brokerType == KafkaClientBrokerType.MetadataBroker
                ? $"broker(metadata)[{host}:{port}]"
                : $"broker(id={brokerId})[{host}:{port}]";
            Name = brokerName;

            var loggerWrapper = logger != null ? new KafkaClientBrokerLogger(this, logger) : null;
            var broker = new KafkaBroker(brokerName, host, port, protocol, settings.ConnectionSettings, loggerWrapper);
            _broker = broker;

            _topics = new ConcurrentDictionary<string, KafkaClientBrokerTopic>();
            _groups = new ConcurrentDictionary<string, KafkaClientBrokerGroup>();
            _producer = new KafkaProducerBroker(broker, this, settings.WorkerPeriod);
            _consumer = new KafkaConsumerBroker(broker, this, settings.WorkerPeriod);
            _coordinator = new KafkaCoordinatorBroker(broker, this, settings.WorkerPeriod);

            var workerPeriod = settings.WorkerPeriod;
            if (workerPeriod < TimeSpan.FromMilliseconds(100))
            {
                workerPeriod = TimeSpan.FromMilliseconds(100);
            }
            _clientTimeout = workerPeriod + TimeSpan.FromSeconds(1) + workerPeriod;
        }

        public void Work(CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;
            _broker.Maintenance(cancellation);

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                if (cancellation.IsCancellationRequested) return;
                ProcessTopic(topic, cancellation);
            }

            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                if (cancellation.IsCancellationRequested) return;
                ProcessGroup(group, cancellation);
            }

            if (cancellation.IsCancellationRequested) return;
            _producer.Produce(cancellation);

            if (cancellation.IsCancellationRequested) return;
            _coordinator.Process(cancellation);

            if (cancellation.IsCancellationRequested) return;
            _consumer.Consume(cancellation);
        }

        public void EnableConsume()
        {
            _consumer.IsConsumeEnabled = true;
        }

        public void DisableConsume()
        {
            _consumer.IsConsumeEnabled = false;
        }

        public void Start(CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;
            _broker.Open(cancellation);

            _producer.Start();
            _coordinator.Start();
            _consumer.Start();
        }

        public void Stop()
        {
            _producer.Stop();
            _coordinator.Stop();
            _consumer.Stop();

            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                group.Status = KafkaClientBrokerGroupStatus.RearrangeRequired;
            }

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition == null) continue;

                    partition.Status = KafkaClientBrokerPartitionStatus.RearrangeRequired;
                }
            }
            _broker.Close();
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaClientBrokerPartition topicPartition)
        {
            KafkaClientBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic) || topic == null)
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaClientBrokerTopic(topicName), (oldKey, oldValue) => oldValue);
            }

            if (topic != null)
            {
                topic.Partitions[topicPartition.PartitionId] = topicPartition;
            }
        }

        public void AddGroupCoordinator([NotNull] string groupName, [NotNull] KafkaClientBrokerGroup group)
        {
            _groups[groupName] = group;
        }

        private void ProcessTopic([NotNull] KafkaClientBrokerTopic topic, CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;

            foreach (var partitionPair in topic.Partitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;
                if (cancellation.IsCancellationRequested) return;

                if (partition.IsUnplugRequired)
                {
                    partition.Status = KafkaClientBrokerPartitionStatus.Unplugged;
                    partition.Producer?.Unplug();
                    partition.Consumer?.Unplug();
                    var partitionId = partition.PartitionId;
                    topic.Partitions.TryRemove(partitionPair.Key, out partition);
                    _producer.RemoveTopicPartition(topic.TopicName, partitionId);
                    _consumer.RemoveTopicPartition(topic.TopicName, partitionId);
                    continue;
                }

                if (partition.Status == KafkaClientBrokerPartitionStatus.Unplugged)
                {
                    if (partition.Producer != null)
                    {
                        _producer.AddTopicPartition(partition.TopicName, partition.Producer);
                    }

                    if (partition.Consumer != null)
                    {
                        _consumer.AddTopicPartition(partition.TopicName, partition.Consumer);
                    }

                    partition.Status = KafkaClientBrokerPartitionStatus.Plugged;
                }

                if (partition.Status == KafkaClientBrokerPartitionStatus.Plugged)
                {
                    if (partition.Producer?.Status == KafkaProducerBrokerPartitionStatus.RearrangeRequired || partition.Consumer?.Status == KafkaConsumerBrokerPartitionStatus.RearrangeRequired)
                    {
                        partition.Status = KafkaClientBrokerPartitionStatus.RearrangeRequired;
                    }
                }
            }
        }

        private void ProcessGroup([NotNull] KafkaClientBrokerGroup group, CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;

            if (group.IsUnplugRequired)
            {
                KafkaClientBrokerGroup removedGroup;
                _groups.TryRemove(group.GroupName, out removedGroup);
                _coordinator.RemoveGroup(group.GroupName);
                group.Status = KafkaClientBrokerGroupStatus.Unplugged;
                return;
            }

            if (group.Status == KafkaClientBrokerGroupStatus.Unplugged)
            {
                group.Coordinator.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                _coordinator.AddGroup(group.GroupName, group.Coordinator);
                group.Status = KafkaClientBrokerGroupStatus.Plugged;
            }

            if (group.Status == KafkaClientBrokerGroupStatus.Plugged)
            {
                if (group.Coordinator.Status == KafkaCoordinatorGroupStatus.RearrangeRequired)
                {
                    group.Status = KafkaClientBrokerGroupStatus.RearrangeRequired;
                }
            }
        }

        public KafkaBrokerResult<int?> SendRequest<TRequest>([NotNull] TRequest request, [NotNull] string sender)
            where TRequest : class, IKafkaRequest
        {
            return _broker.Send(request, sender, _clientTimeout);
        }

        public KafkaBrokerResult<TResponse> GetResponse<TResponse>(int requestId)
            where TResponse : class, IKafkaResponse
        {
            return _broker.Receive<TResponse>(requestId);
        }

        public KafkaClientBrokerInfo GetDiagnosticsInfo()
        {
            return new KafkaClientBrokerInfo(Name, BrokerType, BrokerMetadata, _broker.IsOpenned, _broker.Error,
                _broker.ConnectionTimestampUtc, _broker.LastActivityTimestampUtc, DateTime.UtcNow);
        }
    }
}