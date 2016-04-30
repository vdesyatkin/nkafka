using System;
using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Client.Diagnostics;
using NKafka.Client.Producer.Internal;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;

namespace NKafka.Client.Internal.Broker
{
    internal sealed class KafkaClientBroker
    {
        public bool IsEnabled => _broker.IsEnabled;
        public bool IsOpenned => _broker.IsOpenned;

        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly KafkaBrokerMetadata _metadata;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerTopic> _topics;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerGroup> _groups;
        [NotNull] private readonly KafkaProducerBroker _producer;
        [NotNull] private readonly KafkaConsumerBroker _consumer;
        [NotNull] private readonly KafkaCoordinatorBroker _coordinator;

        private readonly TimeSpan _clientTimeout;

        public KafkaClientBrokerInfo GetDiagnosticsInfo()
        {
            KafkaClientBrokerErrorCode? errorCode = null;
            var brokerErrorCode = _broker.Error;
            if (brokerErrorCode.HasValue)
            {
                switch (brokerErrorCode.Value)
                {
                    case KafkaBrokerStateErrorCode.ConnectionError:
                        errorCode = KafkaClientBrokerErrorCode.ConnectionError;
                        break;
                    case KafkaBrokerStateErrorCode.TransportError:
                        errorCode = KafkaClientBrokerErrorCode.TransportError;
                        break;
                    case KafkaBrokerStateErrorCode.ProtocolError:
                        errorCode = KafkaClientBrokerErrorCode.ProtocolError;
                        break;
                    case KafkaBrokerStateErrorCode.Timeout:
                        errorCode = KafkaClientBrokerErrorCode.Timeout;
                        break;
                    default:
                        errorCode = KafkaClientBrokerErrorCode.UnknownError;
                        break;
                }
            }
            return new KafkaClientBrokerInfo(_broker.Name, DateTime.UtcNow, 
                _metadata, _broker.IsOpenned, errorCode,
                _broker.ConnectionTimestampUtc, _broker.LastActivityTimestampUtc);
        }

        public KafkaClientBroker([NotNull] KafkaBroker broker, [NotNull] KafkaBrokerMetadata metadata, [NotNull] KafkaClientSettings settings)
        {
            _broker = broker;
            _metadata = metadata;
            _topics = new ConcurrentDictionary<string, KafkaClientBrokerTopic>();
            _groups = new ConcurrentDictionary<string, KafkaClientBrokerGroup>();
            _producer = new KafkaProducerBroker(broker, settings.WorkerPeriod);
            _consumer = new KafkaConsumerBroker(broker, settings.WorkerPeriod);
            _coordinator = new KafkaCoordinatorBroker(broker, settings.WorkerPeriod);
            
            var workerPeriod = settings.WorkerPeriod;
            if (workerPeriod < TimeSpan.FromMilliseconds(100)) //todo (E006) settings rage validation?
            {
                workerPeriod = TimeSpan.FromMilliseconds(100);
            }
            _clientTimeout = workerPeriod + TimeSpan.FromSeconds(1) + workerPeriod;
        }

        public void Work()
        {
            _broker.Maintenance();

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                ProcessTopic(topic);                
            }

            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                ProcessGroup(group);
            }

            _coordinator.Process();
            _producer.Produce();
            _consumer.Consume();
        }

        public void Open()
        {
            _broker.Open();
        }

        public void Close()
        {            
            _consumer.Close();
            _producer.Close();
            _coordinator.Close();

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

        private void ProcessTopic([NotNull] KafkaClientBrokerTopic topic)
        {
            foreach (var partitionPair in topic.Partitions)
            {
                var partition = partitionPair.Value;
                if (partition == null) continue;

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
                    if (partition.Producer?.Status == KafkaProducerBrokerPartitionStatus.RearrangeRequired ||
                        partition.Consumer?.Status == KafkaConsumerBrokerPartitionStatus.RearrangeRequired)
                    {
                        partition.Status = KafkaClientBrokerPartitionStatus.RearrangeRequired;
                    }
                }
            }
        }

        private void ProcessGroup([NotNull] KafkaClientBrokerGroup group)
        {                       
            if (group.IsUnplugRequired)
            {
                group.Status = KafkaClientBrokerGroupStatus.Unplugged;
                KafkaClientBrokerGroup removedGroup;
                _groups.TryRemove(group.GroupName, out removedGroup);
                _coordinator.RemoveGroup(group.GroupName);
                return;
            }

            if (group.Status == KafkaClientBrokerGroupStatus.Unplugged)
            {
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

        public KafkaBrokerResult<int?> SendRequest<TRequest>([NotNull] TRequest request) where TRequest: class, IKafkaRequest
        {
            return _broker.Send(request, _clientTimeout);                
        }

        public KafkaBrokerResult<TResponse> GetResponse<TResponse>(int requestId) where TResponse: class, IKafkaResponse
        {
            return _broker.Receive<TResponse>(requestId);            
        }
    }
}