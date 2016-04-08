using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;
using NKafka.Protocol.API.GroupCoordinator;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.Internal.Broker
{
    internal sealed class KafkaClientBroker
    {
        public bool IsEnabled => _broker.IsEnabled;
        public bool IsOpenned => _broker.IsOpenned;

        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerTopic> _topics;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerGroup> _groups;
        [NotNull] private readonly KafkaProducerBroker _producer;
        [NotNull] private readonly KafkaConsumerBroker _consumer;
        [NotNull] private readonly KafkaCoordinatorBroker _coordinator;

        private readonly TimeSpan _clientTimeout;

        public KafkaClientBroker([NotNull] KafkaBroker broker, [NotNull] KafkaClientSettings settings)
        {
            _broker = broker;
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

        #region Topic metadata

        public KafkaBrokerResult<int?> RequestTopicMetadata([NotNull] string topicName)
        {           
            return _broker.Send(new KafkaTopicMetadataRequest(new[] { topicName }), _clientTimeout);
        }

        public KafkaBrokerResult<KafkaTopicMetadata> GetTopicMetadata(int requestId)
        {
            var response = _broker.Receive<KafkaTopicMetadataResponse>(requestId);
            return ConvertMetadata(response);
        }

        private static KafkaBrokerResult<KafkaTopicMetadata> ConvertMetadata(KafkaBrokerResult<KafkaTopicMetadataResponse> response)
        {
            if (!response.HasData) return response.Error;

            var responseData = response.Data;
            var responseBrokers = responseData?.Brokers ?? new KafkaTopicMetadataResponseBroker[0];
            var responseTopics = responseData?.Topics ?? new KafkaTopicMetadataResponseTopic[0];

            if (responseTopics.Count < 1) return KafkaBrokerErrorCode.DataError;
            var responseTopic = responseTopics[0];
            if (string.IsNullOrEmpty(responseTopic?.TopicName)) return KafkaBrokerErrorCode.DataError;

            //todo (E009) handling standard errors (responseTopic.ErrorCode)
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
                //todo (E009) handling standard errors (responsePartition.ErrorCode)
                if (responsePartition?.ErrorCode != KafkaResponseErrorCode.NoError) continue;
                partitions.Add(new KafkaTopicPartitionMetadata(responsePartition.PartitionId, responsePartition.LeaderId));
            }

            return new KafkaTopicMetadata(responseTopic.TopicName, brokers, partitions);
        }

        #endregion Topic metadata

        #region Group metadata

        public KafkaBrokerResult<int?> RequestGroupCoordinator([NotNull] string groupName)
        {
            return _broker.Send(new KafkaGroupCoordinatorRequest(groupName), _clientTimeout);
        }

        public KafkaBrokerResult<KafkaBrokerMetadata> GetGroupCoordinator(int requestId)
        {
            var response = _broker.Receive<KafkaGroupCoordinatorResponse>(requestId);
            return ConvertGroupCoordinator(response);
        }

        private static KafkaBrokerResult<KafkaBrokerMetadata> ConvertGroupCoordinator(KafkaBrokerResult<KafkaGroupCoordinatorResponse> response)
        {
            if (!response.HasData) return response.Error;

            var responseData = response.Data;            
                       
            if (responseData == null || responseData.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                //todo (E009) handling standard errors
                return KafkaBrokerErrorCode.DataError;
            }

            return new KafkaBrokerMetadata(responseData.BrokerId, responseData.Host, responseData.Port, null);
        }

        #endregion Group metadata

    }
}