using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientBroker
    {
        public bool IsEnabled => _broker.IsEnabled;
        public bool IsOpenned => _broker.IsOpenned;

        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaClientBrokerTopic> _topics;
        [CanBeNull] private readonly KafkaProducerBroker _producer;
        [CanBeNull] private readonly KafkaConsumerBroker _consumer;

        private readonly TimeSpan _metadataClientTimeout;

        public KafkaClientBroker([NotNull] KafkaBroker broker, [NotNull] KafkaClientSettings settings, 
            bool hasProducer, bool hasConsumer)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaClientBrokerTopic>();
            _producer = hasProducer ? new KafkaProducerBroker(broker, settings.WorkerPeriod, settings.ProducerSettings) : null;
            _consumer = hasConsumer ? new KafkaConsumerBroker(broker, settings.WorkerPeriod, settings.ConsumerSettings) : null;

            var metadataServerTimeout = settings.ProducerSettings.ProduceServerTimeout; //todo (E006) separated property?
            if (metadataServerTimeout < TimeSpan.Zero) //todo (E006) settings rage validation?
            {
                metadataServerTimeout = TimeSpan.Zero;
            }
            var workerPeriod = settings.WorkerPeriod;
            if (workerPeriod < TimeSpan.Zero) //todo (E006) settings rage validation?
            {
                workerPeriod = TimeSpan.Zero;                
            }
            _metadataClientTimeout = metadataServerTimeout +
                                    TimeSpan.FromMilliseconds(workerPeriod.TotalMilliseconds * 2) +
                                    TimeSpan.FromSeconds(1);
        }

        public void Work()
        {
            _broker.Maintenance();

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition.IsUnplugRequired)
                    {
                        partition.Status = KafkaClientBrokerPartitionStatus.Unplugged;
                        topic.Partitions.TryRemove(partitionPair.Key, out partition);
                        _producer?.RemoveTopicPartition(topic.TopicName, partition.PartitionId);
                        _consumer?.RemoveTopicPartition(topic.TopicName, partition.PartitionId);
                        continue;
                    }

                    if (partition.Status == KafkaClientBrokerPartitionStatus.Unplugged)
                    {
                        if (partition.Producer != null)
                        {
                            _producer?.AddTopicPartition(partition.TopicName, partition.Producer);
                        }

                        if (partition.Consumer != null)
                        {
                            _consumer?.AddTopicPartition(partition.TopicName, partition.Consumer);
                        }
                        
                        partition.Status = KafkaClientBrokerPartitionStatus.Plugged;            
                    }                   

                    if (partition.Producer?.NeedRearrange == true || 
                        partition.Consumer?.Status == KafkaConsumerBrokerPartitionStatus.NeedRearrage)
                    {
                        partition.Status = KafkaClientBrokerPartitionStatus.NeedRearrange;
                    }
                }
            }
            
            _producer?.Produce();
            _consumer?.Consume();            
        }

        public void Open()
        {
            _broker.Open();
        }

        public void Close()
        {
            foreach (var topic in _topics)
            {
                foreach (var partition in topic.Value.Partitions)
                {
                    partition.Value.Status = KafkaClientBrokerPartitionStatus.Unplugged;
                }
                topic.Value.Partitions.Clear();
            }
            _broker.Close();
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaClientBrokerPartition topicPartition)
        {
            KafkaClientBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaClientBrokerTopic(topicName), (oldKey, oldValue) => oldValue);
            }

            topic.Partitions[topicPartition.PartitionId] = topicPartition;            
        }

        #region Topic metadata

        public KafkaBrokerResult<int?> RequestTopicMetadata([NotNull] string topicName)
        {           
            return _broker.Send(new KafkaTopicMetadataRequest(new[] { topicName }), _metadataClientTimeout);
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
            var responseBrokers = responseData.Brokers ?? new KafkaTopicMetadataResponseBroker[0];
            var responseTopics = responseData.Topics ?? new KafkaTopicMetadataResponseTopic[0];

            if (responseTopics.Count < 1) return KafkaBrokerErrorCode.DataError;
            var responseTopic = responseTopics[0];
            if (string.IsNullOrEmpty(responseTopic?.TopicName)) return KafkaBrokerErrorCode.DataError;

            //todo (E009) handling standard errors (responseTopic.ErrorCode)
            var responsePartitons = responseTopic.Partitions ?? new KafkaTopicMetadataResponseTopicPartition[0];

            var brokers = new List<KafkaBrokerMetadata>(responseBrokers.Count);
            foreach (var responseBroker in responseBrokers)
            {
                if (responseBroker == null) continue;
                brokers.Add(new KafkaBrokerMetadata(responseBroker.BrokerId, responseBroker.Host, responseBroker.Port));
            }

            var partitions = new List<KafkaTopicPartitionMetadata>(responsePartitons.Count);
            foreach (var responsePartition in responsePartitons)
            {
                if (responsePartition == null) continue;
                //todo (E009) handling standard errors (responsePartition.ErrorCode)
                partitions.Add(new KafkaTopicPartitionMetadata(responsePartition.PartitionId, responsePartition.LeaderId));
            }

            return new KafkaTopicMetadata(responseTopic.TopicName, brokers, partitions);
        }

        #endregion Topic metadata
        
    }
}
