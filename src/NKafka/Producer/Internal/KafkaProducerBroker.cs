using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Metadata;
using NKafka.Protocol;
using NKafka.Protocol.API.Produce;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Producer.Internal
{
    //todo refactoring
    internal sealed class KafkaProducerBroker
    {
        [NotNull]
        public readonly ConcurrentDictionary<string, KafkaProducerBrokerTopic> Topics;        

        public bool IsEnabled => _broker.IsOpenned && _broker.Error == null;
        public bool IsOpenned => _broker.IsOpenned;

        [NotNull] private readonly KafkaBroker _broker;

        private readonly int _batchByteCountLimit;
        private readonly int _batchMessageCountLimit;
        private readonly KafkaConsistencyLevel _consistencyLevel;
        private readonly KafkaCodecType _codecType;
        private readonly TimeSpan _produceTimeout;

        [NotNull] private readonly ConcurrentDictionary<int, KafkaProduceRequest> _produceRequests;


        private int _produceOffset;

        public KafkaProducerBroker([NotNull] KafkaBroker broker, [NotNull] KafkaProducerSettings settings)
        {
            _broker = broker;
            Topics = new ConcurrentDictionary<string, KafkaProducerBrokerTopic>();
            _batchByteCountLimit = settings.BatchByteCountLimit;
            _batchMessageCountLimit = settings.BatchMessageCountLimit;
            _consistencyLevel = settings.ConsistencyLevel;
            _codecType = settings.CodecType;
            _produceTimeout = settings.ProduceTimeout;
            if (_produceTimeout < TimeSpan.FromSeconds(1))
            {
                _produceTimeout = TimeSpan.FromSeconds(1); //todo range?
            }
            
            _produceRequests = new ConcurrentDictionary<int, KafkaProduceRequest>();

            //todo validate range
        }

        public void Maintenance()
        {
            _broker.Maintenance();

            foreach (var topicPair in Topics)
            {
                var topic = topicPair.Value;
                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition.IsUnplugRequired)
                    {
                        partition.Status = KafkaProducerBrokerPartitionStatus.Unplugged;
                        topic.Partitions.TryRemove(partitionPair.Key, out partition);
                        continue;
                    }
                    
                    if (partition.Status == KafkaProducerBrokerPartitionStatus.Unplugged)
                    {
                        partition.Status = KafkaProducerBrokerPartitionStatus.Plugged;
                    }                    
                }
            }

            foreach (var produceRequestPair in _produceRequests)
            {
                var response = _broker.Receive<KafkaProduceResponse>(produceRequestPair.Key);
                if (response == null) continue;

                KafkaProduceRequest request;
                _produceRequests.TryRemove(produceRequestPair.Key, out request);

                var topics = response.Topics;
                if (topics == null) continue;

                foreach (var topicResponse in response.Topics)
                {
                    if (topicResponse == null) continue;
                    var topicName = topicResponse.TopicName;
                    if (string.IsNullOrEmpty(topicName)) continue;
                    var partitions = topicResponse.Partitions;
                    if (partitions == null) continue;

                    KafkaProducerBrokerTopic topic;
                    if (!Topics.TryGetValue(topicName, out topic)) continue;

                    foreach (var partitionResponse in partitions)
                    {
                        if (partitionResponse == null) continue;
                        var partitionId = partitionResponse.PartitionId;
                        KafkaProducerBrokerPartition partition;
                        if (!topic.Partitions.TryGetValue(partitionId, out partition)) continue;

                        var error = partitionResponse.ErrorCode;                        

                        if (error != KafkaResponseErrorCode.NoError)
                        {
                            //todo failure scenario - rollback request data to partition
                            //todo errors

                            if (error == KafkaResponseErrorCode.NotLeaderForPartition)
                            {
                                partition.Status = KafkaProducerBrokerPartitionStatus.NeedRearrange;
                            }
                        }
                    }
                }
            }
        }

        public void Open()
        {
            _broker.Open();
        }

        public void Close()
        {            
            foreach (var topic in Topics)
            {
                foreach (var partition in topic.Value.Partitions)
                {
                    partition.Value.Status = KafkaProducerBrokerPartitionStatus.Unplugged;
                }
                topic.Value.Partitions.Clear();
            }
            _broker.Close();
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaProducerBrokerPartition topicPartition)
        {
            KafkaProducerBrokerTopic topic;
            if (!Topics.TryGetValue(topicName, out topic))
            {
                topic = Topics.AddOrUpdate(topicName, new KafkaProducerBrokerTopic(topicName), (oldKey, oldValue) => oldValue);
            }

            topic.Partitions[topicPartition.PartitionId] = topicPartition;
        }

        public int? RequestTopicMetadta(string topicName)
        {
            return _broker.Send(new KafkaTopicMetadataRequest(new[] { topicName }));
        }

        [CanBeNull]
        public KafkaTopicMetadata GetTopicMetadata(int requestId)
        {
            return ConvertMetadata(_broker.Receive<KafkaTopicMetadataResponse>(requestId));
        }

        [CanBeNull]
        private KafkaTopicMetadata ConvertMetadata([CanBeNull] KafkaTopicMetadataResponse response)
        {
            if (response == null) return null;
            var responseBrokers = response.Brokers ?? new KafkaTopicMetadataResponseBroker[0];
            var responseTopics = response.Topics ?? new KafkaTopicMetadataResponseTopic[0];

            if (responseTopics.Count < 1) return null;
            var responseTopic = responseTopics[0];
            if (string.IsNullOrEmpty(responseTopic?.TopicName)) return null;

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
                partitions.Add(new KafkaTopicPartitionMetadata(responsePartition.PartitionId, responsePartition.LeaderId));
            }

            return new KafkaTopicMetadata(responseTopic.TopicName, brokers, partitions);
        }

        public void PerformProduce()
        {
            var produceOffset = _produceOffset; 
            
            var partitionList = new List<KafkaProducerBrokerPartition>(100);
            if (produceOffset >= partitionList.Count)
            {
                produceOffset = 0;
            }

            foreach (var topicPair in Topics)
            {
                foreach (var partitionPair in topicPair.Value.Partitions)
                {
                    partitionList.Add(partitionPair.Value);
                }
            }

            bool isBatchFilled;

            do
            {
                isBatchFilled = false;
                var batchTopics = new Dictionary<string, Dictionary<int, List<KafkaMessage>>>();
                var batchByteCount = 0;
                var batchMessageCount = 0;

                for (var i = 0; i < partitionList.Count; i++)
                {
                    var index = i + produceOffset;
                    if (index >= partitionList.Count)
                    {
                        index -= partitionList.Count;
                    }

                    var partition = partitionList[index];
                    if (partition.IsUnplugRequired || partition.Status != KafkaProducerBrokerPartitionStatus.Plugged)
                    {
                        continue;
                    }

                    KafkaMessage message;
                    while (partition.Queue.TryDequeueMessage(out message))
                    {
                        batchMessageCount++;
                        if (message.Key != null)
                        {
                            batchByteCount += message.Key.Length;
                        }
                        if (message.Data != null)
                        {
                            batchByteCount += message.Data.Length;
                        }

                        Dictionary<int, List<KafkaMessage>> topicPartitions;
                        if (!batchTopics.TryGetValue(partition.TopicName, out topicPartitions))
                        {
                            topicPartitions = new Dictionary<int, List<KafkaMessage>>();
                            batchTopics[partition.TopicName] = topicPartitions;
                        }

                        List<KafkaMessage> topicPartionMessages;
                        if (!topicPartitions.TryGetValue(partition.PartitionId, out topicPartionMessages))
                        {
                            topicPartionMessages = new List<KafkaMessage>(_batchMessageCountLimit);
                            topicPartitions[partition.PartitionId] = topicPartionMessages;
                        }
                        topicPartionMessages.Add(message);

                        if (batchByteCount >= _batchByteCountLimit || batchMessageCount >= _batchMessageCountLimit)
                        {
                            isBatchFilled = true;
                            break;
                        }
                    }

                    if (isBatchFilled)
                    {
                        produceOffset = index + 1;
                        break;
                    }                    
                }

                var requestTopics = new List<KafkaProduceRequestTopic>(batchTopics.Count);
                foreach (var batchTopic in batchTopics)
                {
                    var topicName = batchTopic.Key;
                    var batchTopicPartitons = batchTopic.Value;
                    var requestPartitions = new List<KafkaProduceRequestTopicPartition>(batchTopicPartitons.Count);
                    foreach (var batchPartiton in batchTopicPartitons)
                    {
                        var partitonId = batchPartiton.Key;
                        var messages = batchPartiton.Value;
                        var requestPartiton =new KafkaProduceRequestTopicPartition(partitonId, _codecType, messages);
                        requestPartitions.Add(requestPartiton);
                    }
                    var requestTopic = new KafkaProduceRequestTopic(topicName, requestPartitions);
                    requestTopics.Add(requestTopic);
                }
                var batchRequest = new KafkaProduceRequest(_consistencyLevel, _produceTimeout, requestTopics);                

                var batchRequestId = _broker.Send(batchRequest, batchByteCount*2);
                if (batchRequestId == null)
                {
                    //todo failure scenario     
                    continue;               
                }

                if (_consistencyLevel != KafkaConsistencyLevel.None)
                {
                    _produceRequests[batchRequestId.Value] = batchRequest;
                }

            } while (isBatchFilled);            

            _produceOffset = produceOffset;
        }        
    }
}
