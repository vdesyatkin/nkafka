using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Producer.Internal;

namespace NKafka.Producer
{
    public class KafkaProducer
    {
        [NotNull]
        private readonly IReadOnlyList<KafkaProducerWorker> _workers;        
        
        public KafkaProducer([NotNull]KafkaProducerSettings settings)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settings = settings ?? new KafkaProducerSettingsBuilder(null).Build();
            
            var workerCount = settings.ProduceThreadCount;
            if (workerCount < 1)
            {
                workerCount = 1;
            }
            
            var workers = new KafkaProducerWorker[workerCount];
            for (var i = 0; i < workerCount; i++)
            {
                var worker = new KafkaProducerWorker(settings);
                worker.ArrangeTopic += OnArrangeTopic;
                workers[i] = worker;
            }
            _workers = workers;            
        }

        [PublicAPI, CanBeNull]
        public IKafkaProducerTopic AddTopic([NotNull] string topicName, [NotNull] IKafkaProducerPartitioner partitioner)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            if (string.IsNullOrEmpty(topicName) || (partitioner == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var worker = GetTopicWorker(topicName);
            var topicBuffer = new KafkaProducerTopicBuffer(partitioner);
            var topic = new KafkaProducerTopic(topicName, topicBuffer);
            worker.AssignTopic(topic);
            return topicBuffer;
        }

        [PublicAPI, CanBeNull]
        public IKafkaProducerTopic<TKey, TData> AddTopic<TKey, TData>([NotNull] string topicName, 
            [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner, [NotNull] IKafkaProducerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            if (string.IsNullOrEmpty(topicName) || (partitioner == null) || (serializer == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse            

            var worker = GetTopicWorker(topicName);
            var topicBuffer = new KafkaProducerTopicBuffer<TKey, TData>(partitioner, serializer);
            var topic = new KafkaProducerTopic(topicName, topicBuffer);
            worker.AssignTopic(topic);
            return topicBuffer;
        }       

        [PublicAPI]
        public void Start()
        {            
            foreach (var worker in _workers)
            {
                worker.Start();
            }
        }

        [PublicAPI]
        public void Stop()
        {            
            foreach (var worker in _workers)
            {
                worker.Stop();
            }
        }  

        private void OnArrangeTopic([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaProducerBrokerPartition> partitions)
        {
            foreach (var partition in partitions)
            {
                var brokerId = partition.BrokerMetadata.BrokerId;
                var worker = GetBrokerWorker(brokerId);
                worker.AssignTopicPartition(topicName, partition);
            }
        }

        [NotNull]
        private KafkaProducerWorker GetBrokerWorker(int brokerId)
        {            
            var index = brokerId % _workers.Count;
            return _workers[index];
        }

        [NotNull]
        private KafkaProducerWorker GetTopicWorker([NotNull] string topicName)
        {            
            var index = topicName.GetHashCode() % _workers.Count;
            return _workers[index];
        }                
    }
}
