using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Producer.Internal;

namespace NKafka.Producer
{
    internal class KafkaProducer : IKafkaProducer
    {
        [NotNull]
        private readonly IReadOnlyList<KafkaProducerWorker> _workers;        
        
        public KafkaProducer([NotNull]KafkaProducerSettings settings, [NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopic> topics)
        {            
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

            foreach (var topic in topics)
            {
                var index = topic.TopicName.GetHashCode() % workers.Length;
                var worker = workers[index];
                worker.AssignTopic(topic);
            }

            _workers = workers;            
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
                var index = brokerId % _workers.Count;
                var worker = _workers[index];
                worker.AssignTopicPartition(topicName, partition);
            }
        }        
    }
}
