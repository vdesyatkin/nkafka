using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Consumer.Internal
{
    internal class KafkaConsumer : IKafkaConsumer
    {
        [NotNull]
        private readonly IReadOnlyList<KafkaConsumerWorker> _workers;

        public KafkaConsumer([NotNull]KafkaConsumerSettings settings, [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerTopic> topics)
        {
            var workerCount = settings.ConsumeThreadCount;
            if (workerCount < 1)
            {
                workerCount = 1;
            }

            var workers = new KafkaConsumerWorker[workerCount];
            for (var i = 0; i < workerCount; i++)
            {
                var worker = new KafkaConsumerWorker(settings);
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

        private void OnArrangeTopic([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaConsumerBrokerPartition> partitions)
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