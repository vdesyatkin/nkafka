﻿using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal.Broker;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClient : IKafkaClient
    {
        [NotNull, ItemNotNull]
        private readonly IReadOnlyList<KafkaClientWorker> _workers;

        public KafkaClient([NotNull]KafkaClientSettings settings, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaClientGroup> groups)
        {
            var workerCount = settings.WorkerThreadCount;
            if (workerCount < 1)
            {
                workerCount = 1;
            }
            
            var workers = new List<KafkaClientWorker>(workerCount);
            for (var i = 0; i < workerCount; i++)
            {
                var worker = new KafkaClientWorker(settings);
                worker.ArrangeTopic += OnArrangeTopic;
                worker.ArrangeGroup += OnArrangeGroupCoordinator;
                workers.Add(worker);
            }
            _workers = workers;

            foreach (var topic in topics)
            {
                var worker = GetWorker(topic.TopicName.GetHashCode());
                if (worker == null) continue;

                worker.AssignTopic(topic);
            }

            foreach (var group in groups)
            {
                var worker = GetWorker(group.GroupName.GetHashCode());
                if (worker == null) continue;

                worker.AssignGroup(group);
            }            
        }        

        public void Start()
        {
            foreach (var worker in _workers)
            {

                worker.Start();
            }
        }

        public void Stop()
        {
            foreach (var worker in _workers)
            {
                worker.Stop();
            }
        }

        private void OnArrangeTopic([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaClientBrokerPartition> partitions)
        {
            foreach (var partition in partitions)
            {
                var brokerId = partition.BrokerMetadata.BrokerId;
                var worker = GetWorker(brokerId);
                if (worker == null) continue;

                worker.AssignTopicPartition(topicName, partition);
            }
        }

        private void OnArrangeGroupCoordinator([NotNull] string groupName, [NotNull] KafkaClientBrokerGroup groupCoordinator)
        {
            var brokerId = groupCoordinator.BrokerMetadata.BrokerId;
            var worker = GetWorker(brokerId);
            if (worker == null) return;

            worker.AssignGroupCoordinator(groupName, groupCoordinator);
        }

        private KafkaClientWorker GetWorker(int key)
        {
            var index = key % _workers.Count;
            var worker = _workers[index];
            return worker;
        }
    }
}
