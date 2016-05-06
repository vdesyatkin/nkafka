using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Client.Internal.Broker;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClient : IKafkaClient
    {
        [NotNull, ItemNotNull]
        private readonly IReadOnlyList<KafkaClientWorker> _workers;

        public KafkaClientInfo GetDiagnosticsInfo()
        {
            var workerInfos = new List<KafkaClientWorkerInfo>(_workers.Count);
            foreach (var worker in _workers)
            {
                var workerInfo = worker.GetDiagnosticsInfo();
                workerInfos.Add(workerInfo);
            }

            return new KafkaClientInfo(workerInfos, DateTime.UtcNow);
        }

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
                var worker = new KafkaClientWorker(i + 1, settings);
                worker.ArrangeTopic += OnArrangeTopic;
                worker.ArrangeGroup += OnArrangeGroupCoordinator;
                workers.Add(worker);
            }
            _workers = workers;

            foreach (var topic in topics)
            {
                var worker = GetWorker(topic.TopicName.GetHashCode());

                worker?.AssignTopic(topic);
            }

            foreach (var group in groups)
            {
                var worker = GetWorker(group.GroupName.GetHashCode());                

                worker?.AssignGroup(group);
            }            
        }        

        /// <summary>
        /// Non thread-safe.
        /// </summary>
        public void Start()
        {
            foreach (var worker in _workers)
            {
                worker.Start();
            }
        }

        /// <summary>
        /// Non thread-safe.
        /// </summary>
        public void Stop()
        {
            foreach (var worker in _workers)
            {
                worker.Stop();
            }
        }
        
        /// <summary>
        /// Non thread-safe.
        /// </summary>        
        async public Task<bool> TryFlushAndStop(TimeSpan flushTimeout)
        {
            DisableConsume();            
            var isFlushed = await TryFlushInternal(flushTimeout);
            if (!isFlushed)
            {
                EnableConsume(); //todo (E011) ???
                return false;
            }

            Stop();
            return true;
        }        

        #region Flush

        private void DisableConsume()
        {
            foreach (var worker in _workers)
            {
                worker.DisableConsume();
            }
        }

        private void EnableConsume()
        {
            foreach (var worker in _workers)
            {
                worker.EnableConsume();
            }
        }

        [NotNull]
        async public Task<bool> TryFlushInternal(TimeSpan timeout)
        {
            var cancellation = new CancellationTokenSource(timeout);

            // ReSharper disable once MethodSupportsCancellation
            var flushTask = Task.Run(() =>
            {
                var spinWait = new SpinWait();

                while (!cancellation.IsCancellationRequested)
                {
                    var isSynchronized = true;
                    foreach (var worker in _workers)
                    {
                        isSynchronized = isSynchronized && worker.IsAllTopicsSynchronized();
                    }
                    if (isSynchronized)
                    {
                        return true;
                    }
                    spinWait.SpinOnce();
                }

                return false;
            });

            var isFlushed = flushTask != null && await flushTask;
            return isFlushed;
        }

        #endregion Flush

        #region Arrangement

        private void OnArrangeTopic([NotNull] string topicName, [NotNull, ItemNotNull] IReadOnlyCollection<KafkaClientBrokerPartition> partitions)
        {
            foreach (var partition in partitions)
            {
                var brokerId = partition.BrokerMetadata.BrokerId;
                var worker = GetWorker(brokerId);

                worker?.AssignTopicPartition(topicName, partition);
            }
        }

        private void OnArrangeGroupCoordinator([NotNull] string groupName, [NotNull] KafkaClientBrokerGroup groupCoordinator)
        {
            var brokerId = groupCoordinator.BrokerMetadata.BrokerId;
            var worker = GetWorker(brokerId);

            worker?.AssignGroupCoordinator(groupName, groupCoordinator);
        }

        private KafkaClientWorker GetWorker(int key)
        {
            var index = key % _workers.Count;
            var worker = _workers[index];
            return worker;
        }

        #endregion Arrangement
    }
}
