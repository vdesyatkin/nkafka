using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using NKafka.Client.Broker.Internal;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClient : IKafkaClient
    {
        [NotNull, ItemNotNull] private readonly IReadOnlyList<KafkaClientWorker> _workers;

        public KafkaClientStatus Status { get; private set; }

        [NotNull] private readonly object _stateLocker = new object();

        public KafkaClientInfo GetDiagnosticsInfo()
        {
            var workerInfos = new List<KafkaClientWorkerInfo>(_workers.Count);
            // ReSharper disable once InconsistentlySynchronizedField
            foreach (var worker in _workers)
            {
                var workerInfo = worker.GetDiagnosticsInfo();
                workerInfos.Add(workerInfo);
            }

            return new KafkaClientInfo(workerInfos, DateTime.UtcNow);
        }

        public KafkaClient([NotNull]KafkaClientSettings settings,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaClientGroup> groups,
            [CanBeNull] IKafkaClientLogger logger)
        {
            var workerCount = settings.WorkerThreadCount;
            if (workerCount < 1)
            {
                workerCount = 1;
            }

            var workers = new List<KafkaClientWorker>(workerCount);
            for (var i = 0; i < workerCount; i++)
            {
                var worker = new KafkaClientWorker(i + 1, settings, logger);
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
        
        public void Start()
        {
            if (Status == KafkaClientStatus.Started) return;

            lock (_stateLocker)
            {
                if (Status == KafkaClientStatus.Started) return;

                if (Status == KafkaClientStatus.Paused)
                {
                    ResumeWorkers();
                    return;
                }

                foreach (var worker in _workers)
                {
                    worker.Start();
                }

                Status = KafkaClientStatus.Started;
            }
        }
        
        public void Stop()
        {
            if (Status == KafkaClientStatus.Stopped) return;

            lock (_stateLocker)
            {
                if (Status == KafkaClientStatus.Stopped) return;

                if (Status != KafkaClientStatus.Paused)
                {
                    PauseWorkers();
                    Status = KafkaClientStatus.Paused;
                }

                foreach (var worker in _workers)
                {
                    worker.BeginStop();
                }

                var tasks = new List<Task>(_workers.Count);
                foreach (var worker in _workers)
                {
                    var localWorker = worker;
                    var task = Task.Run(() => localWorker.EndStop());
                    tasks.Add(task);
                }
                Task.WhenAll(tasks.ToArray());

                Status = KafkaClientStatus.Stopped;
            }
        }
        
        public void Pause()
        {
            if (Status == KafkaClientStatus.Stopped) return;

            lock (_stateLocker)
            {
                if (Status == KafkaClientStatus.Stopped) return;

                if (Status != KafkaClientStatus.Paused)
                {
                    PauseWorkers();
                    Status = KafkaClientStatus.Paused;
                }
            }
        }        
       
        public bool PauseAndWaitFlush(TimeSpan flushTimeout)
        {
            if (Status == KafkaClientStatus.Stopped) return true;

            lock (_stateLocker)
            {
                if (Status == KafkaClientStatus.Stopped) return true;

                if (Status != KafkaClientStatus.Paused)
                {
                    PauseWorkers();
                    Status = KafkaClientStatus.Paused;
                }

                var cancellation = new CancellationTokenSource(flushTimeout);
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
            }
        }

        #region Flush

        private void PauseWorkers()
        {
            foreach (var worker in _workers)
            {
                worker.Pause();
            }
        }

        private void ResumeWorkers()
        {
            foreach (var worker in _workers)
            {
                worker.Resume();
            }
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
            var index = Math.Abs(key) % _workers.Count;
            // ReSharper disable once InconsistentlySynchronizedField
            var worker = _workers[index];
            return worker;
        }

        #endregion Arrangement
    }
}