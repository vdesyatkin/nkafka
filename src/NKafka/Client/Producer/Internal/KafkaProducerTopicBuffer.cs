using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicBuffer : IKafkaProducerTopic, IKafkaProducerTopicBuffer
    {
        [NotNull] private readonly IKafkaProducerPartitioner _partitioner;
        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _messageQueue;
        private int _enqueuedCount;

        public KafkaProducerTopicBuffer([NotNull] IKafkaProducerPartitioner partitioner)
        {
            _partitioner = partitioner;
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
        }

        public void EnqueueMessage([CanBeNull] KafkaMessage message)
        {
            if (message == null) return;
            _messageQueue.Enqueue(message);
            Interlocked.Increment(ref _enqueuedCount);
        }

        public void EnqueueMessage([CanBeNull] byte[] key, [CanBeNull]  byte[] data)
        {
            EnqueueMessage(new KafkaMessage(key, data));
        }

        public void EnqueueMessage([CanBeNull]  byte[] data)
        {
            EnqueueMessage(new KafkaMessage(null, data));
        }
        
        public void Flush(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions)
        {
            if (partitionIds == null || partitions == null) return;
            var enqueuedCount = _enqueuedCount;
            if (partitionIds.Count == 0) return;

            var processedCount = 0;
            KafkaMessage message;
            while (processedCount < enqueuedCount && _messageQueue.TryDequeue(out message))
            {
                int partitionId;
                try
                {
                    partitionId = _partitioner.GetPartition(message.Key, message.Data, partitionIds);
                }
                catch (Exception)
                {
                    partitionId = partitionIds[0];
                }

                if (!partitions.ContainsKey(partitionId))
                {
                    partitionId = partitionIds[0];
                }

                KafkaProducerTopicPartition partition;
                if (!partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    continue;
                }

                partition.EnqueueMessage(message);
                processedCount++;
            }

            Interlocked.Add(ref _enqueuedCount, -processedCount);
        }
    }

    internal sealed class KafkaProducerTopicBuffer<TKey, TData> : IKafkaProducerTopic<TKey, TData>, IKafkaProducerTopicBuffer
    {
        [NotNull] private readonly IKafkaProducerPartitioner<TKey, TData> _partitioner;
        [NotNull] private readonly IKafkaProducerSerializer<TKey, TData> _serializer;
        [NotNull] private readonly ConcurrentQueue<KafkaMessage<TKey, TData>> _messageQueue;
        private int _enqueuedCount;

        public KafkaProducerTopicBuffer([NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner,
            [NotNull] IKafkaProducerSerializer<TKey, TData> serializer)
        {
            _partitioner = partitioner;
            _serializer = serializer;
            _messageQueue = new ConcurrentQueue<KafkaMessage<TKey, TData>>();
        }

        public void Produce([CanBeNull] KafkaMessage<TKey, TData> message)
        {
            if (message == null) return;
            _messageQueue.Enqueue(message);
            Interlocked.Increment(ref _enqueuedCount);
        }

        public void Produce([CanBeNull] TKey key, [CanBeNull] TData data)
        {
            Produce(new KafkaMessage<TKey, TData>(key, data));
        }

        public void Produce([CanBeNull] TData data)
        {
            Produce(new KafkaMessage<TKey, TData>(default(TKey), data));
        }
        
        public void Flush(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions)
        {
            if (partitionIds == null || partitions == null) return;

            var enqueuedCount = _enqueuedCount;            
            if (partitionIds.Count == 0) return;

            var processedCount = 0;
            KafkaMessage<TKey, TData> message;
            while (processedCount < enqueuedCount && _messageQueue.TryDequeue(out message))
            {
                byte[] key;
                byte[] data;
                try
                {
                    key = _serializer.SerializeKey(message.Key);
                    data = _serializer.SerializeData(message.Data);
                }
                catch (Exception)
                {
                    continue;
                }

                int partitionId;
                try
                {
                    partitionId = _partitioner.GetPartition(message.Key, message.Data, partitionIds);
                }
                catch (Exception)
                {
                    partitionId = partitionIds[0];
                }

                if (!partitions.ContainsKey(partitionId))
                {
                    partitionId = partitionIds[0];
                }

                KafkaProducerTopicPartition partition;
                if (!partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    continue;
                }

                partition.EnqueueMessage(new KafkaMessage(key, data));
                processedCount++;
            }

            Interlocked.Add(ref _enqueuedCount, -processedCount);
        }
    }
}
