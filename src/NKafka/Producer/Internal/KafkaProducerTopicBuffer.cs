using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Producer.Internal
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

        // ReSharper disable once AnnotationRedundancyInHierarchy
        public void Flush([NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartition> partitions)
        {            
            var enqueuedCount = _enqueuedCount;
            var partitionCount = partitions.Count;
            if (partitionCount == 0) return;

            var processedCount = 0;
            KafkaMessage message;
            while (processedCount < enqueuedCount && _messageQueue.TryDequeue(out message))
            {
                int partitionIndex;
                try
                {
                    partitionIndex = _partitioner.GetPartitionIndex(message.Key, message.Data, partitionCount);                    
                }
                catch (Exception)
                {
                    //todo errors
                    partitionIndex = 0;
                }

                if (partitionIndex < 0 || partitionIndex >= partitionCount)
                {
                    partitionIndex = 0;
                }

                var partition = partitions[partitionIndex];
                partition.EnqueueMessage(message);

                processedCount++;
            }

            Interlocked.Add(ref _enqueuedCount, -processedCount);
        }
    }

    internal sealed class KafkaProducerTopicBuffer<TKey, TData> : IKafkaProducerTopic<TKey, TData>, IKafkaProducerTopicBuffer
    {
        [NotNull]
        private readonly IKafkaProducerPartitioner<TKey, TData> _partitioner;
        [NotNull]
        private readonly IKafkaProducerSerializer<TKey, TData> _serializer;
        [NotNull]
        private readonly ConcurrentQueue<KafkaMessage<TKey, TData>> _messageQueue;
        private int _enqueuedCount;

        public KafkaProducerTopicBuffer([NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner, 
            [NotNull] IKafkaProducerSerializer<TKey, TData> serializer)
        {
            _partitioner = partitioner;
            _serializer = serializer;
            _messageQueue = new ConcurrentQueue<KafkaMessage<TKey, TData>>();
        }

        public void EnqueueMessage([CanBeNull] KafkaMessage<TKey, TData> message)
        {
            if (message == null) return;
            _messageQueue.Enqueue(message);
            Interlocked.Increment(ref _enqueuedCount);
        }

        public void EnqueueMessage([CanBeNull] TKey key, [CanBeNull] TData data)
        {
            EnqueueMessage(new KafkaMessage<TKey, TData>(key, data));
        }

        public void EnqueueMessage([CanBeNull] TData data)
        {
            EnqueueMessage(new KafkaMessage<TKey, TData>(default(TKey), data));
        }

        // ReSharper disable once AnnotationRedundancyInHierarchy
        public void Flush([NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartition> partitions)
        {            
            var enqueuedCount = _enqueuedCount;
            var partitionCount = partitions.Count;
            if (partitionCount == 0) return;

            var processedCount = 0;
            KafkaMessage<TKey, TData> message;
            while (processedCount < enqueuedCount && _messageQueue.TryDequeue(out message))
            {
                byte[] key;
                byte[] data;
                try
                {
                    key = _serializer.SerializeKey(message.Key);
                    data = _serializer.SerializeValue(message.Data);
                }
                catch (Exception)
                {
                    //todo errors
                    continue;
                }

                int partitionIndex;
                try
                {
                    partitionIndex = _partitioner.GetPartitionIndex(message.Key, message.Data, partitionCount);
                }
                catch (Exception)
                {
                    //todo errors
                    partitionIndex = 0;
                }

                if (partitionIndex < 0 || partitionIndex >= partitionCount)
                {
                    partitionIndex = 0;
                }

                var partition = partitions[partitionIndex];
                partition.EnqueueMessage(new KafkaMessage(key, data));

                processedCount++;
            }

            Interlocked.Add(ref _enqueuedCount, -processedCount);
        }
    }
}
