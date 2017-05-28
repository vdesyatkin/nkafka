using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicBuffer : IKafkaProducerTopicBuffer
    {
        public string TopicName { get; }
        public int EnqueuedCount => _enqueuedCount;
        public DateTime? EnqueueTimestampUtc => _enqueueTimestampUtc;

        public IKafkaProducerFallbackHandler FallbackHandler { get; }

        [NotNull] private readonly IKafkaProducerPartitioner _partitioner;
        [CanBeNull] private readonly IKafkaProducerTopicBufferLogger _logger;
        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _messageQueue;

        private int _enqueuedCount;
        private DateTime? _enqueueTimestampUtc;

        public KafkaProducerTopicBuffer([NotNull] string topicName,
            [NotNull] IKafkaProducerPartitioner partitioner,
            [CanBeNull] IKafkaProducerFallbackHandler fallbackHandler,
            [CanBeNull] IKafkaProducerTopicBufferLogger logger)
        {
            TopicName = topicName;
            _partitioner = partitioner;
            _logger = logger;
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
            FallbackHandler = fallbackHandler;
        }

        public void EnqueueMessage([CanBeNull] KafkaMessage message)
        {
            if (message == null) return;
            _messageQueue.Enqueue(message);

            Interlocked.Increment(ref _enqueuedCount);
            _enqueueTimestampUtc = DateTime.UtcNow;
        }

        public int DistributeMessagesByPartitions(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions)
        {
            if (partitionIds == null || partitions == null) return 0;
            var enqueuedCount = _enqueuedCount;
            if (partitionIds.Count == 0) return 0;

            var processedCount = 0;
            KafkaMessage message;
            while (processedCount < enqueuedCount && _messageQueue.TryDequeue(out message))
            {
                processedCount++;

                if (message == null)
                {
                    continue;
                }

                int partitionId;
                try
                {
                    partitionId = _partitioner.GetPartition(message, partitionIds);
                }
                catch (Exception exception)
                {
                    Fallback(message, null, KafkaProducerFallbackErrorCode.PartitioningError);
                    partitionId = partitionIds[0];

                    var logger = _logger;
                    if (logger != null)
                    {
                        var errorInfo = new KafkaProducerTopicPartitioningErrorInfo(message, partitionIds, exception);
                        logger.OnPartitioningError(errorInfo);
                    }
                }

                if (!partitions.ContainsKey(partitionId))
                {
                    partitionId = partitionIds[0];
                }

                KafkaProducerTopicPartition partition;
                if (!partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    Fallback(message, null, KafkaProducerFallbackErrorCode.PartitionNotFound);
                    continue;
                }

                partition.BrokerPartition.EnqueueMessage(message);
            }

            Interlocked.Add(ref _enqueuedCount, -processedCount);
            return processedCount;
        }

        private void Fallback(KafkaMessage message, int? partitionId, KafkaProducerFallbackErrorCode reason)
        {
            try
            {
                var fallbackInfo = new KafkaProducerFallbackInfo(TopicName, partitionId, message, reason);
                FallbackHandler?.OnMessageFallback(fallbackInfo);
            }
            catch (Exception)
            {
                //ignored
            }
        }
    }

    internal sealed class KafkaProducerTopicBuffer<TKey, TData> : IKafkaProducerTopicBuffer
    {
        public string TopicName { get; }
        public int EnqueuedCount => _enqueuedCount;
        public DateTime? EnqueueTimestampUtc => _enqueueTimestampUtc;
        public IKafkaProducerFallbackHandler FallbackHandler { get; }
        [CanBeNull]
        private readonly IKafkaProducerFallbackHandler<TKey, TData> _fallbackHandler;

        [NotNull]
        private readonly IKafkaProducerPartitioner<TKey, TData> _partitioner;
        [CanBeNull]
        private readonly IKafkaSerializer<TKey, TData> _serializer;
        [CanBeNull]
        private readonly IKafkaProducerTopicBufferLogger<TKey, TData> _logger;
        [NotNull]
        private readonly ConcurrentQueue<KafkaMessage<TKey, TData>> _messageQueue;

        private int _enqueuedCount;
        private DateTime? _enqueueTimestampUtc;

        public KafkaProducerTopicBuffer([NotNull] string topicName,
            [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner,
            [NotNull] IKafkaSerializer<TKey, TData> serializer,
            [CanBeNull] IKafkaProducerFallbackHandler<TKey, TData> fallbackHandler,
            [CanBeNull] IKafkaProducerTopicBufferLogger<TKey, TData> logger)
        {
            TopicName = topicName;
            _partitioner = partitioner;
            _serializer = serializer;
            _logger = logger;
            _fallbackHandler = fallbackHandler;
            FallbackHandler = fallbackHandler != null ? new FallbackAdapter(fallbackHandler, serializer) : null;
            _messageQueue = new ConcurrentQueue<KafkaMessage<TKey, TData>>();
        }

        public void EnqueueMessage([CanBeNull] KafkaMessage<TKey, TData> message)
        {
            if (message == null) return;
            _messageQueue.Enqueue(message);
            Interlocked.Increment(ref _enqueuedCount);
            _enqueueTimestampUtc = DateTime.UtcNow;
        }

        public int DistributeMessagesByPartitions(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions)
        {
            if (partitionIds == null || partitions == null) return 0;

            var enqueuedCount = _enqueuedCount;
            if (partitionIds.Count == 0) return 0;

            var processedCount = 0;
            KafkaMessage<TKey, TData> message;
            while (processedCount < enqueuedCount && _messageQueue.TryDequeue(out message))
            {
                processedCount++;

                if (message == null)
                {
                    continue;
                }

                KafkaMessage serializedMessage;
                try
                {
                    serializedMessage = _serializer?.SerializeMessage(message);
                    if (serializedMessage == null)
                    {
                        continue;
                    }
                }
                catch (Exception exception)
                {
                    Fallback(message, null, KafkaProducerFallbackErrorCode.SerializationError);
                    var logger = _logger;
                    if (logger != null)
                    {
                        var errorInfo = new KafkaProducerTopicSerializationErrorInfo<TKey, TData>(message, exception);
                        logger.OnSerializationError(errorInfo);
                    }
                    continue;
                }

                int partitionId;
                try
                {
                    partitionId = _partitioner.GetPartition(message, partitionIds);
                }
                catch (Exception exception)
                {
                    Fallback(message, null, KafkaProducerFallbackErrorCode.PartitioningError);
                    partitionId = partitionIds[0];


                    var logger = _logger;
                    if (logger != null)
                    {
                        var errorInfo = new KafkaProducerTopicPartitioningErrorInfo<TKey, TData>(message, partitionIds, exception);
                        logger.OnPartitioningError(errorInfo);
                    }
                }

                if (!partitions.ContainsKey(partitionId))
                {
                    partitionId = partitionIds[0];
                }

                KafkaProducerTopicPartition partition;
                if (!partitions.TryGetValue(partitionId, out partition) || partition == null)
                {
                    Fallback(message, partitionId, KafkaProducerFallbackErrorCode.PartitionNotFound);
                    continue;
                }

                partition.BrokerPartition.EnqueueMessage(serializedMessage);
            }

            Interlocked.Add(ref _enqueuedCount, -processedCount);
            return processedCount;
        }

        private void Fallback(KafkaMessage<TKey, TData> message, int? partitionId, KafkaProducerFallbackErrorCode reason)
        {
            try
            {
                var fallbackInfo = new KafkaProducerFallbackInfo<TKey, TData>(TopicName, partitionId, message, reason);
                _fallbackHandler?.OnMessageFallback(fallbackInfo);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        private class FallbackAdapter : IKafkaProducerFallbackHandler
        {
            [NotNull]
            private readonly IKafkaProducerFallbackHandler<TKey, TData> _fallbackHandler;
            [NotNull]
            private readonly IKafkaSerializer<TKey, TData> _serializer;

            public FallbackAdapter([NotNull] IKafkaProducerFallbackHandler<TKey, TData> fallbackHandler,
                [NotNull] IKafkaSerializer<TKey, TData> serializer)
            {
                _fallbackHandler = fallbackHandler;
                _serializer = serializer;
            }

            public void OnMessageFallback(KafkaProducerFallbackInfo fallbackInfo)
            {
                try
                {
                    var message = fallbackInfo.Message;
                    var deserializedMessage = _serializer.DeserializeMessage(message);
                    if (deserializedMessage == null) return;

                    var genericFallbackInfo = new KafkaProducerFallbackInfo<TKey, TData>(fallbackInfo.TopicName,
                        fallbackInfo.PartitionId, deserializedMessage, fallbackInfo.Reason);
                    _fallbackHandler.OnMessageFallback(genericFallbackInfo);
                }
                catch (Exception)
                {
                    //ignored
                }
            }
        }
    }
}