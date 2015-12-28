using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitionId;

        [NotNull]
        private readonly IKafkaConsumerMessageQueue _messageQueue;

        public bool NeedRearrange;

        private long _lastEnqueuedOffset;
        private long _lastCommittedOffsetRequired;
        private long _lastCommittedOffset;


        public KafkaConsumerBrokerPartition([NotNull] string topicName, int partitionId, [NotNull] IKafkaConsumerMessageQueue messageQueue)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            _messageQueue = messageQueue;
        }

        public void EnqueueMessages([NotNull, ItemNotNull] IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            try
            {
                _messageQueue.Enqueue(messages);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public void CommitOffset(long offset)
        {
            if (offset > _lastCommittedOffsetRequired)
            {
                Interlocked.CompareExchange(ref _lastCommittedOffsetRequired, offset, _lastCommittedOffsetRequired);
            }
        }
    }
}
