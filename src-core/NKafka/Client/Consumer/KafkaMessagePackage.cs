using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaMessagePackage
    {
        public readonly long PackageId;

        public readonly int PartitionId;

        public readonly long BeginOffset;

        public readonly long EndOffset;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaMessage> Messages;

        public KafkaMessagePackage(long packageId, int partitionId, long beginOffset, long endOffset, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> messages)
        {
            Messages = messages;
            PackageId = packageId;
            PartitionId = partitionId;
            BeginOffset = beginOffset;
            EndOffset = endOffset;
        }
    }

    [PublicAPI]
    public sealed class KafkaMessagePackage<TKey, TData>
    {
        public readonly long PackageId;

        public readonly int PartitionId;

        public readonly long BeginOffset;

        public readonly long EndOffset;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaMessage<TKey,TData>> Messages;

        public KafkaMessagePackage(long packageId, int partitionId, long beginOffset, long endOffset,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage<TKey, TData>> messages)
        {
            Messages = messages;
            PackageId = packageId;
            PartitionId = partitionId;
            BeginOffset = beginOffset;
            EndOffset = endOffset;
        }
    }
}
