using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaMessagePackage
    {
        public readonly long PackageId;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaMessage> Messages;

        public KafkaMessagePackage(long packageId, [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> messages)
        {
            Messages = messages;
            PackageId = packageId;
        }
    }

    [PublicAPI]
    public sealed class KafkaMessagePackage<TKey, TData>
    {
        public readonly long PackageId;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaMessage<TKey,TData>> Messages;

        public KafkaMessagePackage(long packageId, [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage<TKey, TData>> messages)
        {
            Messages = messages;
            PackageId = packageId;
        }
    }
}
