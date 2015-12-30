using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaMessagePackage
    {
        public readonly int PackageNumber;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaMessage> Messages;

        public KafkaMessagePackage(int packageNumber, [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> messages)
        {
            Messages = messages;
            PackageNumber = packageNumber;
        }
    }

    [PublicAPI]
    public sealed class KafkaMessagePackage<TKey, TData>
    {
        public readonly int PackageNumber;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaMessage<TKey,TData>> Messages;

        public KafkaMessagePackage(int packageNumber, [NotNull, ItemNotNull] IReadOnlyList<KafkaMessage<TKey, TData>> messages)
        {
            Messages = messages;
            PackageNumber = packageNumber;
        }
    }
}
