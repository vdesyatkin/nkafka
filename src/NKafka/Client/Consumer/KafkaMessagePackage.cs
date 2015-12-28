using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    public sealed class KafkaMessagePackage
    {
        public readonly int PackageNumber;

        public readonly IReadOnlyList<KafkaMessage> Messages;

        public KafkaMessagePackage(int packageNumber, [NotNull] IReadOnlyList<KafkaMessage> messages)
        {
            Messages = messages;
            PackageNumber = packageNumber;
        }
    }

    public sealed class KafkaMessagePackage<TKey, TData>
    {
        public readonly int PackageNumber;

        public readonly IReadOnlyList<KafkaMessage<TKey,TData>> Messages;

        public KafkaMessagePackage(int packageNumber, [NotNull] IReadOnlyList<KafkaMessage<TKey, TData>> messages)
        {
            Messages = messages;
            PackageNumber = packageNumber;
        }
    }
}
