using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public interface IKafkaConsumerTopic
    {
        [NotNull] string TopicName { get; }
        bool IsReady { get; }
        bool IsSynchronized { get; }

        [NotNull, ItemNotNull] IReadOnlyList<KafkaMessagePackage> Consume(int? maxMessageCount = null);
        bool TryEnqueueCommit(long packageId);

        [NotNull] KafkaConsumerTopicInfo GetDiagnosticsInfo();
    }

    [PublicAPI]
    public interface IKafkaConsumerTopic<TKey, TData>
    {
        [NotNull] string TopicName { get; }
        bool IsReady { get; }
        bool IsSynchronized { get; }

        [NotNull, ItemNotNull] IReadOnlyList<KafkaMessagePackage<TKey, TData>> Consume(int? maxMessageCount = null);
        bool TryEnqueueCommit(long packageNumber);
        [NotNull] KafkaConsumerTopicInfo GetDiagnosticsInfo();
    }
}
