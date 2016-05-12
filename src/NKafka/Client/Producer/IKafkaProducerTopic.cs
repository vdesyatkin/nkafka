using JetBrains.Annotations;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerTopic
    {
        [NotNull] string TopicName { get; }
        void EnqueueMessage([NotNull] KafkaMessage message);
        void EnqueueMessage([NotNull] byte[] key, [NotNull] byte[] data);
        void EnqueueMessage([NotNull] byte[] data);

        bool IsReady { get; }
        bool IsSynchronized { get; }
        [NotNull] KafkaProducerTopicInfo GetDiagnosticsInfo();        
    }

    [PublicAPI]
    public interface IKafkaProducerTopic<TKey, TData>
    {
        [NotNull]
        string TopicName { get; }
        void EnqueueMessage([NotNull] KafkaMessage<TKey, TData> message);
        void EnqueueMessage([NotNull] TKey key, [NotNull] TData data);
        void EnqueueMessage([NotNull] TData data);

        bool IsReady { get; }
        bool IsSynchronized { get; }
        [NotNull] KafkaProducerTopicInfo GetDiagnosticsInfo();
    }
}
