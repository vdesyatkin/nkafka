using System.Collections.Generic;

namespace NKafka.Client.Consumer
{
    public interface IKafkaConsumerTopic
    {
        //todo (C001) return-value
        void Consume(IReadOnlyList<KafkaMessageAndOffset> messages);
    }

    public interface IKafkaConsumerTopic<TKey, TData>
    {
        //todo (C001) return-value
        void Consume(IReadOnlyList<KafkaMessageAndOffset<TKey, TData>> messages);
    }
}
