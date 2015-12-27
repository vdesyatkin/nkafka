using System.Collections.Generic;

namespace NKafka.Client.Consumer
{
    public interface IKafkaConsumerTopic
    {        
        void Consume(IReadOnlyList<KafkaMessageAndOffset> messages);
    }

    public interface IKafkaConsumerTopic<TKey, TData>
    {     
        void Consume(IReadOnlyList<KafkaMessageAndOffset<TKey, TData>> messages);
    }
}
