using System.Collections.Generic;

namespace NKafka.Consumer
{
    public interface IKafkaConsumer
    {
        //todo (C001) offest
        //todo (C001) return-value
        void Consume(IReadOnlyList<KafkaMessageAndOffset> messages); 
    }
}
