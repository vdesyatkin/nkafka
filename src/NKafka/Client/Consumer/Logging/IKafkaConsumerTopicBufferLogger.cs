using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Logging
{    
    internal interface IKafkaConsumerTopicBufferLogger
    {        
        void OnSerializationError([NotNull] KafkaConsumerTopicSerializationErrorInfo error);
    }
}
