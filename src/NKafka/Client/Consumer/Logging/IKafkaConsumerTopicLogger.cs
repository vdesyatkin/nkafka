using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Logging
{    
    internal interface IKafkaConsumerTopicLogger
    {
        void OnTransportError([NotNull] KafkaConsumerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] KafkaConsumerTopicProtocolErrorInfo error);
    }
}
