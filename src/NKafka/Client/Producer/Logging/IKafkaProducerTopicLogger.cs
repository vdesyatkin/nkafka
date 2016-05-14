using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    internal interface IKafkaProducerTopicLogger
    {        
        void OnTransportError([NotNull] KafkaProducerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnErrorReset();
    }
}
