using System;
using JetBrains.Annotations;
using NKafka.Connection;

namespace NKafka.Client.Broker.Diagnostics
{
    [PublicAPI]
    public class KafkaClientBrokerErrorInfo
    {
        [NotNull] public readonly IKafkaClientBroker Broker;

        public readonly KafkaClientBrokerErrorCode ErrorCode;

        [CanBeNull] public readonly KafkaConnectionErrorInfo ConnectionError;

        [CanBeNull] public readonly Exception Exception;
        
        public KafkaClientBrokerErrorInfo([NotNull] IKafkaClientBroker broker, KafkaClientBrokerErrorCode errorCode, 
            [CanBeNull] KafkaConnectionErrorInfo connectionError, [CanBeNull] Exception exception)
        {
            Broker = broker;
            ErrorCode = errorCode;
            ConnectionError = connectionError;
            Exception = exception;
        }
    }
}
