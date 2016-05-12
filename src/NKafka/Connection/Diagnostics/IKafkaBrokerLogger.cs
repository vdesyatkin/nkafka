using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    internal interface IKafkaBrokerLogger
    {
        void OnBrokerConnected();

        void OnBrokerDisconnected();

        void OnBrokerError([NotNull] KafkaBrokerErrorInfo error);

        void OnBrokerRequestError([NotNull] KafkaBrokerErrorInfo error, [NotNull] KafkaBrokerRequestInfo requestInfo);
    }
}
