using JetBrains.Annotations;

namespace NKafka.Protocol.API.GroupCoordinator
{
    [PublicAPI]
    internal sealed class KafkaGroupCoordinatorResponse : IKafkaResponse
    {
        /// <summary>
        /// Error code.
        /// </summary>
        public readonly KafkaResponseErrorCode ErrorCode;

        /// <summary>
        /// Broker Id.
        /// </summary>
        public readonly int BrokerId;

        /// <summary>
        /// Broker host.
        /// </summary>
        public readonly string Host;

        /// <summary>
        /// Broker port.
        /// </summary>
        public readonly int Port;

        /// <param name="brokerId">Broker Id.</param>
        /// <param name="host">Borker host.</param>
        /// <param name="port">Broker port.</param>
        public KafkaGroupCoordinatorResponse(KafkaResponseErrorCode errorCode, int brokerId, string host, int port)
        {
            ErrorCode = errorCode;
            BrokerId = brokerId;
            Host = host;
            Port = port;
        }
    }
}
