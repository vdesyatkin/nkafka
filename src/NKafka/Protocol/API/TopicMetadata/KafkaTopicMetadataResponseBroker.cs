namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// Topic broker metadata.
    /// </summary>
    internal sealed class KafkaTopicMetadataResponseBroker
    {
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
        public KafkaTopicMetadataResponseBroker(int brokerId, string host, int port)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
        }
    }
}
