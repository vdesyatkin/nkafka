using JetBrains.Annotations;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// Topic broker metadata.
    /// </summary>
    [PublicAPI]
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

        /// <summary>
        /// Rack of the broker in data-center.
        /// </summary>
        public readonly string Rack;

        /// <param name="brokerId">Broker Id.</param>
        /// <param name="host">Borker host.</param>
        /// <param name="port">Broker port.</param>
        /// <param name="rack">Rack in data-center.</param>        
        public KafkaTopicMetadataResponseBroker(int brokerId, string host, int port, string rack)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
            Rack = rack;
        }
    }
}
