using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class ProduceRequestTopicPartition
    {
        /// <summary>
        /// The partition that data is being published to.
        /// </summary>
        public int PartitionId { get; set; }            

        public MessageSet MessageSet { get; set; }
    }
}
