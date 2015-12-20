namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetRequestTopicPartition
    {
        public int PartitionId { get; set; }

        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        /// Specify -1 to receive the latest offset (i.e. the offset of the next coming message) 
        /// and -2 to receive the earliest available offset. Note that because offsets are pulled in descending order, 
        /// asking for the earliest offset will always return you a single element.
        /// </summary>
        public OffsetTime Time { get; set; }

        public int MaxNumberOfOffsets { get; set; }
    }
}
