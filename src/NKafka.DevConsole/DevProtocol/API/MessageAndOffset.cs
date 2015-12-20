namespace NKafka.DevConsole.DevProtocol.API
{
    public class MessageAndOffset
    {
        public Message Message { get; set; }

        /// <summary>
        /// This is the offset used in kafka as the log sequence number. 
        /// When the producer is sending messages it doesn't actually know the offset and can fill in any value here it likes.
        /// </summary>
        public long Offset { get; set; }
    }
}
