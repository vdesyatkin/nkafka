namespace NKafka.DevConsole.DevProtocol.API
{
    public class Message
    {
        /// <summary>
        /// The key is an optional message key that was used for partition assignment. The key can be null.
        /// </summary>
        public byte[] Key { get; set; }

        /// <summary>
        /// The value is the actual message contents as an opaque byte array. Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
        /// </summary>
        public byte[] Value { get; set; }
    }
}
