namespace NKafka.DevConsole.DevProtocol.API
{
    public class TopicMetadataResponseBroker
    {
        public int BrokerId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
    }
}
