using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopic
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [NotNull] private readonly IKafkaConsumerTopic _consumer;        

        public KafkaConsumerTopic([NotNull] string topicName, [NotNull] IKafkaConsumerTopic consumer)
        {
            TopicName = topicName;
            _consumer = consumer;            
        }

        public KafkaConsumerTopicPartition CreatePartition(int partitionId)
        {
            return new KafkaConsumerTopicPartition(TopicName, partitionId, _consumer);
        }        
    }
}
