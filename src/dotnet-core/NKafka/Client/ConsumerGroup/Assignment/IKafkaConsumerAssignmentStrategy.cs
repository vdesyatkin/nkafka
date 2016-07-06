using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public interface IKafkaConsumerAssignmentStrategy
    {
        KafkaConsumerAssignment Assign(KafkaConsumerAssignmentRequest request);
    }
}
