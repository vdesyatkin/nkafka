using System;

namespace NKafka.Client.Consumer.Internal
{
    public interface IKafkaConsumerCoordinatorOffsetsData
    {
        long ClientOffset { get; }
        long ServerOffset { get; }
        DateTime TimestampUtc { get; }
    }
}
