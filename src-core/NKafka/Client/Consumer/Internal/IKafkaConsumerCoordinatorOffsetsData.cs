using System;

namespace NKafka.Client.Consumer.Internal
{
    public interface IKafkaConsumerCoordinatorOffsetsData
    {        
        long? GroupServerOffset { get; }  
        DateTime TimestampUtc { get; }   
    }
}
