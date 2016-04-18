using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public enum KafkaConsistencyLevel : short
    {
        AllReplicas = -1,
        None = 0,
        OneReplica = 1,        
    }
}
