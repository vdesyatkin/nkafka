using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public enum KafkaConsistencyLevel : short
    {
        AllReplicas = -1,
        None = 0,
        OneReplica = 1,
        TwoReplicas = 2,
        ThreeReplicas = 3,
        FourReplicas = 4,
        FiveReplicas = 5,
    }
}
