using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol
{
    [PublicAPI]
    public enum PartitionAssignmentStrategy : byte
    {   
        Unknown = 0,     
        Range = 1,
        RoundRobin = 2        
    }
}
