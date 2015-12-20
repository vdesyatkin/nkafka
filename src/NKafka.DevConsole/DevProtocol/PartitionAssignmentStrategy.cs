namespace NKafka.DevConsole.DevProtocol
{
    public enum PartitionAssignmentStrategy : byte
    {   
        Unknown = 0,     
        Range = 1,
        RoundRobin = 2        
    }
}
