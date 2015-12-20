namespace NKafka.DevConsole.DevProtocol
{
    public struct ReplicaId
    {
        public ReplicaIdMode? Mode { get; set; }

        public int Id { get; set; }

        public ReplicaId(ReplicaIdMode mode) : this()
        {
            Mode = mode;
            Id = (int)mode;
        }

        public ReplicaId(int replicaId) : this()
        {
            Id = replicaId;
        }                       

        public static readonly ReplicaId AnyReplica = new ReplicaId(ReplicaIdMode.AnyReplica);        
    }
}
