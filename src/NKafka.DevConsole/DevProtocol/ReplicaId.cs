using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol
{
    [PublicAPI]
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
