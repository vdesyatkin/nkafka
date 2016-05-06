using System;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace NKafka.Client
{
    [PublicAPI]
    public interface IKafkaClient
    {
        void Start();
        void Stop();

        [NotNull] Task<bool> TryFlushAndStop(TimeSpan flushTimeout);        
    }
}
