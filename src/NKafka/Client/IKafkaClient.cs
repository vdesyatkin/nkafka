using System;
using JetBrains.Annotations;

namespace NKafka.Client
{
    [PublicAPI]
    public interface IKafkaClient
    {
        KafkaClientStatus Status { get; }

        void Start();
        bool TryPauseAndFlush(TimeSpan flushTimeout);
        void Stop();        
    }
}
