using System;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client
{
    [PublicAPI]
    public interface IKafkaClient
    {
        KafkaClientStatus Status { get; }

        void Start();        
        void Stop();
        void Pause();
        bool PauseAndWaitFlush(TimeSpan flushTimeout);

        [NotNull]
        KafkaClientInfo GetDiagnosticsInfo();
    }
}