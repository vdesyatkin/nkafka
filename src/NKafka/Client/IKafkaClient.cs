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
        bool TryFlush(TimeSpan flushTimeout);
        void Stop();

        [NotNull]
        KafkaClientInfo GetDiagnosticsInfo();
    }
}