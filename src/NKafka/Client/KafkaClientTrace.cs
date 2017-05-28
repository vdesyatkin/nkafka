using System;
using System.Diagnostics;

namespace NKafka.Client
{
    public static class KafkaClientTrace
    {
        public static event Action<string> TraceEvent;

        [Conditional("DEBUG")]
        public static void Trace(string text)
        {
            TraceEvent?.Invoke(text);
        }
    }
}