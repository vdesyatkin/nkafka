using JetBrains.Annotations;
using NKafka.Client.ConsumerGroupObserver.Diagnostics;

namespace NKafka.Client.ConsumerGroupObserver
{    
    [PublicAPI]
    public interface IKafkaConsumerGroupObserver
    {
        string GroupName { get; }
        KafkaConsumerGroupObserverInfo GetDiagnosticsInfo();
    }
}
