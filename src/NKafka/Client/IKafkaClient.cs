using JetBrains.Annotations;

namespace NKafka.Client
{
    [PublicAPI]
    public interface IKafkaClient
    {
        void Start();
        void Stop();
    }
}
