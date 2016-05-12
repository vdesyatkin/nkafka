using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerLogger
    {
    }

    [PublicAPI]
    public interface IKafkaProducerLogger<TKey, TData> : IKafkaProducerLogger
    {
    }
}
