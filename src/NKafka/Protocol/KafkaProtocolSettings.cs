using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    public sealed class KafkaProtocolSettings
    {
        public readonly int DataSizeByteCountLimit;

        public readonly int StringSizeByteCountLimit;

        public readonly int CollectionItemCountLimit;

        public KafkaProtocolSettings(int dataSizeByteCountLimit, int stringSizeByteCountLimit, int collectionItemCountLimit)
        {
            DataSizeByteCountLimit = dataSizeByteCountLimit;
            StringSizeByteCountLimit = stringSizeByteCountLimit;
            CollectionItemCountLimit = collectionItemCountLimit;
        }
    }
}
