using JetBrains.Annotations;

namespace NKafka.Protocol
{    
    [PublicAPI]
    public sealed class KafkaProtocolSettingsBuilder
    {
        private int? _dataSizeByteCountLimit;
        private int? _stringSizeByteCountLimit;
        private int? _collectionItemCountLimit;        

        [NotNull]
        public static KafkaProtocolSettings Default = new KafkaProtocolSettingsBuilder().Build();

        [PublicAPI, NotNull]
        public KafkaProtocolSettingsBuilder SetDataSizeByteCountLimit(int sizeByteCountLimit)
        {
            _dataSizeByteCountLimit = sizeByteCountLimit;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProtocolSettingsBuilder SetStringSizeByteCountLimit(int sizeByteCountLimit)
        {
            _stringSizeByteCountLimit = sizeByteCountLimit;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProtocolSettingsBuilder SetCollectionItemCountLimit(int itemCountLimit)
        {
            _collectionItemCountLimit = itemCountLimit;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProtocolSettings Build()
        {
            var dataSizeByteCountLimit = _dataSizeByteCountLimit ?? 10 * 1024 * 1024;
            var stringSizeByteCountLimit = _stringSizeByteCountLimit ?? 1024;
            var collectionItemCountLimit = _collectionItemCountLimit ?? 1 * 1000 * 1000;            

            return new KafkaProtocolSettings(dataSizeByteCountLimit, stringSizeByteCountLimit, collectionItemCountLimit);
        }
    }
}
