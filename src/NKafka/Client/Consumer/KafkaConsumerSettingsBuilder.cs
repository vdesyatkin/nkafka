using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    public sealed class KafkaConsumerSettingsBuilder
    {
        private TimeSpan? _consumePeriod;
        private int? _consumeBatchMinSizeBytes;
        private TimeSpan? _consumeTimeout;

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetConsumePeriod(TimeSpan consumePeriod)
        {
            _consumePeriod = consumePeriod;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetBatchByteMinSizeBytes(int batchMinSizeBytes)
        {
            _consumeBatchMinSizeBytes = batchMinSizeBytes;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetConsumeTimeout(TimeSpan consumeTimeout)
        {
            _consumeTimeout = consumeTimeout;
            return this;
        }

        public KafkaConsumerSettings Build()
        {
            var consumePeriod = _consumePeriod ?? TimeSpan.FromSeconds(1);
            var batchMinSizeBytes = _consumeBatchMinSizeBytes ?? 200 * 200;
            var consumeTimeout = _consumeTimeout ?? TimeSpan.Zero;

            return new KafkaConsumerSettings(
                consumePeriod,
                batchMinSizeBytes,
                consumeTimeout);
        }
    }
}
