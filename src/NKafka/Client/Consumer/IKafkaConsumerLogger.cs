﻿using JetBrains.Annotations;
using NKafka.Client.Consumer.Logging;

namespace NKafka.Client.Consumer
{    
    [PublicAPI]
    public interface IKafkaConsumerLogger
    {        
        void OnTransportError([NotNull] IKafkaConsumerTopic topic, [NotNull] KafkaConsumerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaConsumerTopic topic, [NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaConsumerTopic topic, [NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaConsumerTopic topic, [NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnPartitionErrorReset([NotNull] IKafkaConsumerTopic topic, [NotNull] KafkaConsumerTopicErrorResetInfo partitionError);
    }

    [PublicAPI]
    public interface IKafkaConsumerLogger<TKey, TData>
    {        
        void OnSerializationError([NotNull] IKafkaConsumerTopic<TKey, TData> topic, [NotNull] KafkaConsumerTopicSerializationErrorInfo error);

        void OnTransportError([NotNull] IKafkaConsumerTopic<TKey, TData> topic, [NotNull] KafkaConsumerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaConsumerTopic<TKey, TData> topic, [NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaConsumerTopic<TKey, TData> topic, [NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaConsumerTopic<TKey, TData> topic, [NotNull] KafkaConsumerTopicProtocolErrorInfo error);

        void OnPartitionErrorReset([NotNull] IKafkaConsumerTopic<TKey, TData> topic, [NotNull] KafkaConsumerTopicErrorResetInfo partitionError);
    }
}
