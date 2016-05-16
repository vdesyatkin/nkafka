using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
    public sealed class KafkaProduceResponse: IKafkaResponse
    {
        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaProduceResponseTopic> Topics;

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. <br/>
        /// (Zero if the request did not violate any quota).
        /// </summary>
        public readonly TimeSpan ThrottleTime;

        public KafkaProduceResponse(IReadOnlyList<KafkaProduceResponseTopic> topics, TimeSpan throttleTime)
        {
            Topics = topics;
            ThrottleTime = throttleTime;
        }
    }
}
