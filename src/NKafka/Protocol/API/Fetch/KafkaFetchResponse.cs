using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    public sealed class KafkaFetchResponse : IKafkaResponse
    {
        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaFetchResponseTopic> Topics;

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. <br/>
        /// (Zero if the request did not violate any quota).
        /// </summary>
        public readonly TimeSpan ThrottleTime;

        public KafkaFetchResponse(IReadOnlyList<KafkaFetchResponseTopic> topics, TimeSpan throttleTime)
        {
            Topics = topics;
            ThrottleTime = throttleTime;
        }
    }
}
