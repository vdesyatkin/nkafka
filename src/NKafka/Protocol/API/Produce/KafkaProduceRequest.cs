using System;
using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    /// <summary>
    /// The produce API is used to send message sets to the server. For efficiency it allows sending message sets intended for many topic partitions in a single request.
    /// </summary>
    internal sealed class KafkaProduceRequest : IKafkaRequest
    {
        /// <summary>
        /// <para>
        /// This field indicates how many acknowledgements the servers should receive before responding to the request.<br/>
        /// If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). <br/>
        /// If it is 1, the server will wait the data is written to the local log before sending a response. <br/>
        /// If it is -1 the server will block until the message is committed by all in sync replicas before sending a response.<br/>
        /// For any number > 1 the server will block waiting for this number of acknowledgements to occur <br/>
        /// (but the server will never wait for more acknowledgements than there are in-sync replicas).
        /// </para>
        /// </summary>
        public readonly KafkaConsistencyLevel RequiredAcks;

        /// <summary>
        /// <para>
        /// This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks.<br/>
        /// The timeout is not an exact limit on the request time for a few reasons: <br/>
        /// (1) it does not include network latency, <br/>
        /// (2) the timer begins at the beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, <br/>
        /// (3) we will not terminate a local write so if the local write time exceeds this timeout it will not be respected. <br/>
        /// To get a hard timeout of this type the client should use the socket timeout.
        /// </para>
        /// </summary>
        public readonly TimeSpan Timeout;

        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaProduceRequestTopic> Topics;

        public KafkaProduceRequest(KafkaConsistencyLevel requiredAcks, TimeSpan timeout, IReadOnlyList<KafkaProduceRequestTopic> topics)
        {
            RequiredAcks = requiredAcks;
            Timeout = timeout;
            Topics = topics;            
        }
    }
}
