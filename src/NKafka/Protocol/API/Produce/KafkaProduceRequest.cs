using System;
using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceRequest : IKafkaRequest
    {
        /// <summary>
        /// This field indicates how many acknowledgements the servers should receive before responding to the request. 
        /// If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). 
        /// If it is 1, the server will wait the data is written to the local log before sending a response. 
        /// If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. 
        /// For any number > 1 the server will block waiting for this number of acknowledgements to occur 
        /// (but the server will never wait for more acknowledgements than there are in-sync replicas).
        /// </summary>
        public KafkaConsistencyLevel RequiredAcks { get; private set; }

        /// <summary>
        /// This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks. 
        /// The timeout is not an exact limit on the request time for a few reasons: 
        /// (1) it does not include network latency, 
        /// (2) the timer begins at the beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, 
        /// (3) we will not terminate a local write so if the local write time exceeds this timeout it will not be respected. 
        /// To get a hard timeout of this type the client should use the socket timeout.
        /// </summary>
        public TimeSpan Timeout { get; private set; }

        public IReadOnlyList<KafkaProduceRequestTopic> Topics { get; set; }

        public KafkaProduceRequest(KafkaConsistencyLevel requiredAcks, TimeSpan timeout, IReadOnlyList<KafkaProduceRequestTopic> topics)
        {
            RequiredAcks = requiredAcks;
            Timeout = timeout;
            Topics = topics;
        }
    }
}
