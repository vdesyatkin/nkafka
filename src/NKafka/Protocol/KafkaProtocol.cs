using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Protocol.API.Produce;
using NKafka.Protocol.API.TopicMetadata;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol
{
    internal sealed class KafkaProtocol
    {
        public readonly int ResponseHeaderSize;

        [NotNull] private readonly KafkaProtocolConfiguration _configuration;                
        [CanBeNull] private readonly string _clientId;
        
        private const int DefaultDataCapacity = 100;        

        public KafkaProtocol(KafkaVersion kafkaVersion, [CanBeNull] string clientId)
        {            
            _clientId = clientId;
            _configuration = CreateConfiguration(kafkaVersion);
            ResponseHeaderSize = 8;          
        }

        public byte[] WriteRequest([NotNull] IKafkaRequest request, int correlationId, int? dataCapacity = null)
        {
            //todo check input data

            KafkaRequestConfiguration requestConfiguration;
            if (!_configuration.Requests.TryGetValue(request.GetType(), out requestConfiguration))
            {
                return null;
            }

            using (var writer = new KafkaBinaryWriter(dataCapacity ?? DefaultDataCapacity))
            {
                writer.BeginWriteSize();

                writer.WriteInt16((short)requestConfiguration.RequestType);
                writer.WriteInt16((short)requestConfiguration.RequestVersion);
                writer.WriteInt32(correlationId);
                writer.WriteString(_clientId);

                requestConfiguration.WriteRequestMethod.Invoke(writer, request);

                writer.EndWriteSize();

                return writer.ToByteArray();                                
            }            
        }

        public KafkaResponseHeader ReadResponseHeader([NotNull] byte[] data, int offset, int count)
        {
            //todo check input data

            using (var reader = new KafkaBinaryReader(data, offset, count))
            {
                var dataSize = reader.ReadInt32();
                if (dataSize < 4)
                {
                    return null; //todo errors
                }
                var correlationId = reader.ReadInt32();

                return new KafkaResponseHeader(dataSize - 4, correlationId);
            }
        }

        public IKafkaResponse ReadResponse([NotNull] IKafkaRequest request, [NotNull] byte[] data, int offset, int count)
        {
            //todo check input data

            KafkaRequestConfiguration requestConfiguration;
            if (!_configuration.Requests.TryGetValue(request.GetType(), out requestConfiguration))
            {
                return null;
            }            

            using (var reader = new KafkaBinaryReader(data, offset, count))
            {
                return requestConfiguration.ReadResponseMethod.Invoke(reader);                
            }
        }        

        #region Configuration

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateV09ApiSupports()
        {
            var requests = new Dictionary<KafkaRequestType, KafkaRequestVersion>
            {
                [KafkaRequestType.TopicMetadata] = KafkaRequestVersion.V0,
                [KafkaRequestType.Produce] = KafkaRequestVersion.V0,

                [KafkaRequestType.GroupCoordinator] = KafkaRequestVersion.V0,
                [KafkaRequestType.JoinGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.SyncGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.Heartbeat] = KafkaRequestVersion.V0,

                [KafkaRequestType.Offsets] = KafkaRequestVersion.V0,
                [KafkaRequestType.OffsetFetch] = KafkaRequestVersion.V1,

                [KafkaRequestType.Fetch] = KafkaRequestVersion.V0,
                [KafkaRequestType.OffsetCommit] = KafkaRequestVersion.V2
            };
            return requests;
        }

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateV08ApiSupports()
        {
            var requests = new Dictionary<KafkaRequestType, KafkaRequestVersion>
            {
                [KafkaRequestType.TopicMetadata] = KafkaRequestVersion.V0,
                [KafkaRequestType.Produce] = KafkaRequestVersion.V0
            };
            return requests;
        }

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateDefaultApiSupports()
        {
            return new Dictionary<KafkaRequestType, KafkaRequestVersion>();            
        }

        private static KafkaProtocolConfiguration CreateConfiguration(KafkaVersion kafkaVersion)
        {
            IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> supportedRequests;
            switch (kafkaVersion)
            {
                case KafkaVersion.V0_9:
                    supportedRequests = CreateV09ApiSupports();
                    break;
                case KafkaVersion.V0_8:
                    supportedRequests = CreateV08ApiSupports();
                    break;
                default:
                    supportedRequests = CreateDefaultApiSupports();
                    break;
            }

            var requests = new Dictionary<Type, KafkaRequestConfiguration>(supportedRequests.Count);
            foreach (var supportRequest in supportedRequests)
            {
                var requestType = supportRequest.Key;
                var requestVersion = supportRequest.Value;

                var requestConfiguration = CreateRequestConfiguration(requestType, requestVersion);
                if (requestConfiguration == null) continue;
                requests[requestConfiguration.RequestClassType] = requestConfiguration;
            }

            return new KafkaProtocolConfiguration(requests);
        }

        private static KafkaRequestConfiguration CreateRequestConfiguration(KafkaRequestType requestType, KafkaRequestVersion requestVersion)
        {
            switch (requestType)
            {
                case KafkaRequestType.TopicMetadata:
                    return new KafkaRequestConfiguration(requestType, requestVersion, typeof(KafkaTopicMetadataRequest),
                        KafkaTopicMetadataApi.WriteRequest, KafkaTopicMetadataApi.ReadResponse);
                case KafkaRequestType.Produce:
                    return new KafkaRequestConfiguration(requestType, requestVersion, typeof(KafkaProduceRequest),
                        KafkaProduceApi.WriteRequest, KafkaProduceApi.ReadResponse);
            }
            return null;
        }

        private static Action<KafkaBinaryWriter, IKafkaRequest> GetRequestWriteMethod(KafkaRequestType requestType)
        {
            switch (requestType)
            {
                case KafkaRequestType.TopicMetadata:
                    return KafkaTopicMetadataApi.WriteRequest;
            }
            return null;
        }

        private sealed class KafkaRequestConfiguration
        {
            public readonly KafkaRequestType RequestType;
            public readonly KafkaRequestVersion RequestVersion;
            [NotNull] public Type RequestClassType;
            [NotNull] public readonly Action<KafkaBinaryWriter, IKafkaRequest> WriteRequestMethod;
            [NotNull] public readonly Func<KafkaBinaryReader, IKafkaResponse> ReadResponseMethod;

            public KafkaRequestConfiguration(KafkaRequestType requestType, KafkaRequestVersion requestVersion,
                [NotNull] Type requestClassType,
                [NotNull] Action<KafkaBinaryWriter, IKafkaRequest> writeRequestMethod,
                [NotNull] Func<KafkaBinaryReader, IKafkaResponse> readResponseMethod)
            {
                RequestType = requestType;
                RequestVersion = requestVersion;
                RequestClassType = requestClassType;
                WriteRequestMethod = writeRequestMethod;
                ReadResponseMethod = readResponseMethod;
            }
        }        

        private sealed class KafkaProtocolConfiguration
        {
            [NotNull]
            public readonly IReadOnlyDictionary<Type, KafkaRequestConfiguration> Requests;            

            public KafkaProtocolConfiguration([NotNull] IReadOnlyDictionary<Type, KafkaRequestConfiguration> requests)
            {
                Requests = requests;
            }
        }

        #endregion Configuration        
    }
}