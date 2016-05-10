using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Protocol.API.Fetch;
using NKafka.Protocol.API.GroupCoordinator;
using NKafka.Protocol.API.Heartbeat;
using NKafka.Protocol.API.JoinGroup;
using NKafka.Protocol.API.LeaveGroup;
using NKafka.Protocol.API.Offset;
using NKafka.Protocol.API.OffsetCommit;
using NKafka.Protocol.API.OffsetFetch;
using NKafka.Protocol.API.Produce;
using NKafka.Protocol.API.SyncGroup;
using NKafka.Protocol.API.TopicMetadata;
using NKafka.Protocol.Serialization;

namespace NKafka.Protocol
{
    internal sealed class KafkaProtocol
    {
        public readonly int ResponseHeaderSize = 8;

        [NotNull] private readonly KafkaProtocolConfiguration _configuration;                
        [CanBeNull] private readonly string _clientId;
        
        private const int DefaultDataCapacity = 100;        

        public KafkaProtocol(KafkaVersion kafkaVersion, [CanBeNull] string clientId)
        {            
            _clientId = clientId;
            _configuration = CreateConfiguration(kafkaVersion);            
        }

        [CanBeNull]
        public byte[] WriteRequest([NotNull] IKafkaRequest request, int correlationId, int? dataCapacity = null)
        {
            // ReSharper disable HeuristicUnreachableCode
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse            
            if (request == null) return null;            
            // ReSharper enable HeuristicUnreachableCode

            KafkaRequestConfiguration requestConfiguration;
            if (!_configuration.Requests.TryGetValue(request.GetType(), out requestConfiguration) || requestConfiguration == null)
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

                requestConfiguration.RequestApi.WriteRequest(writer, request);

                writer.EndWriteSize();

                return writer.ToByteArray();                                
            }            
        }

        [CanBeNull]
        public KafkaResponseHeader ReadResponseHeader(byte[] data, int offset, int count)
        {            
            if (data == null)                
            {                       
                return null;
            }
            if (offset < 0 || offset >= data.Length)
            {
                return null;
            }
            if (count < 1 || count > (data.Length - offset))
            {
                return null;
            }         

            using (var reader = new KafkaBinaryReader(data, offset, count))
            {
                var dataSize = reader.ReadInt32();
                if (dataSize < 4)
                {
                    return null; //todo (E007) protocol error
                }
                var correlationId = reader.ReadInt32();

                return new KafkaResponseHeader(dataSize - 4, correlationId);
            }
        }

        [CanBeNull]
        public IKafkaResponse ReadResponse([NotNull] IKafkaRequest request, byte[] data, int offset, int count)
        {            
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse            
            if (request == null || data == null)
            {
                return null;
            }
            if (offset < 0 || offset >= data.Length)
            {
                return null;
            }
            if (count < 1 || count > (data.Length - offset))
            {
                return null;
            }            

            KafkaRequestConfiguration requestConfiguration;
            if (!_configuration.Requests.TryGetValue(request.GetType(), out requestConfiguration) || requestConfiguration == null)
            {
                return null;
            }

            using (var reader = new KafkaBinaryReader(data, offset, count))
            {
                return requestConfiguration.RequestApi.ReadResponse(reader);
            }
        }

        #region Configuration

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateV010ApiSupports()
        {
            var requests = new Dictionary<KafkaRequestType, KafkaRequestVersion>
            {
                [KafkaRequestType.TopicMetadata] = KafkaRequestVersion.V1,
                [KafkaRequestType.Produce] = KafkaRequestVersion.V2,

                [KafkaRequestType.GroupCoordinator] = KafkaRequestVersion.V0,
                [KafkaRequestType.JoinGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.SyncGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.Heartbeat] = KafkaRequestVersion.V0,
                [KafkaRequestType.LeaveGroup] = KafkaRequestVersion.V0,

                [KafkaRequestType.Offset] = KafkaRequestVersion.V0,
                [KafkaRequestType.OffsetFetch] = KafkaRequestVersion.V1,

                [KafkaRequestType.Fetch] = KafkaRequestVersion.V2,
                [KafkaRequestType.OffsetCommit] = KafkaRequestVersion.V2
            };
            return requests;
        }

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateV09ApiSupports()
        {
            var requests = new Dictionary<KafkaRequestType, KafkaRequestVersion>
            {
                [KafkaRequestType.TopicMetadata] = KafkaRequestVersion.V0,
                [KafkaRequestType.Produce] = KafkaRequestVersion.V1,

                [KafkaRequestType.GroupCoordinator] = KafkaRequestVersion.V0,
                [KafkaRequestType.JoinGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.SyncGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.Heartbeat] = KafkaRequestVersion.V0,
                [KafkaRequestType.LeaveGroup] = KafkaRequestVersion.V0,

                [KafkaRequestType.Offset] = KafkaRequestVersion.V0,
                [KafkaRequestType.OffsetFetch] = KafkaRequestVersion.V1,

                [KafkaRequestType.Fetch] = KafkaRequestVersion.V1,
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
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateEmptyApiSupports()
        {
            return new Dictionary<KafkaRequestType, KafkaRequestVersion>();            
        }

        [NotNull]
        private static KafkaProtocolConfiguration CreateConfiguration(KafkaVersion kafkaVersion)
        {
            IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> supportedRequests;
            switch (kafkaVersion)
            {
                case KafkaVersion.V0_10:
                    supportedRequests = CreateV010ApiSupports();
                    break;
                case KafkaVersion.V0_9:
                    supportedRequests = CreateV09ApiSupports();
                    break;
                case KafkaVersion.V0_8:
                    supportedRequests = CreateV08ApiSupports();
                    break;
                default:
                    supportedRequests = CreateEmptyApiSupports();
                    break;
            }

            var requests = new Dictionary<Type, KafkaRequestConfiguration>(supportedRequests.Count);
            foreach (var supportRequest in supportedRequests)
            {
                var requestType = supportRequest.Key;
                var requestVersion = supportRequest.Value;

                var requestConfiguration = CreateRequestConfiguration(requestType, requestVersion);
                if (requestConfiguration == null) continue;
                requests[requestConfiguration.RequestApi.RequestType] = requestConfiguration;
            }

            return new KafkaProtocolConfiguration(requests);
        }

        private static KafkaRequestConfiguration CreateRequestConfiguration(KafkaRequestType requestType, KafkaRequestVersion requestVersion)
        {
            switch (requestType)
            {
                case KafkaRequestType.TopicMetadata:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaTopicMetadataApi(requestVersion));
                case KafkaRequestType.GroupCoordinator:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaGroupCoordinatorApi());                        
                case KafkaRequestType.Produce:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaProduceApi(requestVersion));                        
                case KafkaRequestType.Offset:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaOffsetApi());
                case KafkaRequestType.Fetch:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaFetchApi(requestVersion));
                case KafkaRequestType.JoinGroup:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaJoinGroupApi());
                case KafkaRequestType.SyncGroup:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaSyncGroupApi());
                case KafkaRequestType.LeaveGroup:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaLeaveGroupApi());
                case KafkaRequestType.Heartbeat:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaHearbeatApi());
                case KafkaRequestType.OffsetFetch:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaOffsetFetchApi());
                case KafkaRequestType.OffsetCommit:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaOffsetCommitApi());
            }
            return null;
        }

        private sealed class KafkaRequestConfiguration
        {
            public readonly KafkaRequestType RequestType;
            public readonly KafkaRequestVersion RequestVersion;
            [NotNull] public readonly IKafkaRequestApi RequestApi;            

            public KafkaRequestConfiguration(KafkaRequestType requestType, KafkaRequestVersion requestVersion,
                [NotNull] IKafkaRequestApi requestApi)
            {
                RequestType = requestType;
                RequestVersion = requestVersion;
                RequestApi = requestApi;
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