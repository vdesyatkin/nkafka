﻿using System;
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
    [PublicAPI]
    public sealed class KafkaProtocol
    {
        public readonly int ResponseHeaderSize = 8;

        private readonly KafkaVersion _kafkaVersion;
        [NotNull] private readonly KafkaProtocolConfiguration _configuration;
        [NotNull] private readonly KafkaProtocolSettings _settings;
        [CanBeNull] private readonly string _clientId;

        private const int DefaultDataCapacity = 100;

        public KafkaProtocol(KafkaVersion kafkaVersion, [NotNull] KafkaProtocolSettings settings, [CanBeNull] string clientId)
        {
            _kafkaVersion = kafkaVersion;
            _clientId = clientId;
            _settings = settings;
            _configuration = CreateConfiguration(kafkaVersion);
        }

        public int GetMessageSize(KafkaMessage message)
        {
            if (message == null) return 0;

            var messageSize = _kafkaVersion >= KafkaVersion.V0_10 ? 22 : 14; // header
            if (message.Key != null)
            {
                messageSize += message.Key.Length;
            }

            if (message.Data != null)
            {
                messageSize += message.Data.Length;
            }

            return messageSize;
        }

        public int GetMessageSizeInBatch(KafkaMessage message)
        {
            if (message == null) return 0;

            return GetMessageSize(message) + 12; // header
        }

        public KafkaRequestType? GetRequestType<TRequest>() where TRequest : IKafkaRequest
        {
            KafkaRequestType requestType;
            return StaticRequestTypes.TryGetValue(typeof(TRequest), out requestType) ? requestType : (KafkaRequestType?)null;
        }

        /// <exception cref="KafkaProtocolException"/>
        [NotNull]
        public byte[] WriteRequest([NotNull] IKafkaRequest request, KafkaRequestType requestType, int correlationId, int? dataCapacity = null)
        {
            if (request == null)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidRequestType);
            }

            KafkaRequestConfiguration requestConfiguration;
            if (!_configuration.Requests.TryGetValue(requestType, out requestConfiguration) || requestConfiguration == null)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidRequestType);
            }

            try
            {
                using (var writer = new KafkaBinaryWriter(dataCapacity ?? DefaultDataCapacity))
                {
                    writer.BeginWriteSize();

                    writer.WriteInt16((short)requestConfiguration.RequestType);
                    writer.WriteInt16((short)requestConfiguration.RequestVersion);
                    writer.WriteInt32(correlationId);
                    writer.WriteString(_clientId);

                    requestConfiguration.RequestApi.WriteRequest(writer, request);

                    writer.EndWriteSize();

                    return writer.ToByteArray() ?? new byte[0];
                }
            }
            catch (KafkaProtocolException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.IOError, exception);
            }
        }

        /// <exception cref="KafkaProtocolException"/>
        [NotNull]
        public KafkaResponseHeader ReadResponseHeader(byte[] data, int offset, int count)
        {
            if (data == null)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }
            if (offset < 0 || offset >= data.Length)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }
            if (count < 1 || count > (data.Length - offset))
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }

            try
            {
                using (var reader = new KafkaBinaryReader(data, offset, count, _settings))
                {
                    var dataSize = reader.ReadInt32();
                    if (dataSize < 4)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
                    }
                    if (dataSize > _settings.DataSizeByteCountLimit)
                    {
                        throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
                    }
                    var correlationId = reader.ReadInt32();

                    return new KafkaResponseHeader(dataSize - 4, correlationId);
                }
            }
            catch (KafkaProtocolException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.IOError, exception);
            }
        }

        /// <exception cref="KafkaProtocolException"/>
        [NotNull]
        public IKafkaResponse ReadResponse(KafkaRequestType requestType, byte[] data, int offset, int count)
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse            
            if (data == null)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }
            if (offset < 0 || offset >= data.Length)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }
            if (count < 1 || count > (data.Length - offset))
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }

            KafkaRequestConfiguration requestConfiguration;
            if (!_configuration.Requests.TryGetValue(requestType, out requestConfiguration) || requestConfiguration == null)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidRequestType);
            }

            try
            {
                using (var reader = new KafkaBinaryReader(data, offset, count, _settings))
                {
                    return requestConfiguration.RequestApi.ReadResponse(reader);
                }
            }
            catch (KafkaProtocolException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.IOError, exception);
            }
        }

        #region Configuration

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateV0102ApiSupports()
        {
            var requests = new Dictionary<KafkaRequestType, KafkaRequestVersion>
            {
                [KafkaRequestType.TopicMetadata] = KafkaRequestVersion.V2,
                [KafkaRequestType.Produce] = KafkaRequestVersion.V2,

                [KafkaRequestType.GroupCoordinator] = KafkaRequestVersion.V0,
                [KafkaRequestType.JoinGroup] = KafkaRequestVersion.V1,
                [KafkaRequestType.SyncGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.Heartbeat] = KafkaRequestVersion.V0,
                [KafkaRequestType.LeaveGroup] = KafkaRequestVersion.V0,

                [KafkaRequestType.Offset] = KafkaRequestVersion.V0, //todo V1 KIP-79 - ListOffsetRequest/ListOffsetResponse v1 and add timestamp search methods to the new consumer
                [KafkaRequestType.OffsetFetch] = KafkaRequestVersion.V2,

                [KafkaRequestType.Fetch] = KafkaRequestVersion.V3,
                [KafkaRequestType.OffsetCommit] = KafkaRequestVersion.V2
            };
            return requests;
        }

        [NotNull]
        private static IReadOnlyDictionary<KafkaRequestType, KafkaRequestVersion> CreateV0101ApiSupports()
        {
            var requests = new Dictionary<KafkaRequestType, KafkaRequestVersion>
            {
                [KafkaRequestType.TopicMetadata] = KafkaRequestVersion.V2,
                [KafkaRequestType.Produce] = KafkaRequestVersion.V2,

                [KafkaRequestType.GroupCoordinator] = KafkaRequestVersion.V0,
                [KafkaRequestType.JoinGroup] = KafkaRequestVersion.V1,
                [KafkaRequestType.SyncGroup] = KafkaRequestVersion.V0,
                [KafkaRequestType.Heartbeat] = KafkaRequestVersion.V0,
                [KafkaRequestType.LeaveGroup] = KafkaRequestVersion.V0,

                [KafkaRequestType.Offset] = KafkaRequestVersion.V0, //todo V1 KIP-79 - ListOffsetRequest/ListOffsetResponse v1 and add timestamp search methods to the new consumer
                [KafkaRequestType.OffsetFetch] = KafkaRequestVersion.V1,

                [KafkaRequestType.Fetch] = KafkaRequestVersion.V3,
                [KafkaRequestType.OffsetCommit] = KafkaRequestVersion.V2
            };
            return requests;
        }

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
                case KafkaVersion.V0_10_2:
                    supportedRequests = CreateV0102ApiSupports();
                    break;
                case KafkaVersion.V0_10_1:
                    supportedRequests = CreateV0101ApiSupports();
                    break;
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

            var requests = new Dictionary<KafkaRequestType, KafkaRequestConfiguration>(supportedRequests.Count);
            foreach (var supportRequest in supportedRequests)
            {
                var requestType = supportRequest.Key;
                var requestVersion = supportRequest.Value;

                var requestConfiguration = CreateRequestConfiguration(requestType, requestVersion);
                if (requestConfiguration == null) continue;
                requests[requestType] = requestConfiguration;
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
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaJoinGroupApi(requestVersion));
                case KafkaRequestType.SyncGroup:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaSyncGroupApi());
                case KafkaRequestType.LeaveGroup:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaLeaveGroupApi());
                case KafkaRequestType.Heartbeat:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaHearbeatApi());
                case KafkaRequestType.OffsetFetch:
                    return new KafkaRequestConfiguration(requestType, requestVersion, new KafkaOffsetFetchApi(requestVersion));
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
            public readonly IReadOnlyDictionary<KafkaRequestType, KafkaRequestConfiguration> Requests;

            public KafkaProtocolConfiguration([NotNull] IReadOnlyDictionary<KafkaRequestType, KafkaRequestConfiguration> requests)
            {
                Requests = requests;
            }
        }

        #endregion Configuration        

        #region RequestTypes

        [NotNull]
        private static readonly Dictionary<Type, KafkaRequestType> StaticRequestTypes = new Dictionary<Type, KafkaRequestType>
        {
            [typeof(KafkaProduceRequest)] = KafkaRequestType.Produce,
            [typeof(KafkaFetchRequest)] = KafkaRequestType.Fetch,
            [typeof(KafkaOffsetRequest)] = KafkaRequestType.Offset,
            [typeof(KafkaTopicMetadataRequest)] = KafkaRequestType.TopicMetadata,
            [typeof(KafkaOffsetCommitRequest)] = KafkaRequestType.OffsetCommit,
            [typeof(KafkaOffsetFetchRequest)] = KafkaRequestType.OffsetFetch,
            [typeof(KafkaGroupCoordinatorRequest)] = KafkaRequestType.GroupCoordinator,
            [typeof(KafkaJoinGroupRequest)] = KafkaRequestType.JoinGroup,
            [typeof(KafkaHeartbeatRequest)] = KafkaRequestType.Heartbeat,
            [typeof(KafkaLeaveGroupRequest)] = KafkaRequestType.LeaveGroup,
            [typeof(KafkaSyncGroupRequest)] = KafkaRequestType.SyncGroup
        };

        #endregion
    }
}