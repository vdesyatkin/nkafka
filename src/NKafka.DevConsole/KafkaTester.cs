using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NKafka.DevConsole.DevProtocol;
using NKafka.DevConsole.DevProtocol.API;
// ReSharper disable UnusedMember.Local
// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedVariable
// ReSharper disable UnusedParameter.Local
// ReSharper disable NotAccessedVariable
// ReSharper disable UseStringInterpolation
// ReSharper disable RedundantAssignment

namespace NKafka.DevConsole
{
    public class KafkaTester
    {
        private const string ClientId = "kafka-proto";
        private const byte MagicNumber = 0;
        private const long DefaultMessageOffset = 0; // wtf?
        private const byte DefaultMessageAttribute = 0;
        private static readonly byte GZipMessageAttribute = (byte)(0x00 | (ProtocolConstants.AttributeCodeMask & (byte)MessageCodec.CodecGzip));

        private static int _correlationId;

        private const ApiVersion DefaultApi = ApiVersion.V0;
        private const string DefaultProtocolType = "consumer";
        private const string UnknownMember = "";

        private static byte[] PackLeaveGroupRequest(LeaveGroupRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.LeaveGroup);

                    writer.Write(PackString16(request.GroupId));
                    writer.Write(PackString16(request.MemberId));
                }
                return stream.ToArray();
            }
        }

        private static LeaveGroupResponse UnpackLeaveGroupResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var error = (ErrorResponseCode)UnpackInt16(reader);                    

                    return new LeaveGroupResponse
                    {
                        Error = error
                    };
                }
            }
        }

        private static byte[] PackSyncGroupReuest(SyncGroupRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.SyncGroup);

                    writer.Write(PackString16(request.GroupId));
                    writer.Write(PackInt32(request.GroupGenerationId));
                    writer.Write(PackString16(request.MemberId));

                    writer.Write(PackInt32(request.Members.Count));
                    foreach (var member in request.Members)
                    {
                        writer.Write(PackString16(member.MemberId));

                        var memberData = new List<byte>(100);

                        memberData.AddRange(PackInt16(member.ProtocolVersion));
                        memberData.AddRange(PackInt32(member.AssignedTopics.Count));
                        foreach (var topic in member.AssignedTopics)
                        {
                            memberData.AddRange(PackString16(topic.TopicName));
                            memberData.AddRange(PackInt32(topic.PartitionIds.Count));
                            foreach (var partitionId in topic.PartitionIds)
                            {
                                memberData.AddRange(PackInt32(partitionId));
                            }                            
                        }
                        memberData.AddRange(PackByteArray32(member.CustomData));                        

                        writer.Write(PackByteArray32(memberData.ToArray()));
                    }
                }
                return stream.ToArray();
            }
        }

        private static SyncGroupResponse UnpackSyncGroupResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var error = (ErrorResponseCode)UnpackInt16(reader);
                  
                    var dataSize = UnpackInt32(reader);
                    if (dataSize <= 0) return new SyncGroupResponse {Error = error};

                    var protocolVersion = UnpackInt16(reader);

                    var topicCount = UnpackInt32(reader);
                    var topics = new List<SyncGroupResponseAssignmentTopic>(topicCount);
                    for (var j = 0; j < topicCount; j++)
                    {
                        var topicName = UnpackString16(reader);
                        var partitionCount = UnpackInt32(reader);
                        var partitions = new List<int>(partitionCount);
                        for (var k = 0; k < partitionCount; k++)
                        {
                            partitions.Add(UnpackInt32(reader));
                        }
                        var topic = new SyncGroupResponseAssignmentTopic { TopicName = topicName, PartitionIds = partitions};
                        topics.Add(topic);
                    }

                    var customData = UnpackByteArray32(reader);                    

                    var assignment = new SyncGroupResponseAssignment
                    {                                         
                        ProtocolVersion = protocolVersion,
                        AssignedTopics = topics,
                        CustomData = customData
                    };

                    return new SyncGroupResponse
                    {
                        Error = error,                        
                        Assignment = assignment                      
                    };
                }
            }
        }

        private static byte[] PackHeartbeatReuest(HeartbeatRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.Heartbeat);

                    writer.Write(PackString16(request.GroupId));
                    writer.Write(PackInt32(request.GroupGenerationId));
                    writer.Write(PackString16(request.MemberId));                    
                }
                return stream.ToArray();
            }
        }

        private static HeartbeatResponse UnpackHeartbeatResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);
                    var error = (ErrorResponseCode)UnpackInt16(reader);                    

                    return new HeartbeatResponse { Erorr = error };
                }
            }
        }

        private static byte[] PackJoinGroupRequest(JoinGroupRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.JoinGroup);

                    writer.Write(PackString16(request.GroupId));
                    writer.Write(PackInt32(request.SessionTimeout.TimeoutMs));
                    writer.Write(PackString16(string.IsNullOrEmpty(request.MemberId) ? UnknownMember : request.MemberId));
                    writer.Write(PackString16(DefaultProtocolType));

                    writer.Write(PackInt32(request.Protocols.Count));
                    foreach (var protocol in request.Protocols)
                    {
                        writer.Write(PackString16(protocol.ProtocolName));

                        var protocolData = new List<byte>(100);

                        protocolData.AddRange(PackInt16(protocol.Version));
                        protocolData.AddRange(PackInt32(protocol.TopicNames.Count));
                        foreach (var topicName in protocol.TopicNames)
                        {
                            protocolData.AddRange(PackString16(topicName));
                        }

                        protocolData.AddRange(PackByteArray32(protocol.CustomData));

                        //protocolData.AddRange(PackString16(protocol.AssignmentStrategies[0]));

                        protocolData.AddRange(PackInt32(protocol.AssignmentStrategies.Count));
                        foreach (var strategy in protocol.AssignmentStrategies)
                        {
                            protocolData.AddRange(PackString16(strategy));
                        }

                        writer.Write(PackByteArray32(protocolData.ToArray()));
                    }
                }
                return stream.ToArray();
            }
        }

        private static JoinGroupResponse UnpackJoinGroupResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var error = (ErrorResponseCode)UnpackInt16(reader);
                    var groupGenerationId = UnpackInt32(reader);
                    var groupProtocol = UnpackString16(reader);
                    var groupLeaderId = UnpackString16(reader);
                    var memberId = UnpackString16(reader);

                    var memberCount = UnpackInt32(reader);
                    var members = new List<JoinGroupResponseMember>(memberCount);
                    for (var i = 0; i < memberCount; i++)
                    {
                        var groupMemberId = UnpackString16(reader);

                        var memberDataSize = UnpackInt32(reader);
                        if (memberDataSize <= 0) continue;

                        var protocolVersion = UnpackInt16(reader);

                        var topicCount = UnpackInt32(reader);
                        var topicNames = new List<string>(topicCount);
                        for (var j = 0; j < topicCount; j++)
                        {
                            var topicName = UnpackString16(reader);
                            topicNames.Add(topicName);
                        }

                        var customData = UnpackByteArray32(reader);

                        var strategyCount = UnpackInt32(reader);
                        var strategies = new List<string>(strategyCount);
                        for (var j = 0; j < strategyCount; j++)
                        {
                            var strategy = UnpackString16(reader);
                            strategies.Add(strategy);
                        }

                        var member = new JoinGroupResponseMember
                        {
                            MemberId = groupMemberId,
                            ProtocolVersion = protocolVersion,
                            TopicNames = topicNames,
                            CustomData = customData,
                            AssignmentStrategies = strategies
                        };
                        members.Add(member);
                    }

                    return new JoinGroupResponse
                    {
                        Erorr = error,
                        GroupGenerationId = groupGenerationId,
                        GroupProtocol = groupProtocol,
                        GroupLeaderId = groupLeaderId,
                        MemberId = memberId,
                        Members = members
                    };
                }
            }
        }

        private static byte[] PackOffsetCommitRequestV2(OffsetCommitRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.OffsetCommit, ApiVersion.V2);

                    writer.Write(PackString16(request.GroupId));
                    writer.Write(PackInt32(request.GroupGenerationId));
                    writer.Write(PackString16(request.MemberId));
                    writer.Write(PackInt64(request.RetentionTime != null ? (long)Math.Round(request.RetentionTime.Value.TotalMilliseconds) : (long?)null));

                    writer.Write(PackInt32(request.Topics.Count));
                    foreach (var topic in request.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.Partitions.Count));
                        foreach (var partition in topic.Partitions)
                        {
                            writer.Write(PackInt32(partition.PartitionId));
                            writer.Write(PackInt64(partition.Offset));                            
                            writer.Write(PackString16(partition.Metadata));
                        }
                    }
                }
                return stream.ToArray();
            }
        }

        private static byte[] PackOffsetCommitRequestV1(OffsetCommitRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.OffsetCommit, ApiVersion.V1);

                    writer.Write(PackString16(request.GroupId));
                    writer.Write(PackInt32(request.GroupGenerationId));
                    writer.Write(PackString16(request.MemberId));

                    var timestamp = request.RetentionTime.HasValue ? DateTime.UtcNow + request.RetentionTime.Value : (DateTime?) null;
                    var timestampMs = timestamp != null ? (long)Math.Round((timestamp.Value - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds) : (long?)null;                    

                    writer.Write(PackInt32(request.Topics.Count));
                    foreach (var topic in request.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.Partitions.Count));
                        foreach (var partition in topic.Partitions)
                        {
                            writer.Write(PackInt32(partition.PartitionId));
                            writer.Write(PackInt64(partition.Offset));
                            writer.Write(PackInt64(timestampMs));
                            writer.Write(PackString16(partition.Metadata));
                        }
                    }
                }
                return stream.ToArray();
            }
        }

        private static byte[] PackOffsetCommitRequestV0(OffsetCommitRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.OffsetCommit, ApiVersion.V0);

                    writer.Write(PackString16(request.GroupId));                    

                    writer.Write(PackInt32(request.Topics.Count));
                    foreach (var topic in request.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.Partitions.Count));
                        foreach (var partition in topic.Partitions)
                        {
                            writer.Write(PackInt32(partition.PartitionId));
                            writer.Write(PackInt64(partition.Offset));                            
                            writer.Write(PackString16(partition.Metadata));
                        }
                    }
                }
                return stream.ToArray();
            }
        }

        private static OffsetCommitResponse UnpackOffsetCommitResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var topicCount = UnpackInt32(reader);
                    var topics = new List<OffsetCommitResponseTopic>(topicCount);
                    for (var i = 0; i < topicCount; i++)
                    {
                        var topicName = UnpackString16(reader);

                        var partitionCount = UnpackInt32(reader);
                        var partitions = new List<OffsetCommitResponseTopicPartition>(partitionCount);
                        for (var j = 0; j < partitionCount; j++)
                        {
                            var partitionId = UnpackInt32(reader);
                            var error = (ErrorResponseCode)UnpackInt16(reader);
                            
                            var partition = new OffsetCommitResponseTopicPartition { PartitionId = partitionId, Error = error };
                            partitions.Add(partition);
                        }

                        var topic = new OffsetCommitResponseTopic { TopicName = topicName, Partitions = partitions };
                        topics.Add(topic);
                    }

                    return new OffsetCommitResponse { Topics = topics };
                }
            }
        }

        private static byte[] PackOffsetRequest(OffsetRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.Offsets);

                    writer.Write(PackInt32(request.ReplicaId.Id));

                    writer.Write(PackInt32(request.Topics.Count));
                    foreach (var topic in request.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.Partitions.Count));
                        foreach (var partition in topic.Partitions)
                        {
                            writer.Write(PackInt32(partition.PartitionId));
                            writer.Write(PackInt64(partition.Time.TimeMs));
                            writer.Write(PackInt32(partition.MaxNumberOfOffsets));
                        }
                    }
                }
                return stream.ToArray();
            }
        }

        private static OffsetResponse UnpackOffsetResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var topicCount = UnpackInt32(reader);
                    var topics = new List<OffsetResponseTopic>(topicCount);
                    for (var i = 0; i < topicCount; i++)
                    {
                        var topicName = UnpackString16(reader);

                        var partitionCount = UnpackInt32(reader);
                        var partitions = new List<OffsetResponseTopicPartition>(partitionCount);
                        for (var j = 0; j < partitionCount; j++)
                        {
                            var partitionId = UnpackInt32(reader);
                            var error = (ErrorResponseCode)UnpackInt16(reader);
                            var offsetCount = UnpackInt32(reader);
                            var offsets = new List<long>();
                            for (var k = 0; k < offsetCount; k++)
                            {
                                var offset = UnpackInt64(reader);
                                offsets.Add(offset);
                            }
                            var partition = new OffsetResponseTopicPartition { PartitionId = partitionId, Error = error, Offsets = offsets };
                            partitions.Add(partition);
                        }

                        var topic = new OffsetResponseTopic { TopicName = topicName, Partitions = partitions };
                        topics.Add(topic);
                    }

                    return new OffsetResponse { Topics = topics };
                }
            }
        }

        private static byte[] PackFetchRequest(FetchRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.Fetch);

                    writer.Write(PackInt32(request.ReplicaId.Id));
                    writer.Write(PackInt32(request.MaxWaitTimeMs));
                    writer.Write(PackInt32(request.MinBytes));                    

                    writer.Write(PackInt32(request.Topics.Count));
                    foreach (var topic in request.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.Partitions.Count));
                        foreach (var partition in topic.Partitions)
                        {
                            writer.Write(PackInt32(partition.PartitionId));
                            writer.Write(PackInt64(partition.FetchOffset));
                            writer.Write(PackInt32(partition.MaxBytes));
                        }
                    }
                }
                return stream.ToArray();
            }
        }        

        private static FetchResponse UnpackFetchResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var topicCount = UnpackInt32(reader);
                    var topics = new List<FetchResponseTopic>(topicCount);
                    for (var i = 0; i < topicCount; i++)
                    {
                        var topicName = UnpackString16(reader);

                        var partitionCount = UnpackInt32(reader);
                        var partitions = new List<FetchResponseTopicPartition>(partitionCount);
                        for (var j = 0; j < partitionCount; j++)
                        {
                            var partitionId = UnpackInt32(reader);
                            var error = (ErrorResponseCode)UnpackInt16(reader);
                            var highwaterMarkOffset = UnpackInt64(reader);
                            var messageSet = UnpackMessageSet(reader);
                            
                            var partition = new FetchResponseTopicPartition { PartitionId = partitionId, Error = error, HighwaterMarkOffset = highwaterMarkOffset, MessageSet = messageSet };
                            partitions.Add(partition);
                        }

                        var topic = new FetchResponseTopic { TopicName = topicName, Partitions = partitions };
                        topics.Add(topic);
                    }

                    return new FetchResponse { Topics = topics };
                }
            }
        }

        private static byte[] PackOffsetFetchRequest(OffsetFetchRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.OffsetFetch, ApiVersion.V1);
                    writer.Write(PackString16(request.GroupId));

                    writer.Write(PackInt32(request.Topics.Count));
                    foreach (var topic in request.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.PartitionIds.Count));
                        foreach (var partitionId in topic.PartitionIds)
                        {
                            writer.Write(PackInt32(partitionId));
                        }
                    }
                }
                return stream.ToArray();
            }
        }

        private static OffsetFetchResponse UnpackOffsetFetchResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);

                    var topicCount = UnpackInt32(reader);
                    var topics = new List<OffsetFetchResponseTopic>(topicCount);
                    for (var i = 0; i < topicCount; i++)
                    {
                        var topicName = UnpackString16(reader);

                        var partitionCount = UnpackInt32(reader);
                        var partitions = new List<OffsetFetchResponseTopicPartition>(partitionCount);
                        for (var j = 0; j < partitionCount; j++)
                        {
                            var partitionId = UnpackInt32(reader);
                            var offset = UnpackInt64(reader);
                            var metadata = UnpackString16(reader);
                            var error = (ErrorResponseCode)UnpackInt16(reader);
                            var partition = new OffsetFetchResponseTopicPartition { PartitionId = partitionId, Offset = offset, Metadata = metadata, Error = error };
                            partitions.Add(partition);
                        }

                        var topic = new OffsetFetchResponseTopic { TopicName = topicName, Partitions = partitions };
                        topics.Add(topic);
                    }

                    return new OffsetFetchResponse { Topics = topics };
                }
            }
        }

        private static byte[] PackGroupCoordinatorRequest(GroupCoordinatorRequest request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.GroupCoordinator);
                    writer.Write(PackString16(request.GroupId));                 
                }
                return stream.ToArray();
            }
        }

        private static GroupCoordinatorResponse UnpackGroupCoordinatorResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);
                    var errorCode = (ErrorResponseCode)UnpackInt16(reader);
                    var coordinatorId = UnpackInt32(reader);
                    var coordinatorHost = UnpackString16(reader);
                    var coordinatorPort = UnpackInt32(reader);
                    return new GroupCoordinatorResponse { Error = errorCode, CoordinatorId = coordinatorId, CoordinatorHost = coordinatorHost, CoordinatorPort = coordinatorPort };
                }
            }
        }

        private static byte[] PackProduceRequest(ProduceRequest produceRequest)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.Produce);
                    writer.Write(PackInt16((short)produceRequest.AckMode));
                    writer.Write(PackInt32(produceRequest.Timeout.TimeoutMs));
                    writer.Write(PackInt32(produceRequest.Topics.Count));
                    foreach (var topic in produceRequest.Topics)
                    {
                        writer.Write(PackString16(topic.TopicName));
                        writer.Write(PackInt32(topic.Partitions.Count));

                        foreach (var partition in topic.Partitions)
                        {
                            writer.Write(PackInt32(partition.PartitionId));
                            var messagesData = PackMessageSet(partition.MessageSet);
                            writer.Write(PackByteArray32(messagesData));
                        }
                    }
                }

                return stream.ToArray();
            }
        }

        private static byte[] PackMessageSet(MessageSet messageSet)
        {
            if (messageSet.Codec == MessageCodec.CodecGzip)
            {
                var rawData = PackMessages(messageSet.Messages, DefaultMessageAttribute);
                var zipBytes = Compression.Zip(rawData);
                var zipMessage = new Message { Key = null, Value = zipBytes };
                var zipMessageSet = CreateMessageSet(new[] { zipMessage });

                return PackMessages(zipMessageSet.Messages, GZipMessageAttribute);
            }

            return PackMessages(messageSet.Messages, DefaultMessageAttribute);
        }

        private static MessageSet UnpackMessageSet(BinaryReader reader)
        {
            var messageSetSize = UnpackInt32(reader);            

            var messages = new List<MessageAndOffset>(100);
            var position = 0;
            while (position < messageSetSize)
            {
                var offset = UnpackInt64(reader);
                var messageSize = UnpackInt32(reader);

                var crc = UnpackUInt32(reader);
                var magicNumber = UnpackInt8(reader);
                var attribute = UnpackInt8(reader);
                var key = UnpackByteArray32(reader);
                var value = UnpackByteArray32(reader);

                var message = new Message { Key = key, Value = value };
                var messageAndOffset = new MessageAndOffset { Message = message, Offset = offset };
                messages.Add(messageAndOffset);

                position += 8 + 4 + messageSize;
            }
            return new MessageSet { Messages = messages };
        }

        private static byte[] PackMessages(IReadOnlyList<MessageAndOffset> messages, byte messageAttribute)
        {
            var data = new List<byte>(100 * messages.Count);
            foreach (var message in messages)
            {
                data.AddRange(PackInt64(message.Offset));
                var messageData = PackMessageWithCrc(message.Message, messageAttribute);
                data.AddRange(PackByteArray32(messageData));
            }
            return data.ToArray();
        }

        private static byte[] PackMessageWithCrc(Message message, byte messageAttribute)
        {
            var messageData = PackMessage(message, messageAttribute);            
            var crcData = PackUInt32(Crc32.Compute(messageData));
            var data = new List<byte>(messageData.Length + 4);
            data.AddRange(crcData);
            data.AddRange(messageData);
            return data.ToArray();
        }

        private static byte[] PackMessage(Message message, byte attribute)
        {
            var data = new List<byte>(100);
            data.AddRange(PackInt8(MagicNumber));            
            data.AddRange(PackInt8(attribute));
            data.AddRange(PackByteArray32(message.Key));
            data.AddRange(PackByteArray32(message.Value));
            return data.ToArray();
        }      

        private static byte[] PackMetadataRequest(TopicMetadataRequest request)
        {
            var topics = request.TopicNames;
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    WriteHeader(writer, ApiKeyRequestType.TopicMetadata);

                    writer.Write(PackInt32(topics?.Count ?? 0));
                    if (topics != null)
                    {
                        foreach (var topic in topics)
                        {
                            writer.Write(PackString16(topic));
                        }
                    }                   
                }

                return stream.ToArray();
            }
        }

        private static void WriteHeader(BinaryWriter writer, ApiKeyRequestType requestType, ApiVersion? apiVersion = null)
        {                        
            var version = PackInt16((short)(apiVersion ?? DefaultApi));
            var clientId = PackString16(ClientId);

            writer.Write(PackInt16((short)requestType));
            writer.Write(version);
            writer.Write(PackInt32(_correlationId++));
            writer.Write(clientId);
        }        

        private static byte[] PackByteArray32(byte[] data)
        {
            if (data == null)
            {
                return PackInt32(-1);
            }
            
            var lengthData = PackInt32((short)data.Length);

            var result = new byte[lengthData.Length + data.Length];
            lengthData.CopyTo(result, 0);
            data.CopyTo(result, lengthData.Length);
            return result;
        }

        private static byte[] PackInt8(byte data)
        {
            return new[] { data };
        }

        private static byte[] PackInt16(short data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }

        private static byte[] PackInt32(int? data)
        {
            return PackInt32(data ?? -1);
        }

        private static byte[] PackInt32(int data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }

        private static byte[] PackUInt32(uint data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }

        private static byte[] PackInt64(long? data)
        {
            return PackInt64(data ?? -1);
        }

        private static byte[] PackInt64(long data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return bytes;
        }

        private static byte[] PackString16(string data)
        {
            if (data == null)
            {
                return PackInt16(-1);
            }
                       
            var stringData = Encoding.UTF8.GetBytes(data);
            var lengthData = PackInt16((short)stringData.Length);

            var result = new byte[lengthData.Length + stringData.Length];
            lengthData.CopyTo(result, 0);
            stringData.CopyTo(result, lengthData.Length);
            return result;
        }

        private static byte UnpackInt8(BinaryReader reader)
        {
            return reader.ReadByte();            
        }

        private static short UnpackInt16(BinaryReader reader)
        {
            var bytes = reader.ReadBytes(2);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt16(bytes, 0);
        }

        private static int UnpackInt32(byte[] bytes)
        {
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt32(bytes, 0);
        }

        private static uint UnpackUInt32(BinaryReader reader)
        {
            var bytes = reader.ReadBytes(4);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToUInt32(bytes, 0);
        }

        private static int UnpackInt32(BinaryReader reader)
        {
            var bytes = reader.ReadBytes(4);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt32(bytes, 0);
        }

        private static long UnpackInt64(BinaryReader reader)
        {
            var bytes = reader.ReadBytes(8);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt64(bytes, 0);
        }

        private static string UnpackString16(BinaryReader reader)
        {
            var size = UnpackInt16(reader);
            if (size == -1) return null;

            var bytes = reader.ReadBytes(size);            

            return Encoding.UTF8.GetString(bytes);
        }

        private static byte[] UnpackByteArray32(BinaryReader reader)
        {
            var size = UnpackInt32(reader);
            if (size == -1) return null;

            var bytes = reader.ReadBytes(size);

            return bytes;
        }

        private static TopicMetadataResponse UnpackMetadata(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {                    
                    var correlationId = UnpackInt32(reader);

                    var brokerCount = UnpackInt32(reader);
                    var brokers = new List<TopicMetadataResponseBroker>(brokerCount);
                    for (var i = 0; i < brokerCount; i++)
                    {
                        var brokerId = UnpackInt32(reader);
                        var host = UnpackString16(reader);
                        var port = UnpackInt32(reader);
                        var broker = new TopicMetadataResponseBroker { BrokerId = brokerId, Host = host, Port = port };
                        brokers.Add(broker);
                    }

                    var topicCount = UnpackInt32(reader);
                    var topics = new List<TopicMetadataResponseTopic>(topicCount);
                    for (var i = 0; i < topicCount; i++)
                    {
                        var errorCode = UnpackInt16(reader);
                        var topicName = UnpackString16(reader);
                        var partitionCount = UnpackInt32(reader);

                        var partitions = new List<TopicMetdataResponseTopicPartition>(partitionCount);
                        for (var j = 0; j < partitionCount; j++)
                        {
                            var partitionErrorCode = UnpackInt16(reader);
                            var partitionId = UnpackInt32(reader);
                            var leaderId = UnpackInt32(reader);

                            var replicaCount = UnpackInt32(reader);
                            var replicas = new List<int>(replicaCount);
                            for (var k = 0; k < replicaCount; k++)
                            {
                                replicas.Add(UnpackInt32(reader));
                            }

                            var isrCount = UnpackInt32(reader);
                            var isrs = new List<int>(isrCount);
                            for (var k = 0; k < isrCount; k++)
                            {
                                isrs.Add(UnpackInt32(reader));
                            }

                            var partition = new TopicMetdataResponseTopicPartition { ErrorCode = (ErrorResponseCode)partitionErrorCode, PartitionId = partitionId, LeaderId = leaderId, Replicas = replicas, Isr = isrs };
                            partitions.Add(partition);
                        }
                        var topic = new TopicMetadataResponseTopic { ErrorCode = errorCode, TopicName = topicName, Partitions = partitions};
                        topics.Add(topic);
                    }

                    return new TopicMetadataResponse { Brokers = brokers, Topics = topics };
                }
            }            
        }

        private static ProduceResponse UnpackProduceResponse(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var correlationId = UnpackInt32(reader);
                    
                    var topicCount = UnpackInt32(reader);
                    var topics = new List<ProduceResponseTopic>(topicCount);

                    for (int i = 0; i < topicCount; i++)
                    {
                        var topicName = UnpackString16(reader);
                        var partitionCount = UnpackInt32(reader);

                        var partitions = new List<ProduceResponseTopicPartition>(partitionCount);

                        for (int j = 0; j < partitionCount; j++)
                        {
                            var partitionId = UnpackInt32(reader);
                            var error = UnpackInt16(reader);
                            var offset = UnpackInt64(reader);

                            var partition = new ProduceResponseTopicPartition
                            {                                
                                PartitionId = partitionId,
                                Error = (ErrorResponseCode)error,
                                Offset = offset
                            };

                            partitions.Add(partition);
                        }

                        var topic = new ProduceResponseTopic { TopicName = topicName, Partitions = partitions };
                        topics.Add(topic);
                    }

                    return new ProduceResponse { Topics = topics};
                }
            }
        }

        private static void Send(NetworkStream stream, byte[] requestData)
        {
            var requestSize = requestData.Length;
            var requestSizeData = PackInt32(requestSize);

            var buffer = new List<byte>(requestSizeData.Length + requestData.Length);
            buffer.AddRange(requestSizeData);
            buffer.AddRange(requestData);
            var data = buffer.ToArray();

            stream.Write(data, 0, data.Length);
        }

        private static byte[] Receive(NetworkStream stream)
        {
            var responseSizeData = new byte[4];
            while (stream.Read(responseSizeData, 0, responseSizeData.Length) == 0)
            {
                Thread.Sleep(1000);
            }
            var responseSize = UnpackInt32(responseSizeData);

            var responseData = new byte[responseSize];
            stream.Read(responseData, 0, responseData.Length);
            return responseData;
        }

        private static MessageSet CreateMessageSet(IReadOnlyList<Message> messages, MessageCodec codec = MessageCodec.CodecNone)
        {
            var messageAndOffsets = new List<MessageAndOffset>(messages.Count);
            foreach (var message in messages)
            {
                var messageAndOffset = new MessageAndOffset { Message = message, Offset = DefaultMessageOffset };
                messageAndOffsets.Add(messageAndOffset);
            }
            return new MessageSet { Messages = messageAndOffsets };
        }

        public void Test(string host, int port, string testTopicName)
        {
            using (var tcpClient = new TcpClient())
            {
                tcpClient.Connect(host, port);
                var stream = tcpClient.GetStream();
                              
                var groupId = "group8";

                var metadataRequest = new TopicMetadataRequest { TopicNames = new[] { testTopicName } };
                Send(stream, PackMetadataRequest(metadataRequest));
                var metadata = UnpackMetadata(Receive(stream));
                var partitionId = metadata.Topics[0].Partitions[0].PartitionId;
                var partitionIds = new List<int>(metadata.Topics[0].Partitions.Count);
                foreach (var partition in metadata.Topics[0].Partitions)
                {
                    partitionIds.Add(partition.PartitionId);
                }

                //var messageText = DateTime.Now.ToString();
                //var message0 = new Message { Key = Encoding.UTF8.GetBytes("key"), Value = Encoding.UTF8.GetBytes(messageText) };
                //var message1 = new Message {Key = Encoding.UTF8.GetBytes("1"), Value = Encoding.UTF8.GetBytes("12345")};
                //var message2 = new Message { Key = Encoding.UTF8.GetBytes("2"), Value = Encoding.UTF8.GetBytes("Вышел зайчик погулять") };
                //var messageSet0 = CreateMessageSet(new[] { message0 }, MessageCodec.CodecNone);
                //var messageSet1 = CreateMessageSet(new[] { message1, message2 }, MessageCodec.CodecNone);
                //var partitionPackage0 = new ProduceRequestTopicPartition { PartitionId = partitionIds[0], MessageSet = messageSet0 };
                //var partitionPackage1 = new ProduceRequestTopicPartition { PartitionId = partitionIds[1], MessageSet = messageSet1 };
                //var topicPackage = new ProduceRequestTopic { TopicName = testTopicName, Partitions = new[] { partitionPackage0, partitionPackage1 } };
                //var produceRequest = new ProduceRequest { AckMode = AckMode.WrittenToLeader, Timeout = new KafkaTimeout(TimeSpan.FromSeconds(5)), Topics = new[] { topicPackage } };
                //Send(stream, PackProduceRequest(produceRequest));
                //var produceResponse = UnpackProduceResponse(Receive(stream));                   

                var groupCoordinatorRequest = new GroupCoordinatorRequest { GroupId = groupId };
                Send(stream, PackGroupCoordinatorRequest(groupCoordinatorRequest));
                var groupCoordinatorResponse = UnpackGroupCoordinatorResponse(Receive(stream));

                var offsetRequestTopicPartition0 = new OffsetRequestTopicPartition { PartitionId = partitionIds[0], Time = OffsetTime.TheEarliest, MaxNumberOfOffsets = 1000 };
                var offsetRequestTopicPartition1 = new OffsetRequestTopicPartition { PartitionId = partitionIds[1], Time = OffsetTime.TheEarliest, MaxNumberOfOffsets = 1000 };
                var offsetRequestTopic = new OffsetRequestTopic { TopicName = testTopicName, Partitions = new[] { offsetRequestTopicPartition0, offsetRequestTopicPartition1 } };
                var offsetRequest = new OffsetRequest { Topics = new[] { offsetRequestTopic } };
                Send(stream, PackOffsetRequest(offsetRequest));
                var offsetResponse = UnpackOffsetResponse(Receive(stream));
                var offset0 = offsetResponse.Topics[0].Partitions[0].Offsets.Min();
                var offset1 = offsetResponse.Topics[0].Partitions[1].Offsets.Min();

                //var joinGroupRequest = new JoinGroupRequest
                //{
                //    GroupId = groupId,
                //    MemberId = "",
                //    SessionTimeout = new KafkaTimeout(TimeSpan.FromSeconds(30)),                    
                //    Protocols = new []
                //    {
                //        new JoinGroupRequestProtocol
                //        {
                //            ProtocolName = "my_consuming",
                //            Version = 1,
                //            TopicNames = new [] { testTopicName },
                //            AssignmentStrategies = new []
                //            {
                //                PartitionAssignmentStrategy.Range.ToString().ToLower(),
                //                PartitionAssignmentStrategy.RoundRobin.ToString().ToLower()                                
                //            },
                //            CustomData = Encoding.UTF8.GetBytes("data=100")                       
                //        }
                //    }
                //};
                //Send(stream, PackJoinGroupRequest(joinGroupRequest));
                //var joinGroupResponse = UnpackJoinGroupResponse(Receive(stream));
                //var groupGenerationId = joinGroupResponse.GroupGenerationId;
                //var memberId = joinGroupResponse.MemberId;

                //var syncGroupRequest = new SyncGroupRequest
                //{
                //    GroupId = groupId,
                //    GroupGenerationId = groupGenerationId,
                //    MemberId = memberId,
                //    Members = new[]
                //    {
                //        new SyncGroupRequestMember
                //        {
                //            MemberId = memberId,
                //            AssignedTopics = new[]
                //            {
                //                new SyncGroupRequestMemberTopic
                //                {
                //                    TopicName = testTopicName,
                //                    PartitionIds = partitionIds
                //                }
                //            }
                //        }
                //    }
                //};
                //Send(stream, PackSyncGroupReuest(syncGroupRequest));
                //var syncGroupResponse = UnpackSyncGroupResponse(Receive(stream));

                //var heartbeatRequest = new HeartbeatRequest
                //{
                //    GroupId = groupId,
                //    GroupGenerationId = groupGenerationId,
                //    MemberId = memberId,
                //};
                //Send(stream, PackHeartbeatReuest(heartbeatRequest));
                //var heartbeatResponse = UnpackHeartbeatResponse(Receive(stream));

                //var offsetFetchRequestTopic = new OffsetFetchRequestTopic { TopicName = testTopicName, PartitionIds = new[] { partitionId } };
                //var offsetFetchRequest = new OffsetFetchRequest { GroupId = groupId, Topics = new[] { offsetFetchRequestTopic } };
                //Send(stream, PackOffsetFetchRequest(offsetFetchRequest));
                //var offsetFetchResponse = UnpackOffsetFetchResponse(Receive(stream));
                //offset = Math.Max(offset, offsetFetchResponse.Topics[0].Partitions[0].Offset + 1);

                var fetchRequestTopicPartition0 = new FetchRequestTopicPartition { PartitionId = partitionIds[0], FetchOffset = offset0, MaxBytes = 100000 };
                var fetchRequestTopicPartition1 = new FetchRequestTopicPartition { PartitionId = partitionIds[1], FetchOffset = offset1, MaxBytes = 100000 };
                var fetchRequestTopic = new FetchRequestTopic { TopicName = testTopicName, Partitions = new[] { fetchRequestTopicPartition0, fetchRequestTopicPartition1 } };
                var fetchRequest = new FetchRequest { ReplicaId = ReplicaId.AnyReplica, MaxWaitTimeMs = 10000, MinBytes = 1, Topics = new[] { fetchRequestTopic } };
                Send(stream, PackFetchRequest(fetchRequest));
                var fetchResponse = UnpackFetchResponse(Receive(stream));

                long? maxOffset = null;
                foreach (var message in fetchResponse.Topics[0].Partitions[0].MessageSet.Messages)
                {
                    var text = string.Format("{0}: {1}", message.Offset, Encoding.UTF8.GetString(message.Message.Value));
                    maxOffset = message.Offset;
                    Console.WriteLine(text);
                }

                foreach (var message in fetchResponse.Topics[0].Partitions[1].MessageSet.Messages)
                {
                    var text = string.Format("{0}: {1}", message.Offset, Encoding.UTF8.GetString(message.Message.Value));
                    maxOffset = message.Offset;
                    Console.WriteLine(text);
                }

                //if (maxOffset.HasValue)
                //{
                //    var offsetCommitRequestTopicPartition = new OffsetCommitRequestTopicPartition { PartitionId = partitionId, Offset = maxOffset.Value, Metadata = "test" };
                //    var offsetCommitRequestTopic = new OffsetCommitRequestTopic { TopicName = testTopicName, Partitions = new[] { offsetCommitRequestTopicPartition } };
                //    var offsetCommitRequest = new OffsetCommitRequest
                //    {
                //        GroupId = groupId,
                //        GroupGenerationId = groupGenerationId,
                //        MemberId = memberId,                        
                //        Topics = new[] {offsetCommitRequestTopic}
                //    };
                //    Send(stream, PackOffsetCommitRequestV2(offsetCommitRequest));
                //    var offsetCommitResponse = UnpackOffsetCommitResponse(Receive(stream));
                //}

                //var leaveGroupRequest = new LeaveGroupRequest { GroupId = groupId, MemberId = memberId};
                //Send(stream, PackLeaveGroupRequest(leaveGroupRequest));
                //var liveGroupResponse = UnpackLeaveGroupResponse(Receive(stream));

                tcpClient.Close();
            }
        }
    }
}