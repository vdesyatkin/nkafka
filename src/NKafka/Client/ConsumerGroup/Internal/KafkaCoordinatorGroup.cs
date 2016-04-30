﻿using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;
        [NotNull, ItemNotNull] public readonly IReadOnlyDictionary<string, KafkaClientTopic> Topics;
        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerGroupSettingsProtocol> Protocols;        

        public KafkaCoordinatorGroupStatus Status;
        private KafkaConsumerGroupErrorCode? _error;
        public DateTime ErrorTimestampUtc { get; private set; }

        [CanBeNull] public KafkaCoordinatorGroupSessionData SessionData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupProtocolData ProtocolData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupLeaderData LeaderData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupAssignmentData AssignmentData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupOffsetsData OffsetsData { get; private set; }

        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> TopicMetadataPartitionIds;
               
        public DateTime HeartbeatTimestampUtc;
        public TimeSpan HeartbeatPeriod { get; private set; }
        public TimeSpan? CustomSessionLifetime { get; private set; }

        public DateTime CommitTimestampUtc;
        public readonly TimeSpan CommitPeriod;
        [CanBeNull] public string CommitMetadata { get; private set; }

        public KafkaCoordinatorGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, 
            [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            TopicMetadataPartitionIds = new Dictionary<string, IReadOnlyList<int>>();            
            Settings = settings;            
            
            CommitPeriod = settings.OffsetCommitPeriod;            

            var topicsDictionary = new Dictionary<string, KafkaClientTopic>(topics.Count);
            foreach (var topic in topics)
            {
                topicsDictionary[topic.TopicName] = topic;
            }
            Topics = topicsDictionary;

            var protocols = new List<KafkaConsumerGroupSettingsProtocol>();
            var settingsProtocols = settings.Protocols;
            if (settingsProtocols != null)
            {
                foreach (var settingsProtocol in settingsProtocols)
                {
                    if (settingsProtocol?.ProtocolName == null) continue;
                    var settingsStrategies = settingsProtocol.AssignmentStrategies;
                    if (settingsStrategies == null || settingsStrategies.Count == 0)
                    {
                        continue;
                    }

                    var strategies = new List<KafkaConsumerAssignmentStrategyInfo>(settingsStrategies.Count);
                    foreach (var settingsStrategy in settingsStrategies)
                    {
                        if (settingsStrategy?.StrategyName == null || settingsStrategy.Strategy == null) continue;

                        strategies.Add(settingsStrategy);
                    }
                    if (strategies.Count == 0) continue;

                    var protocol = new KafkaConsumerGroupSettingsProtocol(settingsProtocol.ProtocolName,
                        settingsProtocol.ProtocolVersion, strategies, settingsProtocol.CustomData);
                    protocols.Add(protocol);
                }
            }

            if (protocols.Count == 0)
            {
                protocols.Add(KafkaConsumerGroupSettingsBuilder.DefaultProtocol);
            }
            Protocols = protocols;

            ResetSettings();
        }

        [CanBeNull] public IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> GetPartitionOffsets([NotNull] string topicName)
        {
            if (Status != KafkaCoordinatorGroupStatus.Ready) return null;

            var topics = OffsetsData?.Topics;
            if (topics == null) return null;

            KafkaCoordinatorGroupOffsetsDataTopic topic;
            if (topics.TryGetValue(topicName, out topic) && topic != null)
            {
                return topic.PartitionsReadOnly;
            }

            return null;
        }

        [NotNull]
        public KafkaConsumerGroupSessionInfo GetSessionDiagnosticsInfo()
        {
            var sessionData = SessionData;
            var assignmentData = AssignmentData;
            var offsetsData = OffsetsData;
            var protocolData = ProtocolData;
            var leaderData = LeaderData;

            var memberInfo = new KafkaConsumerGroupMemberInfo(
                sessionData?.GenerationId,
                sessionData?.MemberId,
                sessionData?.IsLeader ?? false,
                sessionData?.TimestampUtc ?? DateTime.UtcNow);

            KafkaConsumerGroupProtocolInfo protcolInfo = null;

            if (protocolData != null)
            {
                var protocolName = protocolData.ProtocolName;
                var protocolVersion = protocolData.ProtocolVersion;
                var protocolTimestampUtc = protocolData.TimestampUtc;
                string assignmentStrategyName = null;
                if (leaderData != null)
                {
                    assignmentStrategyName = leaderData.AssignmentStrategyName;
                    if (leaderData.TimestampUtc > protocolTimestampUtc)
                    {
                        protocolTimestampUtc = leaderData.TimestampUtc;
                    }
                }

                protcolInfo = new KafkaConsumerGroupProtocolInfo(protocolName, protocolVersion, assignmentStrategyName, protocolTimestampUtc);
            }

            KafkaConsumerGroupOffsetsInfo offsetsInfo = null;
            if (offsetsData != null)
            {
                var topicInfos = new List<KafkaConsumerGroupOffsetsTopicInfo>(offsetsData.Topics.Count);
                foreach (var topicPair in offsetsData.Topics)
                {
                    var topic = topicPair.Value;
                    var topicName = topicPair.Key;
                    if (topic == null || topicName == null) continue;                    

                    var partitionInfos = new List<KafkaConsumerGroupOffsetsPartitionInfo>(topic.Partitions.Count);
                    foreach (var partitionPair in topic.Partitions)
                    {
                        var partition = partitionPair.Value;
                        if (partition == null) continue;
                        var partitionId = partitionPair.Key;

                        var partitionInfo = new KafkaConsumerGroupOffsetsPartitionInfo(partitionId, 
                            partition.ClientOffset, partition.ServerOffset, partition.TimestampUtc);
                        partitionInfos.Add(partitionInfo);
                    }
                    var topicInfo = new KafkaConsumerGroupOffsetsTopicInfo(topicName, partitionInfos);
                    topicInfos.Add(topicInfo);
                }
                offsetsInfo = new KafkaConsumerGroupOffsetsInfo(topicInfos, offsetsData.TimestampUtc);
            }
            else
            {
                if (assignmentData != null)
                {
                    var topicInfos = new List<KafkaConsumerGroupOffsetsTopicInfo>(assignmentData.AssignedTopicPartitions.Count);
                    foreach (var topicPair in assignmentData.AssignedTopicPartitions)
                    {
                        var topicPartitions = topicPair.Value;
                        var topicName = topicPair.Key;
                        if (topicPartitions == null || topicName == null) continue;

                        var partitionInfos = new List<KafkaConsumerGroupOffsetsPartitionInfo>(topicPartitions.Count);
                        foreach (var partitionId in topicPartitions)
                        {                            

                            var partitionInfo = new KafkaConsumerGroupOffsetsPartitionInfo(partitionId,
                                null, null, assignmentData.TimestampUtc);
                            partitionInfos.Add(partitionInfo);
                        }
                        var topicInfo = new KafkaConsumerGroupOffsetsTopicInfo(topicName, partitionInfos);
                        topicInfos.Add(topicInfo);
                    }
                    offsetsInfo = new KafkaConsumerGroupOffsetsInfo(topicInfos, assignmentData.TimestampUtc);
                }
            }

            var status = KafkaConsumerGroupStatus.NotInitialized;
            switch (Status)
            {
                case KafkaCoordinatorGroupStatus.NotInitialized:
                    status = KafkaConsumerGroupStatus.NotInitialized;
                    break;
                case KafkaCoordinatorGroupStatus.RearrangeRequired:
                    status = KafkaConsumerGroupStatus.Rearrange;
                    break;
                case KafkaCoordinatorGroupStatus.JoinGroupRequested:
                    status = KafkaConsumerGroupStatus.JoinGroup;
                    break;
                case KafkaCoordinatorGroupStatus.JoinedAsMember:                    
                case KafkaCoordinatorGroupStatus.AdditionalTopicsRequired:
                case KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested:
                case KafkaCoordinatorGroupStatus.JoinedAsLeader:
                    status = KafkaConsumerGroupStatus.Assigning;
                    break;
                case KafkaCoordinatorGroupStatus.SyncGroupRequested:
                    status = KafkaConsumerGroupStatus.SyncGroup;
                    break;
                case KafkaCoordinatorGroupStatus.FirstHeartbeatRequired:
                case KafkaCoordinatorGroupStatus.FirstHeatbeatRequested:
                    status = KafkaConsumerGroupStatus.FirstHeatbeat;
                    break;
                case KafkaCoordinatorGroupStatus.OffsetFetchRequired:
                case KafkaCoordinatorGroupStatus.OffsetFetchRequested:
                    status = KafkaConsumerGroupStatus.OffsetsFilling;
                    break;
                case KafkaCoordinatorGroupStatus.Ready:
                    status = KafkaConsumerGroupStatus.Ready;
                    break;
                case KafkaCoordinatorGroupStatus.Error:
                    status = KafkaConsumerGroupStatus.Error;
                    break;
                case KafkaCoordinatorGroupStatus.Rebalance:
                    status = KafkaConsumerGroupStatus.Rebalance;
                    break;
            }

            return new KafkaConsumerGroupSessionInfo(GroupName, DateTime.UtcNow,
                status == KafkaConsumerGroupStatus.Ready,
                status, //todo 
                _error,
                ErrorTimestampUtc,
                memberInfo,
                protcolInfo,
                offsetsInfo
                );
        }

        public void SetError(KafkaConsumerGroupErrorCode errorCode)
        {
            ErrorTimestampUtc = DateTime.UtcNow;
            _error = errorCode;            
        }
        
        public void ResetError()
        {            
            _error = null;
        }

        public void SetSessionData(int generationId, string memberId, bool isLeader)
        {
            SessionData = new KafkaCoordinatorGroupSessionData(generationId, memberId, isLeader, DateTime.UtcNow);
        }        

        public void SetProtocolData(string protocolName, short? protocolVersion)
        {
            ProtocolData = new KafkaCoordinatorGroupProtocolData(protocolName, protocolVersion, DateTime.UtcNow);
        }

        public void SetLeaderData([CanBeNull] string assignmentStrategyName,
            [NotNull] IReadOnlyList<KafkaCoordinatorGroupMemberData> groupMembers,
            [NotNull] IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMemberData>> topicMembers,
            [NotNull] IReadOnlyList<string> additionalTopicNames)
        {
            LeaderData = new KafkaCoordinatorGroupLeaderData(assignmentStrategyName, groupMembers, topicMembers, additionalTopicNames, DateTime.UtcNow);
        }

        public void SetAssignmentData([NotNull] IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions)
        {
            AssignmentData = new KafkaCoordinatorGroupAssignmentData(topicPartitions, DateTime.UtcNow);
        }

        public void SetOffsetsData([NotNull] IReadOnlyDictionary<string, KafkaCoordinatorGroupOffsetsDataTopic> topics)
        {
            OffsetsData = new KafkaCoordinatorGroupOffsetsData(topics, DateTime.UtcNow);
        }

        public void SetSessionLifetime(TimeSpan sessionLifetime)
        {
            CustomSessionLifetime = sessionLifetime; 
            UpdateHeartbeatPeriod(sessionLifetime);
        }

        private void UpdateHeartbeatPeriod(TimeSpan sessionLifetime)
        {
            var heartbeatPeriod = TimeSpan.FromSeconds(sessionLifetime.TotalSeconds / 2 - 1);
            if (Settings.HeartbeatPeriod < heartbeatPeriod)
            {
                heartbeatPeriod = Settings.HeartbeatPeriod;
            }
            if (heartbeatPeriod < TimeSpan.FromSeconds(1))
            {
                heartbeatPeriod = TimeSpan.FromSeconds(1); //todo (E006)
            }
            HeartbeatPeriod = heartbeatPeriod;
        }

        public void SetCommitMetadata(string metadata)
        {
            CommitMetadata = metadata;
        }

        public void ResetSettings()
        {
            CustomSessionLifetime = null;
            UpdateHeartbeatPeriod(Settings.GroupSessionLifetime);
            CommitMetadata = Settings.OffsetCommitMetadata;            
        }

        public void ResetData()
        {
            SessionData = null;
            ProtocolData = null;
            LeaderData = null;
            AssignmentData = null;
            OffsetsData = null;
            TopicMetadataPartitionIds.Clear();
        }
    }
}