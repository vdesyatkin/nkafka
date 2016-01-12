using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;
using NKafka.Connection;
using NKafka.Protocol.API.JoinGroup;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaCoordinatorGroup> _groups;

        private readonly TimeSpan _coordinatorClientTimeout;

        public KafkaCoordinatorBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _groups = new ConcurrentDictionary<string, KafkaCoordinatorGroup>();            
            _coordinatorClientTimeout = consumePeriod + TimeSpan.FromSeconds(1) + consumePeriod;
        }

        public void RemoveGroup([NotNull] string groupName)
        {
            KafkaCoordinatorGroup group;
            _groups.TryRemove(groupName, out group);
        }

        public void AddGroup([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup groupCoordinator)
        {
            _groups[groupName] = groupCoordinator;
        }

        public void Process()
        {
            foreach (var groupPair in _groups)
            {
                var groupName = groupPair.Key;
                var group = groupPair.Value;
                ProcessGroup(groupName, group);
            }
        }

        private void ProcessGroup([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup group)
        {
            if (group.Status == KafkaCoordinatorGroupStatus.RearrageRequired)
            {
                return;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.NotInitialized)
            {
                var topics = group.Topics;
                if (topics.Count == 0) return;

                foreach (var topic in topics)
                {
                    if (topic.Status != KafkaClientTopicStatus.Ready) return;
                }


            }
        }

        #region JoinGroup

        private KafkaJoinGroupRequest CreateJoinGroupRequest([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup group)
        {
            var topics = group.Topics;
            if (topics.Count == 0) return null;

            var topicNames = new List<string>(topics.Count);
            foreach (var topic in topics)
            {
                topicNames.Add(topic.TopicName);
            }

            var settingsProtocols = group.Settings.Protocols;
            if (settingsProtocols == null || settingsProtocols.Count == 0) return null;

            var protocols = new List<KafkaJoinGroupRequestProtocol>(settingsProtocols.Count);
            foreach (var settingsProtocol in settingsProtocols)
            {
                var protocolName = settingsProtocol?.ProtocolName;
                if (string.IsNullOrEmpty(protocolName)) continue;                

                var settingsAssignmentStrategies = settingsProtocol.AssignmentStrategies;
                if (settingsAssignmentStrategies == null || settingsAssignmentStrategies.Count == 0)
                {
                    continue;
                }

                var assignmentStrategies = new List<string>(settingsAssignmentStrategies.Count);
                foreach (var settingsAssignmentStrategy in settingsAssignmentStrategies)
                {                    
                    var strategyName = settingsAssignmentStrategy?.StrategyName;
                    if (string.IsNullOrEmpty(strategyName)) continue;
                    assignmentStrategies.Add(settingsAssignmentStrategy.StrategyName);
                }
                if (assignmentStrategies.Count == 0)
                {
                    continue;
                }

                var protocolVersion = settingsProtocol.ProtocolVersion;
                var customData = settingsProtocol.CustomData;

                var protocol = new KafkaJoinGroupRequestProtocol(protocolName, protocolVersion, topicNames, assignmentStrategies, customData);
                protocols.Add(protocol);
            }

            if (protocols.Count == 0) return null;
            
            var request = new KafkaJoinGroupRequest(groupName, group.MemberId, group.Settings.GroupSessionTimeout, protocols);
            return request;
        }

        #endregion JoinGroup
    }
}
