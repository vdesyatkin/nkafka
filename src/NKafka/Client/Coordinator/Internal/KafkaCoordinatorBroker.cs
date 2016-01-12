using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;
using NKafka.Connection;
using NKafka.Protocol.API.JoinGroup;

namespace NKafka.Client.Coordinator.Internal
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

            //todo (C004) session timeout, protocols
            var request = new KafkaJoinGroupRequest(groupName, group.MemberId, TimeSpan.Zero, null);
            return request;
        }

        #endregion JoinGroup
    }
}
