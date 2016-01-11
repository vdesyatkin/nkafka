using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignmentStrategyInfo
    {        
        public readonly string StrategyName;

        public IKafkaConsumerAssignmentStrategy Strategy;

        public KafkaConsumerAssignmentStrategyInfo(     
            [NotNull] string strategyName, 
            [NotNull] IKafkaConsumerAssignmentStrategy strategy)
        {            
            StrategyName = strategyName;
            Strategy = strategy;
        }
    }
}
