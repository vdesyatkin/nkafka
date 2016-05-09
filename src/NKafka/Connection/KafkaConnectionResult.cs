namespace NKafka.Connection
{
    internal struct KafkaConnectionResult<TData>
    {
        public readonly TData Result;

        public readonly KafkaConnectionErrorCode? Error;

        public KafkaConnectionResult(TData result, KafkaConnectionErrorCode? error)
        {
            Result = result;
            Error = error;
        }

        public static implicit operator KafkaConnectionResult<TData>(TData result)
        {
            return new KafkaConnectionResult<TData>(result, null);
        }

        public static implicit operator KafkaConnectionResult<TData>(KafkaConnectionErrorCode error)
        {
            return new KafkaConnectionResult<TData>(default(TData), error);
        }
    }
}
