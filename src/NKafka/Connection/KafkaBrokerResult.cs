namespace NKafka.Connection
{
    internal struct KafkaBrokerResult<TData>
    {
        public readonly bool HasData;        

        public readonly TData Data;

        public readonly KafkaBrokerErrorCode? Error;

        public bool HasError => Error != null;

        public KafkaBrokerResult(bool hasData, TData data, KafkaBrokerErrorCode? error)
        {
            Data = data;
            HasData = hasData;
            Error = error;            
        }

        public static implicit operator KafkaBrokerResult<TData>(TData data)
        {
            return new KafkaBrokerResult<TData>(data != null, data, null);
        }

        public static implicit operator KafkaBrokerResult<TData>(KafkaBrokerErrorCode errorCode)
        {
            return new KafkaBrokerResult<TData>(false, default(TData), errorCode);
        }

        public static implicit operator KafkaBrokerResult<TData>(KafkaBrokerErrorCode? errorCode)
        {
            return new KafkaBrokerResult<TData>(false, default(TData), errorCode);
        }
    }
}
