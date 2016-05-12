using System.Net.Sockets;
using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConnectionSocketErrorInfo
    {
        public readonly SocketError? SocketErrorCode;

        public readonly int ErrorCode;

        public readonly int? NativeErrorCode;

        public KafkaConnectionSocketErrorInfo(SocketError? socketErrorCode, int errorCode, int? nativeErrorCode)
        {
            SocketErrorCode = socketErrorCode;
            ErrorCode = errorCode;
            NativeErrorCode = nativeErrorCode;
        }
    }
}
