using System;
using System.Net.Sockets;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Connection
{
    internal sealed class KafkaConnection
    {
        [NotNull] private readonly string _host;
        private readonly int _port;

        [CanBeNull] private TcpClient _tcpClient;

        public KafkaConnection([NotNull] string host, int port)
        {
            _host = host;
            _port = port;            
        }

        public bool TryOpen(CancellationToken cancellation) //todo (E002) socket errors
        {                       
            try
            {
                var tcpClient = new TcpClient();
                var asyncConnectResult = tcpClient.BeginConnect(_host, _port, null, null);
                WaitHandle.WaitAny(new[] {asyncConnectResult.AsyncWaitHandle, cancellation.WaitHandle});
                if (cancellation.IsCancellationRequested)
                {
                    return false;
                }
                if (!asyncConnectResult.IsCompleted)
                {
                    return false;                    
                }

                tcpClient.EndConnect(asyncConnectResult);
                _tcpClient = tcpClient;
                return true;
            }
            catch (Exception)
            {
                //todo (E002) socket errors
                return false;
            }
        }

        public void Close()
        {
            try
            {
                _tcpClient?.Close();
            }
            catch (Exception)
            {
                //ignored
            }
            _tcpClient = null;
        }

        [CanBeNull]
        public NetworkStream GetStream()
        {
            try
            {
                return _tcpClient?.GetStream();
            }            
            catch (Exception)
            {
                //todo (E002) socket errors
                return null;
            }
        }
    }
}
