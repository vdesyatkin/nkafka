using System;
using System.IO;
using System.Net.Sockets;
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

        public bool TryOpen() //todo errors
        {                       
            try
            {
                var tcpClient = new TcpClient();
                tcpClient.Connect(_host, _port);
                _tcpClient = tcpClient;
                return true;
            }
            catch (Exception)
            {
                //todo errors
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
        public Stream GetStream()
        {
            try
            {                
                return _tcpClient?.GetStream();
            }
            catch (Exception)
            {
                //todo errors
                return null;
            }
        }
    }
}
