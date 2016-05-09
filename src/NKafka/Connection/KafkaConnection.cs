using System;
using System.Net.Sockets;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Connection
{
    internal sealed class KafkaConnection
    {
        [NotNull]
        private readonly string _host;
        private readonly int _port;

        [CanBeNull]
        private TcpClient _tcpClient;

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
                WaitHandle.WaitAny(new[] { asyncConnectResult.AsyncWaitHandle, cancellation.WaitHandle });
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

        public bool TryWrite([NotNull] byte[] data, int offset, int length)
        {
            //todo (E002) socket errors
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return false;

                stream.Write(data, offset, length);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        [CanBeNull]
        public int? TryRead([NotNull] byte[] data, int offset, int length)
        {
            //todo (E002) socket errors
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return null;

                return stream.Read(data, offset, length);
            }
            catch (Exception)
            {
                return null;
            }
        }

        [CanBeNull]
        public IAsyncResult TryBeginRead([NotNull] byte[] data, int offset, int length, AsyncCallback callback, object state = null)
        {
            //todo (E002) socket errors
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return null;

                return stream.BeginRead(data, offset, length, callback, state);
            }
            catch (Exception)
            {
                return null;
            }
        }

        [CanBeNull]
        public int? TryEndRead(IAsyncResult asyncResult)
        {
            //todo (E002) socket errors
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return null;

                return stream.EndRead(asyncResult);
            }
            catch (Exception)
            {
                return null;
            }
        }

        public bool IsDataAvailable()
        {
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return false;

                return stream.DataAvailable;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}