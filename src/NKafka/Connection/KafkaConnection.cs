using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security;
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

        public KafkaConnectionResult<bool> TryOpen(CancellationToken cancellation)
        {
            try
            {
                var tcpClient = new TcpClient();
                var asyncConnectResult = tcpClient.BeginConnect(_host, _port, null, null);
                WaitHandle.WaitAny(new[] {asyncConnectResult.AsyncWaitHandle, cancellation.WaitHandle});
                if (cancellation.IsCancellationRequested)
                {
                    return KafkaConnectionErrorCode.Cancelled;
                }
                if (!asyncConnectResult.IsCompleted)
                {
                    return KafkaConnectionErrorCode.ClientTimeout;
                }

                tcpClient.EndConnect(asyncConnectResult);
                _tcpClient = tcpClient;
                return true;
            }
            catch (SocketException socketException)
            {
                if (socketException.SocketErrorCode == SocketError.Success ||
                    socketException.SocketErrorCode == SocketError.IsConnected)
                {
                    return true;
                }

                return ConvertException(socketException);
            }
            catch (Exception exception)
            {
                return ConvertException(exception);
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
                //todo (E013) connection close
                //ignored
            }
            _tcpClient = null;
        }

        public KafkaConnectionResult<bool> TryWrite([NotNull] byte[] data, int offset, int length)
        {
            if (!CheckBufferData(data, offset, length))
            {
                return KafkaConnectionErrorCode.BadRequest;
            }
            
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return KafkaConnectionErrorCode.ConnectionClosed;

                stream.Write(data, offset, length);
                return true;
            }
            catch (Exception exception)
            {
                return ConvertException(exception);
            }
        }
        
        public KafkaConnectionResult<int> TryRead([NotNull] byte[] data, int offset, int length)
        {
            if (!CheckBufferData(data, offset, length))
            {
                return KafkaConnectionErrorCode.BadRequest;
            }
            
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return KafkaConnectionErrorCode.ConnectionClosed;

                return stream.Read(data, offset, length);
            }
            catch (Exception exception)
            {
                return ConvertException(exception);
            }
        }
        
        public KafkaConnectionResult<IAsyncResult> TryBeginRead([NotNull] byte[] data, int offset, int length, AsyncCallback callback, object state = null)
        {
            if (!CheckBufferData(data, offset, length))
            {
                return KafkaConnectionErrorCode.BadRequest;
            }

            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return KafkaConnectionErrorCode.ConnectionClosed;

                var asyncResult = stream.BeginRead(data, offset, length, callback, state);
                return new KafkaConnectionResult<IAsyncResult>(asyncResult, null);
            }
            catch (Exception exception)
            {
                return ConvertException(exception);
            }
        }
        
        public KafkaConnectionResult<int> TryEndRead(IAsyncResult asyncResult)
        {
            if (asyncResult == null)
            {
                return KafkaConnectionErrorCode.BadRequest;
            }
            
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null) return KafkaConnectionErrorCode.ConnectionClosed;

                return stream.EndRead(asyncResult);
            }
            catch (Exception exception)
            {
                return ConvertException(exception);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CheckBufferData(byte[] data, int offset, int length)
        {
            if (data == null || data.Length == 0 ||
                offset < 0 || offset >= data.Length ||
                length <= 0 || length > data.Length - offset)
            {
                return false;
            }

            return true;
        }

        private KafkaConnectionErrorCode ConvertException(Exception exception) //todo (E013) connection log context
        {
            var socketException = exception as SocketException;
            if (socketException != null)
            {
                //todo (E013) connection socket error
                return ConvertError(socketException.SocketErrorCode);
            }

            var securityException = exception as SecurityException;            
            if (securityException != null)
            {
                //todo (E013) connection security error
                return KafkaConnectionErrorCode.NotAuthorized;
            }

            //todo (E013) connection custom error

            if (exception is IOException)
            {                
                return KafkaConnectionErrorCode.TransportError;
            }

            if (exception is ObjectDisposedException)
            {
                return KafkaConnectionErrorCode.ConnectionMaintenance;
            }

            if (exception is InvalidOperationException)
            {
                return KafkaConnectionErrorCode.UnsupportedOperation;
            }                       

            if (exception is ArgumentException)
            {
                return KafkaConnectionErrorCode.UnsupportedOperation;
            }

            if (exception is NotSupportedException)
            {
                return KafkaConnectionErrorCode.UnsupportedOperation;
            }
            
            return KafkaConnectionErrorCode.UnknownError;
        }

        private static KafkaConnectionErrorCode ConvertError(SocketError socketError)
        {
            switch (socketError)
            {
                case SocketError.Success:
                    return KafkaConnectionErrorCode.UnknownError; //wtf?
                case SocketError.SocketError:
                    return KafkaConnectionErrorCode.TransportError;
                case SocketError.Interrupted:
                    return KafkaConnectionErrorCode.Cancelled;
                case SocketError.AccessDenied:
                    return KafkaConnectionErrorCode.NotAuthorized;
                case SocketError.Fault:
                    break;  //todo (E002)
                case SocketError.InvalidArgument:
                    return KafkaConnectionErrorCode.InvalidHost;                    
                case SocketError.TooManyOpenSockets:
                    break;  //todo (E002)
                case SocketError.WouldBlock:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.InProgress:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.AlreadyInProgress:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.NotSocket:
                    return KafkaConnectionErrorCode.InvalidHost;
                case SocketError.DestinationAddressRequired:
                    return KafkaConnectionErrorCode.InvalidHost;
                case SocketError.MessageSize:
                    return KafkaConnectionErrorCode.TooBigMessage;
                case SocketError.ProtocolType:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.ProtocolOption:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.ProtocolNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.SocketNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.OperationNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedOperation;
                case SocketError.ProtocolFamilyNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.AddressFamilyNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.AddressAlreadyInUse:
                    break;  //todo (E002)
                case SocketError.AddressNotAvailable:
                    return KafkaConnectionErrorCode.HostNotAvailable;
                case SocketError.NetworkDown:
                    return KafkaConnectionErrorCode.NetworkNotAvailable;
                case SocketError.NetworkUnreachable:
                    return KafkaConnectionErrorCode.NetworkNotAvailable;
                case SocketError.NetworkReset:
                    break;  //todo (E002)
                case SocketError.ConnectionAborted:
                    return KafkaConnectionErrorCode.ConnectionClosed;
                case SocketError.ConnectionReset:
                    break;  //todo (E002)
                case SocketError.NoBufferSpaceAvailable:
                    break;  //todo (E002)
                case SocketError.IsConnected:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.NotConnected:
                    return KafkaConnectionErrorCode.ConnectionClosed;
                case SocketError.Shutdown:
                    return KafkaConnectionErrorCode.ConnectionClosed;
                case SocketError.TimedOut:
                    return KafkaConnectionErrorCode.ClientTimeout;
                case SocketError.ConnectionRefused:
                    break;  //todo (E002)
                case SocketError.HostDown:
                    return KafkaConnectionErrorCode.HostNotAvailable;
                case SocketError.HostUnreachable:
                    return KafkaConnectionErrorCode.HostNotAvailable;
                case SocketError.ProcessLimit:
                    break;  //todo (E002)
                case SocketError.SystemNotReady:
                    return KafkaConnectionErrorCode.NetworkNotAvailable;
                case SocketError.VersionNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.NotInitialized:
                    break;  //todo (E002)
                case SocketError.Disconnecting:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.TypeNotFound:
                    break;  //todo (E002)
                case SocketError.HostNotFound:
                    break; //todo (E002)
                case SocketError.TryAgain:
                    return KafkaConnectionErrorCode.ClientTimeout;                    
                case SocketError.NoRecovery:
                    break;  //todo (E002)
                case SocketError.NoData:
                    break; //todo
                case SocketError.IOPending:
                    return KafkaConnectionErrorCode.TransportError;
                case SocketError.OperationAborted:
                    break;  //todo (E002)
                default:
                    return KafkaConnectionErrorCode.UnknownError;                    
            }

            return KafkaConnectionErrorCode.UnknownError;
        }
    }
}