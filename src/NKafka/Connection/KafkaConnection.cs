using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Connection.Logging;

namespace NKafka.Connection
{
    internal sealed class KafkaConnection
    {
        [NotNull] private readonly string _host;
        private readonly int _port;

        [CanBeNull] private TcpClient _tcpClient;

        private bool _isConnectinMaintenance;

        public KafkaConnection([NotNull] string host, int port)
        {
            _host = host;
            _port = port;
        }

        /// <exception cref="KafkaConnectionException"/>
        public void Open(CancellationToken cancellation)
        {
            try
            {
                _isConnectinMaintenance = true;

                var tcpClient = new TcpClient();
                var asyncConnectResult = tcpClient.BeginConnect(_host, _port, null, null);
                WaitHandle.WaitAny(new[] {asyncConnectResult.AsyncWaitHandle, cancellation.WaitHandle});
                if (cancellation.IsCancellationRequested)
                {
                    throw new KafkaConnectionException(KafkaConnectionErrorCode.Cancelled);
                }
                if (!asyncConnectResult.IsCompleted)
                {
                    throw new KafkaConnectionException(KafkaConnectionErrorCode.ClientTimeout);
                }

                tcpClient.EndConnect(asyncConnectResult);
                _tcpClient = tcpClient;
            }
            catch (KafkaConnectionException)
            {
                throw;
            }
            catch (SocketException socketException)
            {
                if (socketException.SocketErrorCode == SocketError.IsConnected)
                {
                    return;
                }

                throw ConvertException(socketException);
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
            finally
            {
                _isConnectinMaintenance = false;
            }
        }
        
        /// <exception cref="KafkaConnectionException"/>
        public void Close()
        {
            _isConnectinMaintenance = true;

            var tcpClient = _tcpClient;
            _tcpClient = null;

            try
            {                
                tcpClient?.Close();
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
            finally
            {                
                _isConnectinMaintenance = false;
            }            
        }
        
        public void Reopen(CancellationToken cancellation)
        {
            _isConnectinMaintenance = true;

            var tcpClient = _tcpClient;            
            _tcpClient = null;

            try
            {
                tcpClient?.Close();             
            }
            catch (Exception)
            {
                //ignored
            }
                                
            Open(cancellation);
            GetStream();

            _isConnectinMaintenance = false;
        }

        /// <exception cref="KafkaConnectionException"/>
        [PublicAPI]
        public void Write([NotNull] byte[] data, int offset, int length)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                stream.Write(data, offset, length);
            }
            catch (KafkaConnectionException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        public void BeginWrite([NotNull] byte[] data, int offset, int length,
            [CanBeNull] AsyncCallback callback, [CanBeNull] object state = null)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                stream.BeginWrite(data, offset, length, callback, state);
            }
            catch (KafkaConnectionException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        public void EndWrite([NotNull] IAsyncResult asyncResult)
        {
            if (asyncResult == null)
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                stream.EndWrite(asyncResult);
            }
            catch (KafkaConnectionException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        [PublicAPI]
        public int Read([NotNull] byte[] data, int offset, int length)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                return stream.Read(data, offset, length);
            }
            catch (KafkaConnectionException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        public void BeginRead([NotNull] byte[] data, int offset, int length,
            [CanBeNull] AsyncCallback callback, [CanBeNull] object state = null)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                stream.BeginRead(data, offset, length, callback, state);
            }
            catch (KafkaConnectionException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        public int EndRead([NotNull] IAsyncResult asyncResult)
        {
            if (asyncResult == null)
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                return stream.EndRead(asyncResult);
            }
            catch (KafkaConnectionException)
            {                
                throw;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        public bool IsDataAvailable()
        {
            try
            {
                var stream = _tcpClient?.GetStream();
                if (stream == null)
                {
                    return false;
                }

                return stream.DataAvailable;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }
        }

        [NotNull]
        private NetworkStream GetStream()
        {
            NetworkStream stream;
            try
            {
                stream = _tcpClient?.GetStream();
            }
            catch (ObjectDisposedException)
            {
                throw new KafkaConnectionException(KafkaConnectionErrorCode.ConnectionMaintenance);
            }

            if (stream == null)
            {
                if (_isConnectinMaintenance)
                {
                    throw new KafkaConnectionException(KafkaConnectionErrorCode.ConnectionMaintenance);
                }
                throw new KafkaConnectionException(KafkaConnectionErrorCode.ConnectionClosed);
            }

            return stream;
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

        [NotNull]
        private KafkaConnectionException ConvertException([NotNull] Exception exception)
        {
            var socketException = exception as SocketException;
            if (socketException != null)
            {
                var socketErrorInfo = new KafkaConnectionSocketErrorInfo(socketException.SocketErrorCode, socketException.ErrorCode, socketException.NativeErrorCode);
                return new KafkaConnectionException(ConvertError(socketException.SocketErrorCode), socketException, socketErrorInfo);
            }

            if (exception is SecurityException)
            {
                return new KafkaConnectionException(KafkaConnectionErrorCode.NotAuthorized, exception);
            }

            if (exception is IOException)
            {
                return new KafkaConnectionException(KafkaConnectionErrorCode.TransportError, exception);
            }

            if (exception is ObjectDisposedException)
            {
                return new KafkaConnectionException(KafkaConnectionErrorCode.ConnectionMaintenance, exception);
            }

            if (exception is InvalidOperationException)
            {
                return new KafkaConnectionException(KafkaConnectionErrorCode.UnsupportedOperation, exception);
            }

            if (exception is ArgumentException)
            {
                return new KafkaConnectionException(KafkaConnectionErrorCode.UnsupportedOperation, exception);
            }

            if (exception is NotSupportedException)
            {
                return new KafkaConnectionException(KafkaConnectionErrorCode.UnsupportedOperation, exception);
            }

            return new KafkaConnectionException(KafkaConnectionErrorCode.UnknownError, exception);
        }

        private static KafkaConnectionErrorCode ConvertError(SocketError socketError)
        {
            switch (socketError)
            {
                case SocketError.Success:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.SocketError:
                    return KafkaConnectionErrorCode.TransportError;
                case SocketError.Interrupted:
                    return KafkaConnectionErrorCode.Cancelled;
                case SocketError.AccessDenied:
                    return KafkaConnectionErrorCode.NotAuthorized;
                case SocketError.Fault:
                    return KafkaConnectionErrorCode.TransportError;
                case SocketError.InvalidArgument:
                    return KafkaConnectionErrorCode.InvalidHost;
                case SocketError.TooManyOpenSockets:
                    return KafkaConnectionErrorCode.ConnectionNotAllowed;
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
                    return KafkaConnectionErrorCode.ConnectionNotAllowed;
                case SocketError.AddressNotAvailable:
                    return KafkaConnectionErrorCode.HostNotAvailable;
                case SocketError.NetworkDown:
                    return KafkaConnectionErrorCode.NetworkNotAvailable;
                case SocketError.NetworkUnreachable:
                    return KafkaConnectionErrorCode.HostUnreachable;
                case SocketError.NetworkReset:
                    return KafkaConnectionErrorCode.NetworkNotAvailable;
                case SocketError.ConnectionAborted:
                    return KafkaConnectionErrorCode.ConnectionRefused;
                case SocketError.ConnectionReset:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.NoBufferSpaceAvailable:
                    return KafkaConnectionErrorCode.ConnectionNotAllowed;
                case SocketError.IsConnected:
                    return KafkaConnectionErrorCode.ConnectionMaintenance;
                case SocketError.NotConnected:
                    return KafkaConnectionErrorCode.ConnectionClosed;
                case SocketError.Shutdown:
                    return KafkaConnectionErrorCode.ConnectionClosed;
                case SocketError.TimedOut:
                    return KafkaConnectionErrorCode.ClientTimeout;
                case SocketError.ConnectionRefused:
                    return KafkaConnectionErrorCode.ConnectionRefused;
                case SocketError.HostDown:
                    return KafkaConnectionErrorCode.HostNotAvailable;
                case SocketError.HostUnreachable:
                    return KafkaConnectionErrorCode.HostUnreachable;
                case SocketError.ProcessLimit:
                    return KafkaConnectionErrorCode.ConnectionNotAllowed;
                case SocketError.SystemNotReady:
                    return KafkaConnectionErrorCode.NetworkNotAvailable;
                case SocketError.VersionNotSupported:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.NotInitialized:
                    return KafkaConnectionErrorCode.ConnectionNotAllowed;
                case SocketError.Disconnecting:
                    return KafkaConnectionErrorCode.ConnectionClosed;
                case SocketError.TypeNotFound:
                    return KafkaConnectionErrorCode.UnsupportedHost;
                case SocketError.HostNotFound:
                    return KafkaConnectionErrorCode.HostUnreachable;
                case SocketError.TryAgain:
                    return KafkaConnectionErrorCode.ClientTimeout;
                case SocketError.NoRecovery:
                    return KafkaConnectionErrorCode.HostUnreachable;
                case SocketError.NoData:
                    return KafkaConnectionErrorCode.HostUnreachable;
                case SocketError.IOPending:
                    return KafkaConnectionErrorCode.TransportError;
                case SocketError.OperationAborted:
                    return KafkaConnectionErrorCode.OperationRefused;
                default:
                    return KafkaConnectionErrorCode.UnknownError;
            }
        }
    }
}