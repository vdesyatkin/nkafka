using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
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
                var tcpClient = new TcpClient();                
                var asyncConnectTask = tcpClient.ConnectAsync(_host, _port);
                if (asyncConnectTask == null)
                {
                    throw new KafkaConnectionException(this, KafkaConnectionErrorCode.ConnectionNotAllowed);
                }
                asyncConnectTask.Wait(cancellation);
                
                if (cancellation.IsCancellationRequested)
                {
                    throw new KafkaConnectionException(this, KafkaConnectionErrorCode.Cancelled);
                }
                if (asyncConnectTask.IsFaulted)
                {
                    var exceptions = asyncConnectTask.Exception?.InnerExceptions;
                    if (exceptions != null && exceptions.Count > 0 && exceptions[0] != null)
                    {
                        throw exceptions[0];
                    }                    
                }
                if (!asyncConnectTask.IsCompleted)
                {
                    throw new KafkaConnectionException(this, KafkaConnectionErrorCode.ClientTimeout);
                }
                                
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
        }
        
        /// <exception cref="KafkaConnectionException"/>
        public void Close()
        {           
            var tcpClient = _tcpClient;
            _tcpClient = null;

            try
            {                
                tcpClient?.Dispose();
            }
            catch (Exception exception)
            {
                throw ConvertException(exception);
            }                     
        }        

        /// <exception cref="KafkaConnectionException"/>
        [PublicAPI]
        public void Write([NotNull] byte[] data, int offset, int length)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(this, KafkaConnectionErrorCode.BadRequest);
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
        public void WriteAsync([NotNull] byte[] data, int offset, int length,
            [CanBeNull] Action<object, KafkaConnectionException> callback, [CanBeNull] object state = null)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(this, KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();
                                
                stream.WriteAsync(data, offset, length)?.ContinueWith(EndWrite, new WriteAsyncCallback(callback, state), TaskContinuationOptions.None);
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
        private void EndWrite([NotNull] Task asyncWriteTask, object state)
        {
            var callback = state as WriteAsyncCallback;
            if (callback == null) return;

            try
            {                
                if (asyncWriteTask.IsFaulted)
                {
                    var exceptions = asyncWriteTask.Exception?.InnerExceptions;
                    if (exceptions != null && exceptions.Count > 0 && exceptions[0] != null)
                    {
                        callback.Callback?.Invoke(callback.State, ConvertException(exceptions[0]));
                    }
                }
                if (!asyncWriteTask.IsCompleted)
                {
                    callback.Callback?.Invoke(callback.State, new KafkaConnectionException(this, KafkaConnectionErrorCode.ClientTimeout));
                }
                
                callback.Callback?.Invoke(callback.State, null);
            }            
            catch (Exception exception)
            {
                callback.Callback?.Invoke(callback.State, ConvertException(exception));
            }
        }

        /// <exception cref="KafkaConnectionException"/>
        [PublicAPI]
        public int Read([NotNull] byte[] data, int offset, int length)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(this, KafkaConnectionErrorCode.BadRequest);
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
        public void ReadAsync([NotNull] byte[] data, int offset, int length,
            [CanBeNull] Action<object, int, KafkaConnectionException> callback, [CanBeNull] object state = null)
        {
            if (!CheckBufferData(data, offset, length))
            {
                throw new KafkaConnectionException(this, KafkaConnectionErrorCode.BadRequest);
            }

            try
            {
                var stream = GetStream();

                stream.ReadAsync(data, offset, length)?.ContinueWith(EndRead, new ReadAsyncCallback(callback, state), TaskContinuationOptions.None);
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
        private void EndRead([NotNull] Task<int> asyncReadTask, object state)
        {
            var callback = state as ReadAsyncCallback;
            if (callback == null) return;

            try
            {
                if (asyncReadTask.IsFaulted)
                {
                    var exceptions = asyncReadTask.Exception?.InnerExceptions;
                    if (exceptions != null && exceptions.Count > 0 && exceptions[0] != null)
                    {
                        callback.Callback?.Invoke(callback.State, 0, ConvertException(exceptions[0]));
                    }
                }
                if (!asyncReadTask.IsCompleted)
                {
                    callback.Callback?.Invoke(callback.State, 0, new KafkaConnectionException(this, KafkaConnectionErrorCode.ClientTimeout));
                }

                callback.Callback?.Invoke(callback.State, asyncReadTask.Result, null);
            }
            catch (Exception exception)
            {
                callback.Callback?.Invoke(callback.State, 0, ConvertException(exception));
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
                throw new KafkaConnectionException(this, KafkaConnectionErrorCode.ConnectionMaintenance);
            }

            if (stream == null)
            {                
                throw new KafkaConnectionException(this, KafkaConnectionErrorCode.ConnectionClosed);
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
            var connectionException = exception as KafkaConnectionException;
            if (connectionException != null)
            {
                return connectionException;
            }
                        
            var socketException = exception as SocketException;
            if (socketException != null)
            {
                var socketErrorInfo = new KafkaConnectionSocketErrorInfo(socketException.SocketErrorCode, (int)socketException.SocketErrorCode, null);
                return new KafkaConnectionException(this, ConvertError(socketException.SocketErrorCode), socketException, socketErrorInfo);
            }

            if (exception is SecurityException)
            {
                return new KafkaConnectionException(this, KafkaConnectionErrorCode.NotAuthorized, exception);
            }

            if (exception is IOException)
            {
                return new KafkaConnectionException(this, KafkaConnectionErrorCode.TransportError, exception);
            }

            if (exception is ObjectDisposedException)
            {
                return new KafkaConnectionException(this, KafkaConnectionErrorCode.ConnectionMaintenance, exception);
            }

            if (exception is InvalidOperationException)
            {
                return new KafkaConnectionException(this, KafkaConnectionErrorCode.UnsupportedOperation, exception);
            }

            if (exception is ArgumentException)
            {
                return new KafkaConnectionException(this, KafkaConnectionErrorCode.UnsupportedOperation, exception);
            }

            if (exception is NotSupportedException)
            {
                return new KafkaConnectionException(this, KafkaConnectionErrorCode.UnsupportedOperation, exception);
            }

            return new KafkaConnectionException(this, KafkaConnectionErrorCode.UnknownError, exception);
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

        private class WriteAsyncCallback
        {
            public readonly Action<object, KafkaConnectionException> Callback;
            public readonly object State;
            public WriteAsyncCallback(Action<object, KafkaConnectionException> callback, object state)
            {
                Callback = callback;
                State = state;
            }
        }

        private class ReadAsyncCallback
        {
            public readonly Action<object, int, KafkaConnectionException> Callback;
            public readonly object State;
            public ReadAsyncCallback(Action<object, int, KafkaConnectionException> callback, object state)
            {
                Callback = callback;
                State = state;
            }
        }
    }
}