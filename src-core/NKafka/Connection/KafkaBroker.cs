using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Connection.Logging;
using NKafka.Protocol;
using NKafka.Protocol.API.TopicMetadata;
// ReSharper disable InconsistentlySynchronizedField

namespace NKafka.Connection
{
    public sealed class KafkaBroker
    {
        public bool IsOpenned => _isOpenned;        

        public KafkaBrokerStateErrorCode? Error => _sendError ?? _receiveError;

        public DateTime? ConnectionTimestampUtc
        {
            get
            {
                var connectionTimestampUtc = _connectionTimestampUtc;
                return connectionTimestampUtc > DateTime.MinValue ? connectionTimestampUtc : (DateTime?)null;
            }
        }

        public DateTime? LastActivityTimestampUtc
        {
            get
            {
                var lastActivityTimestampUtc = _lastActivityTimestampUtc;
                return lastActivityTimestampUtc > DateTime.MinValue ? lastActivityTimestampUtc : (DateTime?)null;
            }
        }

        [NotNull] public KafkaProtocol Protocol => _kafkaProtocol;

        [NotNull] private readonly string _name;
        [NotNull] private readonly string _host;
        private readonly int _port;        
        [NotNull] private readonly KafkaProtocol _kafkaProtocol;
        [NotNull] private readonly KafkaConnectionSettings _settings;
        [CanBeNull] private readonly IKafkaBrokerLogger _logger;

        [CanBeNull] private ConnectionState _currentConnection;
        [CanBeNull] private ConnectionState _previousConnection;

        [NotNull] private readonly ConcurrentDictionary<int, RequestState> _requests;
        [NotNull] private readonly ResponseState _responseState;

        private KafkaBrokerStateErrorCode? _sendError;
        private KafkaBrokerStateErrorCode? _receiveError;

        private volatile bool _isOpenned;        

        private DateTime _connectionTimestampUtc;
        private DateTime _lastActivityTimestampUtc;
        private int? _heartbeatRequestId;

        private int _currentRequestId;

        public KafkaBroker([NotNull] string name,
            [NotNull] string host, int port,
            [NotNull] KafkaProtocol kafkaProtocol,
            [CanBeNull] KafkaConnectionSettings settings,
            [CanBeNull] IKafkaBrokerLogger logger)
        {
            _name = name;
            _host = host;
            _port = port;            
            _kafkaProtocol = kafkaProtocol;
            _settings = settings ?? KafkaConnectionSettingsBuilder.Default;
            _logger = logger;

            _requests = new ConcurrentDictionary<int, RequestState>();
            _responseState = new ResponseState(kafkaProtocol);

            _isOpenned = false;                      
        }

        public void Open(CancellationToken cancellation)
        {            
            var connection = TryOpenConnection(cancellation);
            _currentConnection = connection;
            if (connection == null) return;

            _isOpenned = true;
            BeginRead(connection);            
        }

        public void Close()
        {            
            _isOpenned = false;
            CloseConnection(_previousConnection);
            CloseConnection(_currentConnection);
            _sendError = null;
            _receiveError = null;
            _requests.Clear();
        }

        public void Maintenance(CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;
            if (!_isOpenned) return;
            
            //set timeout error if needed            
            foreach (var requestPair in _requests)
            {
                var request = requestPair.Value;
                if (request == null) continue;

                if (request.Response == null && request.Error == null)
                {
                    var requestTimestampUtc = request.SentTimestampUtc ?? request.CreateTimestampUtc;
                    if (DateTime.UtcNow >= requestTimestampUtc + request.Timeout)
                    {
                        request.Error = KafkaBrokerErrorCode.ClientTimeout;
                        _sendError = KafkaBrokerStateErrorCode.ClientTimeout;
                        LogConnectionError(KafkaBrokerErrorCode.ClientTimeout, "CheckTimeout",
                            KafkaConnectionErrorCode.ClientTimeout, request.RequestInfo);
                    }                    
                }
            }            

            //reconnect if needed
            var error = _sendError ?? _receiveError;
            var reconnectPeriod = error != null
                ? _settings.ErrorStateReconnectPeriod
                : _settings.RegularReconnectPeriod;

            var currentConnection = _currentConnection;

            // reconnect if needed
            if (DateTime.UtcNow - _connectionTimestampUtc >= reconnectPeriod)
            {
                if (cancellation.IsCancellationRequested) return;

                if (currentConnection != null)
                {
                    currentConnection.IsDeprecated = true;
                }

                var previousConnection = _previousConnection;
                if (previousConnection != null)
                {
                    previousConnection.IsClosed = true;
                    CloseConnection(previousConnection);
                }

                _previousConnection = currentConnection;

                currentConnection = TryOpenConnection(cancellation);
                _currentConnection = currentConnection;
                if (currentConnection != null)
                {
                    BeginRead(currentConnection);
                }                                

                return;
            }            

            // begin read if previous try has been failed
            if (currentConnection?.IsReading == false)
            {
                BeginRead(currentConnection);
            }

            //heartbeat: check response if requested
            var heartbeatRequestId = _heartbeatRequestId;
            if (heartbeatRequestId.HasValue)
            {
                var heartbeatResponse = Receive<KafkaTopicMetadataResponse>(heartbeatRequestId.Value);
                if (heartbeatResponse.HasData || heartbeatResponse.HasError)
                {
                    _heartbeatRequestId = null;
                }
            }

            //heartbeat: send request if needed (and haven't sent already)
            error = _sendError ?? _receiveError;
            var heartbeatPeriod = error != null && _heartbeatRequestId == null
                ? (TimeSpan?)_settings.HeartbeatPeriod
                : null;

            if (heartbeatPeriod != null && DateTime.UtcNow - _lastActivityTimestampUtc >= heartbeatPeriod.Value)
            {
                var heartbeatReqeust = new KafkaTopicMetadataRequest(new[] { "_hearbeat" });
                _heartbeatRequestId = Send(heartbeatReqeust, _name, heartbeatPeriod.Value).Data;
            }
        }

        [CanBeNull]
        private ConnectionState TryOpenConnection(CancellationToken cancellation)
        {
            var connection = new KafkaConnection(_host, _port);
            var connectionState = new ConnectionState(connection);
            try
            {                
                connection.Open(cancellation);
                _connectionTimestampUtc = DateTime.UtcNow;
                _lastActivityTimestampUtc = DateTime.UtcNow;
                LogConnected();
                return connectionState;
            }
            catch (KafkaConnectionException connectionException)
            {
                _sendError = ConvertStateError(connectionState, connectionException);
                LogConnectionError("OpenConnection", connectionState, connectionException);
                return null;
            }
        }        

        private void CloseConnection([CanBeNull] ConnectionState connection)
        {
            if (connection == null) return;
            try
            {
                connection.Connection.Close();
            }
            catch (KafkaConnectionException connectionException)
            {                
                LogConnectionError("CloseConnection", connection, connectionException);
            }
        }

        public KafkaBrokerResult<int?> Send<TRequest>([NotNull] TRequest request, [NotNull] string sender,
            TimeSpan timeout, int? dataCapacity = null)
            where TRequest : class, IKafkaRequest
        {
            return SendInternal(request, sender, timeout, dataCapacity, true);
        }

        public void SendWithoutResponse<TRequest>([NotNull] TRequest request, [NotNull] string sender,
            int? dataCapacity = null)
            where TRequest : class, IKafkaRequest
        {
            SendInternal(request, sender, TimeSpan.Zero, dataCapacity, false);
        }

        private KafkaBrokerResult<int?> SendInternal<TRequest>([NotNull] TRequest request, [NotNull] string sender,
            TimeSpan timeout, int? dataCapacity, bool responseIsCheckable)
            where TRequest : class, IKafkaRequest
        {
            var connection = _currentConnection;
            if (!_isOpenned || connection == null)
            {
                return KafkaBrokerErrorCode.ConnectionClosed;
            }            

            var requstType = _kafkaProtocol.GetRequestType<TRequest>();
            if (requstType == null)
            {
                return KafkaBrokerErrorCode.BadRequest;
            }

            if (_settings.TransportLatency > TimeSpan.Zero)
            {
                timeout = timeout +
                    _settings.TransportLatency + //request
                    _settings.TransportLatency; //response
            }

            var requestId = Interlocked.Increment(ref _currentRequestId);

            var requestInfo = new KafkaBrokerRequestInfo(requstType.Value, requestId, sender);

            byte[] data;
            try
            {
                data = _kafkaProtocol.WriteRequest(request, requstType.Value, requestId, dataCapacity);
            }
            catch (KafkaProtocolException protocolException)
            {                
                LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "WriteRequest", protocolException, requestInfo);
                return KafkaBrokerErrorCode.ProtocolError;
            }

            var requestState = new RequestState(requestInfo, connection, DateTime.UtcNow, timeout);
            if (responseIsCheckable)
            {
                _requests[requestId] = requestState;
            }

            try
            {                
                _lastActivityTimestampUtc = DateTime.UtcNow;
                connection.Connection.BeginWrite(data, 0, data.Length, OnSent, requestState);
                requestState.SentTimestampUtc = DateTime.UtcNow;
                return requestId;
            }
            catch (KafkaConnectionException connectionException)
            {
                _requests.TryRemove(requestId, out requestState);                

                var error = ConvertError(connection, connectionException);
                _sendError = ConvertStateError(connection, connectionException);

                LogConnectionError(error, "BeginWriteRequest", connectionException, requestInfo);
                return error;
            }           
        }

        public KafkaBrokerResult<TResponse> Receive<TResponse>(int requestId) where TResponse : class, IKafkaResponse
        {
            RequestState requestState;
            if (!_requests.TryGetValue(requestId, out requestState) || requestState == null)
            {
                return KafkaBrokerErrorCode.BadRequest;
            }

            if (requestState.Error.HasValue)
            {
                var result = requestState.Error.Value;
                _requests.TryRemove(requestId, out requestState);
                return result;
            }

            var response = requestState.Response as TResponse;
            if (response != null)
            {
                _requests.TryRemove(requestId, out requestState);
                return response;
            }

            return (TResponse)null;
        }

        #region Async send

        private void OnSent(IAsyncResult result)
        {            
            if (!_isOpenned || result == null)
            {
                return;
            }

            var requestState = result.AsyncState as RequestState;
            if (requestState == null) return;
            var connection = requestState.Connection;

            try
            {
                connection.Connection.EndWrite(result);
            }
            catch (KafkaConnectionException connectionException)
            {                
                var error = ConvertError(connection, connectionException);
                requestState.Error = error;
                
                if (requestState.Connection.IsDeprecated) return;

                _sendError = ConvertStateError(connection, connectionException);
                LogConnectionError(error, "EndWriteRequest", connectionException, requestState.RequestInfo);
                return;
            }

            _sendError = null;
        }

        #endregion Async send

        #region Async receive        
        
        private void BeginRead([NotNull] ConnectionState connection)
        {
            if (connection.IsClosed) return;

            var responseState = _responseState;                
            var responseHeaderBuffer = responseState.ResponseHeaderBuffer;

            try
            {
                connection.Connection.BeginRead(responseHeaderBuffer, 0, responseHeaderBuffer.Length, 
                    OnReceived, connection);
                connection.IsReading = true;                
            }
            catch (KafkaConnectionException connectionException)
            {
                _receiveError = ConvertStateError(connection, connectionException);
                LogConnectionError("BeginReadResponseHeader", connection, connectionException);
                connection.IsReading = false;
            }            
        }       

        private void OnReceived(IAsyncResult result)
        {
            var responseState = _responseState;

            if (!_isOpenned)
            {                
                return;
            }

            ConnectionState connection = null;            
            try
            {               
                connection = result?.AsyncState as ConnectionState;
                if (connection == null) return;
                if (connection.IsClosed) return;

                int dataSize;
                try
                {
                    dataSize = connection.Connection.EndRead(result);
                }
                catch (KafkaConnectionException connectionException)
                {                    
                    if (connection.IsDeprecated) return;                    

                    _receiveError = ConvertStateError(connection, connectionException);
                    LogConnectionError("EndReadResponseHeader", connection, connectionException);
                    return;
                }                

                if (dataSize == 0)
                {
                    return;
                }

                var responseHeaderOffset = dataSize;
                var responseHeaderBuffer = responseState.ResponseHeaderBuffer;                

                var isDataAvailable = false;
                do
                {
                    // read responseHeader (if it has not read)
                    if (responseHeaderOffset < responseHeaderBuffer.Length)
                    {

                        try
                        {
                            dataSize = connection.Connection.Read(responseHeaderBuffer, responseHeaderOffset, 
                                responseHeaderBuffer.Length - responseHeaderOffset);
                        }
                        catch (KafkaConnectionException connectionException)
                        {                            
                            if (connection.IsDeprecated) return;                            

                            _receiveError = ConvertStateError(connection, connectionException);
                            LogConnectionError("EndRepeatedReadResponseHeader", connection, connectionException);
                            return;
                        }

                        responseHeaderOffset += dataSize;
                        if (responseHeaderOffset < responseHeaderBuffer.Length)
                        {
                            continue;
                        }
                    }

                    responseHeaderOffset = 0;

                    KafkaResponseHeader responseHeader;
                    try
                    {
                        responseHeader = _kafkaProtocol.ReadResponseHeader(responseHeaderBuffer, 
                            responseHeaderOffset, responseHeaderBuffer.Length);
                    }
                    catch (KafkaProtocolException protocolException)
                    {                        
                        if (connection.IsDeprecated) return;                        

                        LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "ReadResponseHeader", protocolException);
                        _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                        continue;
                    }

                    _lastActivityTimestampUtc = DateTime.UtcNow;

                    // find request by correlation id
                    var requestId = responseHeader.CorrelationId;
                    RequestState requestState;
                    if (!_requests.TryGetValue(requestId, out requestState) || requestState == null)
                    {
                        // unexpected response?
                        if (responseHeader.DataSize > 0)
                        {
                            var tempBuffer = new byte[responseHeader.DataSize];

                            try
                            {
                                connection.Connection.Read(tempBuffer, 0, tempBuffer.Length);
                            }
                            catch (KafkaConnectionException connectionException)
                            {
                                if (connection.IsDeprecated) return;

                                _receiveError = ConvertStateError(connection, connectionException);
                                LogConnectionError("ReadUnexpectedResponse", connection, connectionException);
                                return;
                            }
                        }
                        return;
                    }

                    // read response data
                    var responseBuffer = new byte[responseHeader.DataSize];
                    var responseOffset = 0;
                    do
                    {
                        int responsePackageSize;
                        try
                        {
                            responsePackageSize = connection.Connection.Read(responseBuffer, responseOffset, responseBuffer.Length - responseOffset);
                        }
                        catch (KafkaConnectionException connectionException)
                        {
                            var error = ConvertError(connection, connectionException);                            
                            requestState.Error = error;
                            if (connection.IsDeprecated) return;

                            _receiveError = ConvertStateError(connection, connectionException);
                            LogConnectionError(error, "ReadResponse", connectionException, requestState.RequestInfo);
                            return;
                        }

                        if (responsePackageSize == 0)
                        {
                            break;
                        }
                        responseOffset += responsePackageSize;
                    } while (responseOffset < responseBuffer.Length);

                    if (responseOffset != responseBuffer.Length)
                    {
                        if (connection.IsDeprecated)
                        {
                            requestState.Error = KafkaBrokerErrorCode.ConnectionMaintenance;
                            return;
                        }

                        requestState.Error = KafkaBrokerErrorCode.ProtocolError;
                        _receiveError = KafkaBrokerStateErrorCode.ProtocolError;                        
                        LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "ReadResponseCheckDataSize",
                            KafkaProtocolErrorCode.UnexpectedResponseSize, requestState.RequestInfo);
                        continue;
                    }

                    IKafkaResponse response;
                    try
                    {
                        response = _kafkaProtocol.ReadResponse(requestState.RequestInfo.RequestType, responseBuffer, 0, responseBuffer.Length);
                    }
                    catch (KafkaProtocolException protocolException)
                    {                        
                        _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                        requestState.Error = KafkaBrokerErrorCode.ProtocolError;
                        LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "ReadResponse",
                            protocolException, requestState.RequestInfo);
                        continue;
                    }

                    _lastActivityTimestampUtc = DateTime.UtcNow;
                    requestState.Response = response;
                    _receiveError = null;

                    try
                    {
                        isDataAvailable = connection.Connection.IsDataAvailable();
                    }
                    catch (KafkaConnectionException connectionException)
                    {
                        if (connection.IsDeprecated) return;

                        _receiveError = ConvertStateError(connection, connectionException);
                        LogConnectionError("CheckDataAvailability", connection, connectionException);
                    }

                } while (isDataAvailable);
            }
            finally
            {
                if (connection != null && _isOpenned)
                {
                    BeginRead(connection);
                }
            }
        }

        #endregion Async receive              

        private void LogProtocolError(KafkaBrokerErrorCode errorCode, string errorDescription,
            [NotNull] KafkaProtocolException protocolException, [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            if (!_isOpenned) return;

            var logger = _logger;
            if (logger == null) return;

            var errorInfo = new KafkaBrokerProtocolErrorInfo(errorCode, errorDescription, protocolException.ErrorCode, requestInfo, protocolException.InnerException);

            logger.OnProtocolError(errorInfo);
        }

        private void LogProtocolError(KafkaBrokerErrorCode errorCode, string errorDescription,
            KafkaProtocolErrorCode protocolErrorCode, [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            if (!_isOpenned) return;

            var logger = _logger;
            if (logger == null) return;

            var errorInfo = new KafkaBrokerProtocolErrorInfo(errorCode, errorDescription, protocolErrorCode, requestInfo, null);
            logger.OnProtocolError(errorInfo);
        }

        private void LogConnectionError(KafkaBrokerErrorCode errorCode, string errorDescription, [NotNull] KafkaConnectionException connectionException,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            if (!_isOpenned) return;

            var logger = _logger;
            if (logger == null) return;

            var errorInfo = new KafkaBrokerTransportErrorInfo(errorCode, errorDescription,
                connectionException.ErrorInfo, requestInfo, connectionException.InnerException);
            logger.OnTransportError(errorInfo);
        }

        private void LogConnectionError(KafkaBrokerErrorCode errorCode, string errorDescription, KafkaConnectionErrorCode connectionErrorCode,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            if (!_isOpenned) return;

            var logger = _logger;
            if (logger == null) return;

            var connectionErrorInfo = new KafkaConnectionErrorInfo(connectionErrorCode, null);
            var errorInfo = new KafkaBrokerTransportErrorInfo(errorCode, errorDescription,
                connectionErrorInfo, requestInfo, null);
            logger.OnTransportError(errorInfo);
        }

        private void LogConnectionError(string errorDescription, 
            [NotNull] ConnectionState state,
            [NotNull] KafkaConnectionException connectionException,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            if (!_isOpenned) return;

            var logger = _logger;
            if (logger == null) return;

            var errorCode = ConvertError(state, connectionException);
            var errorInfo = new KafkaBrokerTransportErrorInfo(errorCode, errorDescription,
                connectionException.ErrorInfo, requestInfo, connectionException.InnerException);
            logger.OnTransportError(errorInfo);
        }

        private void LogConnected()
        {
            _logger?.OnConnected();
        }

        private KafkaBrokerStateErrorCode ConvertStateError([NotNull] ConnectionState state, [NotNull] KafkaConnectionException connectionException)
        {
            if (state.IsDeprecated) return KafkaBrokerStateErrorCode.ConnectionMaintenance;

            switch (connectionException.ErrorInfo.ErrorCode)
            {
                case KafkaConnectionErrorCode.ConnectionClosed:
                    return KafkaBrokerStateErrorCode.ConnectionClosed;
                case KafkaConnectionErrorCode.ConnectionMaintenance:
                    return KafkaBrokerStateErrorCode.ConnectionMaintenance;
                case KafkaConnectionErrorCode.BadRequest:
                    return KafkaBrokerStateErrorCode.ProtocolError;
                case KafkaConnectionErrorCode.TransportError:
                    return KafkaBrokerStateErrorCode.TransportError;
                case KafkaConnectionErrorCode.ClientTimeout:
                    return KafkaBrokerStateErrorCode.ClientTimeout;
                case KafkaConnectionErrorCode.Cancelled:
                    return KafkaBrokerStateErrorCode.Cancelled;
                case KafkaConnectionErrorCode.InvalidHost:
                    return KafkaBrokerStateErrorCode.InvalidHost;
                case KafkaConnectionErrorCode.UnsupportedHost:
                    return KafkaBrokerStateErrorCode.UnsupportedHost;
                case KafkaConnectionErrorCode.NetworkNotAvailable:
                    return KafkaBrokerStateErrorCode.NetworkNotAvailable;
                case KafkaConnectionErrorCode.ConnectionNotAllowed:
                    return KafkaBrokerStateErrorCode.ConnectionNotAllowed;
                case KafkaConnectionErrorCode.ConnectionRefused:
                    return KafkaBrokerStateErrorCode.ConnectionRefused;
                case KafkaConnectionErrorCode.HostUnreachable:
                    return KafkaBrokerStateErrorCode.HostUnreachable;
                case KafkaConnectionErrorCode.HostNotAvailable:
                    return KafkaBrokerStateErrorCode.HostNotAvailable;
                case KafkaConnectionErrorCode.NotAuthorized:
                    return KafkaBrokerStateErrorCode.NotAuthorized;
                case KafkaConnectionErrorCode.UnsupportedOperation:
                    return KafkaBrokerStateErrorCode.TransportError;
                case KafkaConnectionErrorCode.OperationRefused:
                    return KafkaBrokerStateErrorCode.TransportError;
                case KafkaConnectionErrorCode.TooBigMessage:
                    return KafkaBrokerStateErrorCode.TransportError;
                case KafkaConnectionErrorCode.UnknownError:
                    return KafkaBrokerStateErrorCode.UnknownError;
                default:
                    return KafkaBrokerStateErrorCode.UnknownError;
            }
        }

        private KafkaBrokerErrorCode ConvertError([NotNull] ConnectionState state, [NotNull] KafkaConnectionException connectionException)
        {
            if (state.IsDeprecated) return KafkaBrokerErrorCode.ConnectionMaintenance;

            switch (connectionException.ErrorInfo.ErrorCode)
            {
                case KafkaConnectionErrorCode.ConnectionClosed:
                    return KafkaBrokerErrorCode.ConnectionClosed;
                case KafkaConnectionErrorCode.ConnectionMaintenance:
                    return KafkaBrokerErrorCode.ConnectionMaintenance;
                case KafkaConnectionErrorCode.BadRequest:
                    return KafkaBrokerErrorCode.BadRequest;
                case KafkaConnectionErrorCode.TransportError:
                    return KafkaBrokerErrorCode.TransportError;
                case KafkaConnectionErrorCode.ClientTimeout:
                    return KafkaBrokerErrorCode.ClientTimeout;
                case KafkaConnectionErrorCode.Cancelled:
                    return KafkaBrokerErrorCode.Cancelled;
                case KafkaConnectionErrorCode.InvalidHost:
                    return KafkaBrokerErrorCode.HostUnreachable;
                case KafkaConnectionErrorCode.UnsupportedHost:
                    return KafkaBrokerErrorCode.HostUnreachable;
                case KafkaConnectionErrorCode.NetworkNotAvailable:
                    return KafkaBrokerErrorCode.ConnectionClosed;
                case KafkaConnectionErrorCode.ConnectionNotAllowed:
                    return KafkaBrokerErrorCode.ConnectionClosed;
                case KafkaConnectionErrorCode.ConnectionRefused:
                    return KafkaBrokerErrorCode.ConnectionRefused;
                case KafkaConnectionErrorCode.HostUnreachable:
                    return KafkaBrokerErrorCode.HostUnreachable;
                case KafkaConnectionErrorCode.HostNotAvailable:
                    return KafkaBrokerErrorCode.HostNotAvailable;
                case KafkaConnectionErrorCode.NotAuthorized:
                    return KafkaBrokerErrorCode.NotAuthorized;
                case KafkaConnectionErrorCode.UnsupportedOperation:
                    return KafkaBrokerErrorCode.TransportError;
                case KafkaConnectionErrorCode.OperationRefused:
                    return KafkaBrokerErrorCode.OperationRefused;
                case KafkaConnectionErrorCode.TooBigMessage:
                    return KafkaBrokerErrorCode.TooBigMessage;
                case KafkaConnectionErrorCode.UnknownError:
                    return KafkaBrokerErrorCode.UnknownError;
                default:
                    return KafkaBrokerErrorCode.UnknownError;
            }
        }

        #region State classes

        private sealed class RequestState
        {
            [NotNull]
            public readonly KafkaBrokerRequestInfo RequestInfo;
            [NotNull]
            public readonly ConnectionState Connection;
            public readonly TimeSpan? Timeout;

            public readonly DateTime CreateTimestampUtc;
            public DateTime? SentTimestampUtc;

            [CanBeNull]
            public IKafkaResponse Response;
            public KafkaBrokerErrorCode? Error;

            public RequestState([NotNull] KafkaBrokerRequestInfo requestInfo, [NotNull] ConnectionState connection,
                DateTime createTimestampUtc, TimeSpan? timeout)
            {
                RequestInfo = requestInfo;
                Connection = connection;
                CreateTimestampUtc = createTimestampUtc;
                Timeout = timeout;
            }
        }

        private sealed class ResponseState
        {
            [NotNull]
            public readonly byte[] ResponseHeaderBuffer;

            public ResponseState([NotNull] KafkaProtocol kafkaProtocol)
            {
                ResponseHeaderBuffer = new byte[kafkaProtocol.ResponseHeaderSize];
            }
        }

        private sealed class ConnectionState
        {
            [NotNull] public readonly KafkaConnection Connection;
            public bool IsDeprecated;
            public bool IsClosed;
            public bool IsReading;

            public ConnectionState([NotNull] KafkaConnection connection)
            {
                Connection = connection;
            }
        }

        #endregion State classes        
    }
}