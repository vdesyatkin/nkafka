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

        public bool IsEnabled => _isOpenned && !_isConnectionMaintenance;

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
        [NotNull] private readonly KafkaConnection _connection;
        [NotNull] private readonly KafkaProtocol _kafkaProtocol;
        [NotNull] private readonly KafkaConnectionSettings _settings;
        [CanBeNull] private readonly IKafkaBrokerLogger _logger;

        [NotNull] private readonly ConcurrentDictionary<int, RequestState> _requests;
        [NotNull] private readonly ResponseState _responseState;

        private KafkaBrokerStateErrorCode? _sendError;
        private KafkaBrokerStateErrorCode? _receiveError;

        private volatile bool _isOpenned;
        private volatile bool _isReading;
        private volatile bool _isConnectionMaintenance;
        [NotNull] private volatile object _connectionLocker;

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
            _connection = new KafkaConnection(host, port);
            _kafkaProtocol = kafkaProtocol;
            _settings = settings ?? KafkaConnectionSettingsBuilder.Default;
            _logger = logger;

            _requests = new ConcurrentDictionary<int, RequestState>();
            _responseState = new ResponseState(kafkaProtocol);

            _isOpenned = false;
            _isReading = false;
            _isConnectionMaintenance = false;

            _connectionLocker = new object();
        }

        public void Open(CancellationToken cancellation)
        {
            _isConnectionMaintenance = true;
            TryOpenConnection(cancellation);
            _isConnectionMaintenance = false;
            BeginRead(_connectionTimestampUtc);
            _isOpenned = true;
        }

        public void Close()
        {
            _isConnectionMaintenance = true;
            _isOpenned = false;
            CloseConnection();
            _sendError = null;
            _receiveError = null;
            _requests.Clear();
        }

        public void Maintenance(CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;
            if (!_isOpenned) return;

            var hasActiveRequests = false;
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

                    hasActiveRequests = true;
                }
            }

            //reconnect if needed
            var error = _sendError ?? _receiveError;          

            // reconnect after error
            if (error != null && DateTime.UtcNow - _connectionTimestampUtc >= _settings.ErrorStateReconnectPeriod)
            {
                if (cancellation.IsCancellationRequested) return;
                _isConnectionMaintenance = true;

                lock (_connectionLocker)
                {
                    TryReopenConnection(cancellation);                    
                    BeginRead(_connectionTimestampUtc);
                }

                _isConnectionMaintenance = false;

                return;
            }

            // regular reconnect
            if (error == null && DateTime.UtcNow - _connectionTimestampUtc >= _settings.RegularReconnectPeriod)
            {
                if (cancellation.IsCancellationRequested) return;
                _isConnectionMaintenance = true;

                if (hasActiveRequests) return;

                lock (_connectionLocker)
                {
                    // double check - are there active requests
                    foreach (var requestPair in _requests)
                    {
                        var request = requestPair.Value;
                        if (request == null) continue;

                        if (request.Response == null && request.Error == null)
                        {                           
                            hasActiveRequests = true;
                            break;
                        }
                    }
                    if (hasActiveRequests) return;

                    TryReopenConnection(cancellation);
                    BeginRead(_connectionTimestampUtc);
                }

                _isConnectionMaintenance = false;

                return;
            }

            // begin read if previous try has been failed
            if (!_isReading && !_isConnectionMaintenance)
            {
                BeginRead(_connectionTimestampUtc);
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

        private void TryOpenConnection(CancellationToken cancellation)
        {
            _connectionTimestampUtc = DateTime.UtcNow;
            _lastActivityTimestampUtc = DateTime.UtcNow;

            try
            {
                _connection.Open(cancellation);
                LogConnected();
            }
            catch (KafkaConnectionException connectionException)
            {
                _sendError = ConvertStateError(connectionException);
                LogConnectionError("OpenConnection", connectionException);
            }
        }

        private void TryReopenConnection(CancellationToken cancellation)
        {
            _connectionTimestampUtc = DateTime.UtcNow;
            _lastActivityTimestampUtc = DateTime.UtcNow;

            try
            {
                _connection.Reopen(cancellation);
                LogConnected();
            }
            catch (KafkaConnectionException connectionException)
            {
                _sendError = ConvertStateError(connectionException);
                LogConnectionError("ReopenConnection", connectionException);
            }
        }

        private void CloseConnection()
        {
            try
            {
                _connection.Close();
            }
            catch (KafkaConnectionException connectionException)
            {                
                LogConnectionError("CloseConnection", connectionException);
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
            if (!_isOpenned)
            {
                return KafkaBrokerErrorCode.ConnectionClosed;
            }
            if (_isConnectionMaintenance)
            {
                return KafkaBrokerErrorCode.ConnectionMaintenance;
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
                if (_isConnectionMaintenance) return KafkaBrokerErrorCode.ConnectionMaintenance;

                LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "WriteRequest", protocolException, requestInfo);
                return KafkaBrokerErrorCode.ProtocolError;
            }

            var requestState = new RequestState(requestInfo, DateTime.UtcNow, timeout);

            try
            {
                if (responseIsCheckable)
                {
                    Monitor.Enter(_connectionLocker);
                    if (!_isOpenned) return KafkaBrokerErrorCode.ConnectionClosed;
                    if (_isConnectionMaintenance) return KafkaBrokerErrorCode.ConnectionMaintenance;
                    
                    _requests[requestId] = requestState;
                }
                _lastActivityTimestampUtc = DateTime.UtcNow;
                _connection.BeginWrite(data, 0, data.Length, OnSent, requestState);
                requestState.SentTimestampUtc = DateTime.UtcNow;
                return requestId;
            }
            catch (KafkaConnectionException connectionException)
            {
                _requests.TryRemove(requestId, out requestState);
                if (_isConnectionMaintenance) return KafkaBrokerErrorCode.ConnectionMaintenance;

                var error = ConvertError(connectionException);
                _sendError = ConvertStateError(connectionException);

                LogConnectionError(error, "BeginWriteRequest", connectionException, requestInfo);
                return error;
            }
            finally
            {
                if (responseIsCheckable)
                {
                    Monitor.Exit(_connectionLocker);
                }
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

            try
            {
                _connection.EndWrite(result);
            }
            catch (KafkaConnectionException connectionException)
            {                
                var error = ConvertError(connectionException);
                if (requestState != null)
                {
                    requestState.Error = error;
                }
                if (_isConnectionMaintenance) return;

                _sendError = ConvertStateError(connectionException);
                LogConnectionError(error, "EndWriteRequest", connectionException, requestState?.RequestInfo);
                return;
            }

            _sendError = null;
        }

        #endregion Async send

        #region Async receive        
        
        private void BeginRead(DateTime connectionTimestampUtc)
        {            
            lock (_connectionLocker)
            {
                if (!_isOpenned)
                {                    
                    _isReading = false;
                    return;
                }
                if (connectionTimestampUtc != _connectionTimestampUtc)
                {
                    // deprecated connection                    
                    _isReading = false;
                    return;
                }

                var responseState = _responseState;                
                var responseHeaderBuffer = responseState.ResponseHeaderBuffer;

                try
                {                    
                    _connection.BeginRead(responseHeaderBuffer, 0, responseHeaderBuffer.Length, 
                        OnReceived, connectionTimestampUtc);
                    _isReading = true;
                }
                catch (KafkaConnectionException connectionException)
                {
                    _isReading = false;                    

                    _receiveError = ConvertStateError(connectionException);
                    LogConnectionError("BeginReadResponseHeader", connectionException);
                }
            }
        }       

        private void OnReceived(IAsyncResult result)
        {
            var responseState = _responseState;

            if (!_isOpenned)
            {                
                return;
            }

            DateTime readConnectionTimestampUtc = DateTime.MinValue;
            try
            {
                if (result == null)
                {                    
                    return;
                }
                
                if (result.AsyncState is DateTime)
                {
                    readConnectionTimestampUtc = (DateTime) result.AsyncState;
                }                

                int dataSize;
                try
                {
                    dataSize = _connection.EndRead(result);
                }
                catch (KafkaConnectionException connectionException)
                {                    
                    if (_isConnectionMaintenance) return;
                    if (connectionException.ErrorInfo.ErrorCode == KafkaConnectionErrorCode.ConnectionMaintenance) return;
                    if (readConnectionTimestampUtc != _connectionTimestampUtc) return;                                       

                    _receiveError = ConvertStateError(connectionException);
                    LogConnectionError("EndReadResponseHeader", connectionException);
                    return;
                }

                if (readConnectionTimestampUtc != _connectionTimestampUtc)
                {
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
                            dataSize = _connection.Read(responseHeaderBuffer, responseHeaderOffset, 
                                responseHeaderBuffer.Length - responseHeaderOffset);
                        }
                        catch (KafkaConnectionException connectionException)
                        {                            
                            if (_isConnectionMaintenance) return;                            

                            _receiveError = ConvertStateError(connectionException);
                            LogConnectionError("EndRepeatedReadResponseHeader", connectionException);
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
                        if (_isConnectionMaintenance) return;                        

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
                                _connection.Read(tempBuffer, 0, tempBuffer.Length);
                            }
                            catch (KafkaConnectionException connectionException)
                            {
                                if (_isConnectionMaintenance) return;

                                _receiveError = ConvertStateError(connectionException);
                                LogConnectionError("ReadUnexpectedResponse", connectionException);
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
                            responsePackageSize = _connection.Read(responseBuffer, responseOffset, responseBuffer.Length - responseOffset);
                        }
                        catch (KafkaConnectionException connectionException)
                        {
                            var error = ConvertError(connectionException);                            
                            requestState.Error = error;
                            if (_isConnectionMaintenance) return;

                            _receiveError = ConvertStateError(connectionException);
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
                        if (_isConnectionMaintenance)
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
                        isDataAvailable = _connection.IsDataAvailable();
                    }
                    catch (KafkaConnectionException connectionException)
                    {
                        if (_isConnectionMaintenance) return;

                        _receiveError = ConvertStateError(connectionException);
                        LogConnectionError("CheckDataAvailability", connectionException);
                    }

                } while (isDataAvailable);
            }
            finally
            {
                BeginRead(readConnectionTimestampUtc);
            }
        }

        #endregion Async receive      

        #region State classes

        private sealed class RequestState
        {
            [NotNull] public readonly KafkaBrokerRequestInfo RequestInfo;
            public readonly TimeSpan? Timeout;

            public readonly DateTime CreateTimestampUtc;
            public DateTime? SentTimestampUtc;

            [CanBeNull]
            public IKafkaResponse Response;
            public KafkaBrokerErrorCode? Error;

            public RequestState([NotNull] KafkaBrokerRequestInfo requestInfo,
                DateTime createTimestampUtc, TimeSpan? timeout)
            {
                RequestInfo = requestInfo;
                CreateTimestampUtc = createTimestampUtc;
                Timeout = timeout;
            }
        }

        private sealed class ResponseState
        {
            [NotNull] public readonly byte[] ResponseHeaderBuffer;            

            public ResponseState([NotNull] KafkaProtocol kafkaProtocol)
            {
                ResponseHeaderBuffer = new byte[kafkaProtocol.ResponseHeaderSize];
            }
        }

        #endregion State classes        

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

        private void LogConnectionError(string errorDescription, [NotNull] KafkaConnectionException connectionException,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            if (!_isOpenned) return;

            var logger = _logger;
            if (logger == null) return;

            var errorCode = ConvertError(connectionException);
            var errorInfo = new KafkaBrokerTransportErrorInfo(errorCode, errorDescription,
                connectionException.ErrorInfo, requestInfo, connectionException.InnerException);
            logger.OnTransportError(errorInfo);
        }

        private void LogConnected()
        {
            _logger?.OnConnected();
        }

        private KafkaBrokerStateErrorCode ConvertStateError([NotNull] KafkaConnectionException connectionException)
        {
            if (_isConnectionMaintenance) return KafkaBrokerStateErrorCode.ConnectionMaintenance;

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

        private KafkaBrokerErrorCode ConvertError([NotNull] KafkaConnectionException connectionException)
        {
            if (_isConnectionMaintenance) return KafkaBrokerErrorCode.ConnectionMaintenance;

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
    }
}