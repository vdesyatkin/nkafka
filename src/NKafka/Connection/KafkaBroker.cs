using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Connection.Logging;
using NKafka.Protocol;
using NKafka.Protocol.API.TopicMetadata;

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
        private volatile bool _isConnectionMaintenance;

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
            _isConnectionMaintenance = false;
        }

        public void Open(CancellationToken cancellation)
        {
            _isConnectionMaintenance = true;
            TryOpenConnection(cancellation);
            _isConnectionMaintenance = false;
            _isOpenned = true;
        }

        public void Close()
        {
            _isConnectionMaintenance = true;
            _isOpenned = false;
            CloseConnection();
            _sendError = null;
            _receiveError = null;
            _isConnectionMaintenance = false;
            _requests.Clear();
        }

        public void Maintenance(CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;
            if (!_isOpenned) return;

            //set timeout error if needed
            var hasAcitveRequests = false;
            foreach (var requestPair in _requests)
            {
                var request = requestPair.Value;
                if (request == null) continue;

                if (request.Timeout.HasValue && request.Response == null && request.Error == null)
                {
                    var requestTimestampUtc = request.SentTimestampUtc ?? request.CreateTimestampUtc;
                    if (DateTime.UtcNow >= requestTimestampUtc + request.Timeout)
                    {                        
                        request.Error = KafkaBrokerErrorCode.ClientTimeout;
                        _sendError = KafkaBrokerStateErrorCode.ClientTimeout;
                        LogConnectionError(KafkaBrokerErrorCode.ClientTimeout, "CheckTimeout", 
                            KafkaConnectionErrorCode.ClientTimeout, request.RequestInfo);
                    }
                    continue;
                }
                if (request.Response != null || request.Error != null)
                {
                    hasAcitveRequests = true;
                }
            }

            //reconnect if needed
            var error = _sendError ?? _receiveError;
            var reconnectionPeriod = error != null
                ? _settings.ErrorStateReconnectPeriod
                : _settings.RegularReconnectPeriod;

            if (DateTime.UtcNow - _connectionTimestampUtc >= reconnectionPeriod)
            {
                if (cancellation.IsCancellationRequested) return;
                _isConnectionMaintenance = true;

                if (hasAcitveRequests)
                {                    
                    return;
                }

                CloseConnection();
                TryOpenConnection(cancellation);
                _isConnectionMaintenance = false;
                return;
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

            //hearbeat: send request if needed (and haven't sent already)
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
                return;
            }

            BeginRead();
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

        public KafkaBrokerResult<int?> SendWithoutResponse<TRequest>([NotNull] TRequest request, [NotNull] string sender, 
            int? dataCapacity = null)
            where TRequest : class, IKafkaRequest
        {
            return SendInternal(request, sender, TimeSpan.Zero, dataCapacity, false);
        }

        private KafkaBrokerResult<int?> SendInternal<TRequest>([NotNull] TRequest request, [NotNull] string sender,
            TimeSpan timeout, int? dataCapacity, bool processResponse)
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
                LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "WriteRequest", protocolException, requestInfo);
                return KafkaBrokerErrorCode.ProtocolError;
            }            
            
            var requestState = new RequestState(requestInfo, DateTime.UtcNow, timeout);
            if (processResponse)
            {
                _requests[requestId] = requestState;
                _lastActivityTimestampUtc = DateTime.UtcNow;
            }

            try
            {
                _connection.BeginWrite(data, 0, data.Length, OnSent, requestState);
            }
            catch (KafkaConnectionException connectionException)
            {
                var error = ConvertError(connectionException);
                _sendError = ConvertStateError(connectionException);
                requestState.Error = error;
                LogConnectionError(error, "BeginWriteRequest", connectionException, requestInfo);
                return error;
            }

            requestState.SentTimestampUtc = DateTime.UtcNow;
            return requestId;
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
            var responseState = _responseState;

            if (!_isOpenned || result == null)
            {
                responseState.ResponseHeaderOffset = 0;
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
                _sendError = ConvertStateError(connectionException);
                if (requestState != null)
                {
                    requestState.Error = error;
                }
                LogConnectionError(error, "EndWriteRequest", connectionException, requestState?.RequestInfo);
                return;
            }
            
            _sendError = null;
        }

        #endregion Async send

        #region Async receive        

        private void BeginRead()
        {
            var responseState = _responseState;            
            if (responseState.ResponseHeaderOffset >= responseState.ResponseHeaderBuffer.Length)
            {
                responseState.ResponseHeaderOffset = 0;                
            }

            try
            {
                _connection.BeginRead(responseState.ResponseHeaderBuffer, responseState.ResponseHeaderOffset, 
                    responseState.ResponseHeaderBuffer.Length - responseState.ResponseHeaderOffset, OnReceived);
            }
            catch (KafkaConnectionException connectionException)
            {                
                _receiveError = ConvertStateError(connectionException);
                LogConnectionError("BeginReadResponseHeader", connectionException);
            }
        }

        private void OnReceived(IAsyncResult result)
        {
            var responseState = _responseState;

            if (!_isOpenned || result == null)
            {
                responseState.ResponseHeaderOffset = 0;
                return;
            }

            try
            {                
                int dataSize;
                try
                {
                    dataSize = _connection.EndRead(result);
                }
                catch (KafkaConnectionException connectionException)
                {                    
                    responseState.ResponseHeaderOffset = 0;
                    _receiveError = ConvertStateError(connectionException);
                    LogConnectionError("EndReadResponseHeader", connectionException);
                    return;
                }
                
                if (dataSize == 0)
                {
                    return;
                }
                
                responseState.ResponseHeaderOffset = responseState.ResponseHeaderOffset + dataSize;

                var isDataAvailable = false;
                do
                {
                    // read responseHeader (if it has not read)
                    if (responseState.ResponseHeaderOffset < responseState.ResponseHeaderBuffer.Length)
                    {

                        try
                        {
                            dataSize = _connection.EndRead(result);
                        }
                        catch (KafkaConnectionException connectionException)
                        {                            
                            responseState.ResponseHeaderOffset = 0;
                            _receiveError = ConvertStateError(connectionException);
                            LogConnectionError("EndRepeatedReadResponseHeader", connectionException);
                            return;
                        }                        

                        responseState.ResponseHeaderOffset = responseState.ResponseHeaderOffset + dataSize;
                        if (responseState.ResponseHeaderOffset < responseState.ResponseHeaderBuffer.Length)
                        {
                            continue;
                        }
                    }

                    KafkaResponseHeader responseHeader;
                    try
                    {
                        responseHeader = _kafkaProtocol.ReadResponseHeader(responseState.ResponseHeaderBuffer, 0, responseState.ResponseHeaderBuffer.Length);
                    }
                    catch (KafkaProtocolException protocolException)
                    {
                        responseState.ResponseHeaderOffset = 0;
                        LogProtocolError(KafkaBrokerErrorCode.ProtocolError, "ReadResponseHeader", protocolException);
                        _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                        continue;
                    }

                    responseState.ResponseHeaderOffset = 0;

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
                                _receiveError = ConvertStateError(connectionException);
                                LogConnectionError("ReadUnexpectedResponse", connectionException);
                                return;
                            }
                        }
                        return;
                    }

                    // read response data
                    var responseBuffer = new byte[responseHeader.DataSize];
                    var responseSize = 0;
                    do
                    {
                        int responsePackageSize;
                        try
                        {
                            responsePackageSize = _connection.Read(responseBuffer, responseSize, responseBuffer.Length - responseSize);
                        }
                        catch (KafkaConnectionException connectionException)
                        {
                            var error = ConvertError(connectionException);
                            _receiveError = ConvertStateError(connectionException);
                            requestState.Error = error;
                            LogConnectionError(error, "ReadResponse",  connectionException, requestState.RequestInfo);
                            return;
                        }
                        
                        if (responsePackageSize == 0)
                        {
                            break;
                        }
                        responseSize += responsePackageSize;
                    } while (responseSize < responseBuffer.Length);

                    if (responseSize != responseBuffer.Length)
                    {                        
                        _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                        requestState.Error = KafkaBrokerErrorCode.ProtocolError;
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
                        _receiveError = ConvertStateError(connectionException);
                        LogConnectionError("CheckDataAvailability", connectionException);
                    }

                } while (isDataAvailable);
            }
            finally
            {
                BeginRead();
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
            [NotNull]
            public readonly byte[] ResponseHeaderBuffer;

            public int ResponseHeaderOffset;

            public ResponseState([NotNull] KafkaProtocol kafkaProtocol)
            {
                ResponseHeaderBuffer = new byte[kafkaProtocol.ResponseHeaderSize];
            }
        }

        #endregion State classes        

        private void LogProtocolError(KafkaBrokerErrorCode errorCode, string errorDescription, 
            [NotNull] KafkaProtocolException protocolException, [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            var logger = _logger;
            if (logger == null) return;
            var errorInfo = new KafkaBrokerProtocolErrorInfo(errorCode, errorDescription, protocolException.ErrorCode, requestInfo, protocolException.InnerException);
            
            logger.OnProtocolError(errorInfo);
        }

        private void LogProtocolError(KafkaBrokerErrorCode errorCode, string errorDescription, 
            KafkaProtocolErrorCode protocolErrorCode, [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            var logger = _logger;
            if (logger == null) return;            
            var errorInfo = new KafkaBrokerProtocolErrorInfo(errorCode, errorDescription, protocolErrorCode, requestInfo, null);
            logger.OnProtocolError(errorInfo);
        }

        private void LogConnectionError(KafkaBrokerErrorCode errorCode, string errorDescription, [NotNull] KafkaConnectionException connectionException,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
            var logger = _logger;
            if (logger == null) return;            
            var errorInfo = new KafkaBrokerTransportErrorInfo(errorCode, errorDescription,
                connectionException.ErrorInfo, requestInfo, connectionException.InnerException);
            logger.OnTransportError(errorInfo);
        }

        private void LogConnectionError(KafkaBrokerErrorCode errorCode, string errorDescription, KafkaConnectionErrorCode connectionErrorCode,
            [CanBeNull] KafkaBrokerRequestInfo requestInfo = null)
        {
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

        private static KafkaBrokerStateErrorCode ConvertStateError([NotNull] KafkaConnectionException connectionException)
        {                     
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
                    return KafkaBrokerStateErrorCode.ProtocolError;
                case KafkaConnectionErrorCode.OperationRefused:
                    return KafkaBrokerStateErrorCode.ProtocolError;
                case KafkaConnectionErrorCode.TooBigMessage:
                    return KafkaBrokerStateErrorCode.TransportError;
                case KafkaConnectionErrorCode.UnknownError:
                    return KafkaBrokerStateErrorCode.UnknownError;
                default:
                    return KafkaBrokerStateErrorCode.UnknownError;
            }
        }

        private static KafkaBrokerErrorCode ConvertError([NotNull] KafkaConnectionException connectionException)
        {            
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
                    return KafkaBrokerErrorCode.UnsupportedOperation;
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