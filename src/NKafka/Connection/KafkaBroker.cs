using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Protocol;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Connection
{
    internal sealed class KafkaBroker
    {
        public bool IsOpenned => _isOpenned;

        public bool IsEnabled => _isOpenned && !_isConnectionMaintenance;

        public KafkaBrokerStateErrorCode? Error => _sendError ?? _receiveError;

        [NotNull]
        public string Name { get; }

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


        [NotNull]
        private readonly KafkaConnection _connection;
        [NotNull]
        private readonly KafkaProtocol _kafkaProtocol;
        [NotNull]
        private readonly KafkaConnectionSettings _settings;

        [NotNull]
        private readonly ConcurrentDictionary<int, RequestState> _requests;
        [NotNull]
        private readonly ResponseState _responseState;

        private KafkaBrokerStateErrorCode? _sendError;
        private KafkaBrokerStateErrorCode? _receiveError;

        private volatile bool _isOpenned;
        private volatile bool _isConnectionMaintenance;

        private DateTime _connectionTimestampUtc;
        private DateTime _lastActivityTimestampUtc;
        private int? _heartbeatRequestId;

        private int _currentRequestId;

        public KafkaBroker([NotNull] KafkaConnection connection, [NotNull] KafkaProtocol kafkaProtocol,
            [NotNull] string name, [CanBeNull] KafkaConnectionSettings settings)
        {
            _connection = connection;
            _kafkaProtocol = kafkaProtocol;
            _settings = settings ?? KafkaConnectionSettingsBuilder.Default;
            Name = name;

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
                        //todo (E013) broker: client timeout
                        request.Error = KafkaBrokerErrorCode.ClientTimeout;
                        _sendError = KafkaBrokerStateErrorCode.ClientTimeout;
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
                _heartbeatRequestId = Send(heartbeatReqeust, heartbeatPeriod.Value).Data;
            }
        }

        private void TryOpenConnection(CancellationToken cancellation)
        {
            _connectionTimestampUtc = DateTime.UtcNow;
            _lastActivityTimestampUtc = DateTime.UtcNow;
            var openResult = _connection.TryOpen(cancellation);
            if (openResult.Error != null)
            {
                //todo (E013) broker: connection error  
                _sendError = ConvertStateError(openResult.Error.Value);
                return;
            }            
            if (!openResult.Result)
            {
                //todo (E013) broker: connection error
                _sendError = KafkaBrokerStateErrorCode.ConnectionNotAllowed;
                return;
            }

            BeginRead();
        }

        private void CloseConnection()
        {
            _connection.Close();
        }

        public KafkaBrokerResult<int?> Send<TRequest>(TRequest request, TimeSpan timeout, int? dataCapacity = null)
            where TRequest : class, IKafkaRequest
        {
            return SendInternal(request, timeout, dataCapacity, true);
        }

        public KafkaBrokerResult<int?> SendWithoutResponse<TRequest>(TRequest request, int? dataCapacity = null)
            where TRequest : class, IKafkaRequest
        {
            return SendInternal(request, TimeSpan.Zero, dataCapacity, false);
        }

        private KafkaBrokerResult<int?> SendInternal<TRequest>(TRequest request, TimeSpan timeout, int? dataCapacity, bool processResponse)
            where TRequest : class, IKafkaRequest
        {
            if (request == null) return KafkaBrokerErrorCode.BadRequest;
            if (!_isOpenned)
            {
                return KafkaBrokerErrorCode.ConnectionClosed;
            }
            if (_isConnectionMaintenance)
            {
                return KafkaBrokerErrorCode.ConnectionMaintenance;
            }

            if (_settings.TransportLatency > TimeSpan.Zero)
            {
                timeout = timeout +
                    _settings.TransportLatency + //request
                    _settings.TransportLatency; //response
            }

            var requestId = Interlocked.Increment(ref _currentRequestId);

            byte[] data;
            try
            {
                data = _kafkaProtocol.WriteRequest(request, requestId, dataCapacity);
                if (data == null)
                {
                    //todo (E013) broker: protocol error
                    return KafkaBrokerErrorCode.ProtocolError;
                }
            }
            catch (Exception)
            {
                //todo (E013) broker: protocol error
                return KafkaBrokerErrorCode.ProtocolError;
            }

            var requestState = new RequestState(request, DateTime.UtcNow, timeout);
            if (processResponse)
            {
                _requests[requestId] = requestState;
                _lastActivityTimestampUtc = DateTime.UtcNow;
            }

            var writeResult = _connection.TryWrite(data, 0, data.Length);
            if (writeResult.Error != null)
            {
                //todo (E013) broker: send request error  
                _sendError = ConvertStateError(writeResult.Error.Value);
                return ConvertError(writeResult.Error.Value);
            }
            requestState.SentTimestampUtc = DateTime.UtcNow;
            _sendError = null;
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

        #region Async receive        

        private void BeginRead()
        {
            var headerBuffer = _responseState.ResponseHeaderBuffer;

            var beginReadResult = _connection.TryBeginRead(headerBuffer, 0, headerBuffer.Length, OnReceived);

            if (beginReadResult.Error != null)
            {
                //todo (E013) broker: begin read error (begin read again?)
                _receiveError = ConvertStateError(beginReadResult.Error.Value);
            }

            if (beginReadResult.Result == null)
            {
                _receiveError = KafkaBrokerStateErrorCode.TransportError;
            }
        }

        private void OnReceived(IAsyncResult result)
        {
            if (!_isOpenned) return;

            try
            {
                while (_connection.IsDataAvailable())
                {
                    var responseHeader = ReadResponseHeader(result);
                    if (responseHeader == null)
                    {
                        return;
                    }

                    _lastActivityTimestampUtc = DateTime.UtcNow;

                    var requestId = responseHeader.CorrelationId;
                    RequestState requestState;
                    if (!_requests.TryGetValue(requestId, out requestState) || requestState == null)
                    {
                        // unexpected response?
                        if (responseHeader.DataSize > 0)
                        {
                            var tempBuffer = new byte[responseHeader.DataSize];
                            var readResult = _connection.TryRead(tempBuffer, 0, tempBuffer.Length);
                            if (readResult.Error != null)
                            {
                                _receiveError = ConvertStateError(readResult.Error.Value);
                            }

                            //todo (E013) broker: received unexpected response
                        }
                        return;
                    }

                    var response = ReadResponse(requestState, responseHeader);
                    if (response == null)
                    {
                        return;
                    }

                    _lastActivityTimestampUtc = DateTime.UtcNow;
                    requestState.Response = response;
                    _receiveError = null;
                }
            }
            finally
            {
                BeginRead();
            }
        }

        [CanBeNull]
        private KafkaResponseHeader ReadResponseHeader(IAsyncResult result)
        {
            if (result == null)
            {
                //todo (E013) broker: empty async result
                _receiveError = KafkaBrokerStateErrorCode.TransportError;
                return null;
            }

            var responseHeaderSizeResult = _connection.TryEndRead(result);
            if (responseHeaderSizeResult.Error != null)
            {
                //todo (E013) broker: read response header size data error
                _receiveError = ConvertStateError(responseHeaderSizeResult.Error.Value);
                return null;
            }
            var responseHeaderSize = responseHeaderSizeResult.Result;

            if (responseHeaderSize == 0)
            {
                return null;
            }

            if (responseHeaderSize != _responseState.ResponseHeaderBuffer.Length)
            {
                //todo (E013) broker: response header size error
                _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                return null;
            }

            try
            {
                var responseHeader = _kafkaProtocol.ReadResponseHeader(_responseState.ResponseHeaderBuffer, 0, responseHeaderSize);
                if (responseHeader == null)
                {
                    //todo (E013) broker: response header protocol error
                    _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                    return null;
                }
                return responseHeader;
            }
            catch (Exception)
            {
                //todo (E013) broker: response header protocol exception
                _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                return null;
            }
        }

        [CanBeNull]
        private IKafkaResponse ReadResponse([NotNull] RequestState state, [NotNull]KafkaResponseHeader responseHeader)
        {
            var responseBuffer = new byte[responseHeader.DataSize];
            int responseSize = 0;
            do
            {
                var readSizeResult = _connection.TryRead(responseBuffer, responseSize, responseBuffer.Length - responseSize);
                if (readSizeResult.Error != null)
                {
                    // //todo (E013) broker: read response data error
                    _receiveError = ConvertStateError(readSizeResult.Error.Value);
                    state.Error = ConvertError(readSizeResult.Error.Value);
                    return null;
                }
                var readSize = readSizeResult.Result;
                if (readSize == 0)
                {
                    break;
                }
                responseSize += readSize;
            } while (responseSize < responseBuffer.Length);

            if (responseSize != responseBuffer.Length)
            {
                //todo (E013) broker: response size error
                _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                state.Error = KafkaBrokerErrorCode.ProtocolError;
                return null;
            }

            try
            {
                var response = _kafkaProtocol.ReadResponse(state.Request, responseBuffer, 0, responseBuffer.Length);
                if (response == null)
                {
                    //todo (E013) broker: response protocol error
                    _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                    state.Error = KafkaBrokerErrorCode.ProtocolError;
                    return null;
                }
                return response;
            }
            catch (Exception)
            {
                //todo (E013) broker: response protocol exception
                _receiveError = KafkaBrokerStateErrorCode.ProtocolError;
                state.Error = KafkaBrokerErrorCode.ProtocolError;
                return null;
            }
        }

        #endregion Async receive      

        #region State classes

        private sealed class RequestState
        {
            [NotNull]
            public readonly IKafkaRequest Request;
            public readonly TimeSpan? Timeout;

            public readonly DateTime CreateTimestampUtc;
            public DateTime? SentTimestampUtc;

            [CanBeNull]
            public IKafkaResponse Response;
            public KafkaBrokerErrorCode? Error;

            public RequestState([NotNull] IKafkaRequest request, DateTime createTimestampUtc, TimeSpan? timeout)
            {
                Request = request;
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

        #endregion State classes
        
        private static KafkaBrokerStateErrorCode ConvertStateError(KafkaConnectionErrorCode connectionError)
        {            
            switch (connectionError)
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

        private static KafkaBrokerErrorCode ConvertError(KafkaConnectionErrorCode connectionError)
        {
            switch (connectionError)
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