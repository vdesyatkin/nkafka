using System;
using System.Collections.Concurrent;
using System.IO;
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
        
        [PublicAPI]
        public string Name { get; }

        [NotNull] private readonly KafkaConnection _connection;
        [NotNull] private readonly KafkaProtocol _kafkaProtocol;
        [CanBeNull] private readonly KafkaConnectionSettings _settings;

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

        public KafkaBroker([NotNull] KafkaConnection connection, [NotNull] KafkaProtocol kafkaProtocol,
            [CanBeNull] string name, [NotNull] KafkaConnectionSettings settings)
        {
            _connection = connection;
            _kafkaProtocol = kafkaProtocol;
            _settings = settings;
            Name = name;
            
            _requests = new ConcurrentDictionary<int, RequestState>();
            _responseState = new ResponseState(kafkaProtocol);

            _isOpenned = false;
            _isConnectionMaintenance = false;
        }

        public void Open()
        {
            _isConnectionMaintenance = true;
            TryOpenConnection();
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

        public void Maintenance()
        {
            if (!_isOpenned) return;            

            //set timeout error if needed
            var hasAcitveRequests = false;
            foreach (var requestPair in _requests)
            {
                var request = requestPair.Value;
                if (request.Timeout.HasValue && request.Response == null && request.Error == null)
                {
                    var requestTimestampUtc = request.SentTimestampUtc ?? request.CreateTimestampUtc;
                    if (DateTime.UtcNow >= requestTimestampUtc + request.Timeout)
                    {
                        request.Error = KafkaBrokerErrorCode.Timeout;
                        _sendError = KafkaBrokerStateErrorCode.Timeout;
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
                ? _settings?.ErrorStateReconnectPeriod
                : _settings?.RegularReconnectPeriod;            

            if (reconnectionPeriod.HasValue && DateTime.UtcNow - _connectionTimestampUtc >= reconnectionPeriod.Value)
            {
                if (hasAcitveRequests)
                {
                    _isConnectionMaintenance = true;
                    return;
                }
                
                CloseConnection();
                TryOpenConnection();
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
                ? _settings?.HeartbeatPeriod
                : null;

            if (heartbeatPeriod != null && DateTime.UtcNow - _lastActivityTimestampUtc >= heartbeatPeriod.Value)
            {
                var heartbeatReqeust = new KafkaTopicMetadataRequest(new [] {"_hearbeat"});
                _heartbeatRequestId = Send(heartbeatReqeust, heartbeatPeriod.Value).Data;
            }
        }

        private void TryOpenConnection()
        {
            _connectionTimestampUtc = DateTime.UtcNow;
            _lastActivityTimestampUtc = DateTime.UtcNow;
            try
            {
                if (_connection.TryOpen() != true)
                {
                    _sendError = KafkaBrokerStateErrorCode.ConnectionError;
                    return;
                }
            }
            catch (Exception)
            {
                _sendError = KafkaBrokerStateErrorCode.ConnectionError;
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
            catch (Exception)
            {
                //ignored
            }
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
            if (!IsEnabled) return KafkaBrokerErrorCode.InvalidState;

            if (_settings != null && _settings.TransportLatency > TimeSpan.Zero)
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
                if (data == null) return KafkaBrokerErrorCode.DataError;
            }
            catch (Exception)
            {                
                return KafkaBrokerErrorCode.DataError;
            }

            var stream = _connection.GetStream();
            if (stream == null)
            {
                _sendError = KafkaBrokerStateErrorCode.ConnectionError;
                return KafkaBrokerErrorCode.TransportError;
            }            

            var requestState = new RequestState(request, DateTime.UtcNow, timeout);
            if (processResponse)
            {
                _requests[requestId] = requestState;
                _lastActivityTimestampUtc = DateTime.UtcNow;
            }
            
            try
            {
                stream.Write(data, 0, data.Length);
                requestState.SentTimestampUtc = DateTime.UtcNow;
                _sendError = null;
                return requestId;
            }
            catch (Exception)
            {
                _sendError = KafkaBrokerStateErrorCode.IOError;
                return KafkaBrokerErrorCode.TransportError;
            }
        }
       
        public KafkaBrokerResult<TResponse> Receive<TResponse>(int requestId) where TResponse : class, IKafkaResponse
        {
            RequestState requestState;
            if (!_requests.TryGetValue(requestId, out requestState))
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
            var stream = _connection.GetStream();
            if (stream == null)
            {
                _receiveError = KafkaBrokerStateErrorCode.ConnectionError;                
                return;
            }

            var headerBuffer = _responseState.ResponseHeaderBuffer;            

            try
            {
                stream.BeginRead(headerBuffer, 0, headerBuffer.Length, OnReceived, stream);
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerStateErrorCode.IOError;                
            }
        }

        private void OnReceived(IAsyncResult result)
        {
            if (!_isOpenned) return;
            
            try
            {                
                var stream = result?.AsyncState as Stream;
                if (stream == null) return;

                var responseHeader = ReadResponseHeader(stream, result);
                if (responseHeader == null)
                {
                    return;
                }

                _lastActivityTimestampUtc = DateTime.UtcNow;

                var requestId = responseHeader.CorrelationId;
                RequestState requestState;
                if (!_requests.TryGetValue(requestId, out requestState))
                {
                    if (responseHeader.DataSize > 0)
                    {
                        try
                        {
                            var tempBuffer = new byte[responseHeader.DataSize];
                            stream.Read(tempBuffer, 0, tempBuffer.Length);
                        }
                        catch (Exception)
                        {
                            _receiveError = KafkaBrokerStateErrorCode.IOError;                            
                        }
                    }
                    return;
                }

                var response = ReadResponse(stream, requestState, responseHeader);
                if (response == null)
                {
                    return;
                }

                _lastActivityTimestampUtc = DateTime.UtcNow;
                requestState.Response = response;                
                _receiveError = null;
            }
            finally 
            {
                BeginRead();
            }
        }

        [CanBeNull]
        private KafkaResponseHeader ReadResponseHeader([NotNull] Stream stream, IAsyncResult result)
        {
            int responseHeaderSize;
            try
            {
                responseHeaderSize = stream.EndRead(result);
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerStateErrorCode.IOError;
                return null;
            }

            if (responseHeaderSize == 0)
            {
                return null;
            }

            if (responseHeaderSize != _responseState.ResponseHeaderBuffer.Length)
            {
                _receiveError = KafkaBrokerStateErrorCode.DataError;
                return null;
            }
            
            try
            {
                var responseHeader = _kafkaProtocol.ReadResponseHeader(_responseState.ResponseHeaderBuffer, 0, responseHeaderSize);
                if (responseHeader == null)
                {
                    _receiveError = KafkaBrokerStateErrorCode.DataError;
                    return null;
                }
                return responseHeader;                
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerStateErrorCode.DataError;
                return null;
            }
        }

        [CanBeNull]
        private IKafkaResponse ReadResponse([NotNull] Stream stream, [NotNull] RequestState state, [NotNull]KafkaResponseHeader responseHeader)
        {
            var responseBuffer = new byte[responseHeader.DataSize];
            int responseSize;
            try
            {
                responseSize = stream.Read(responseBuffer, 0, responseBuffer.Length);
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerStateErrorCode.IOError;
                state.Error = KafkaBrokerErrorCode.TransportError;
                return null;
            }

            if (responseSize != responseBuffer.Length)
            {
                _receiveError = KafkaBrokerStateErrorCode.DataError;
                state.Error = KafkaBrokerErrorCode.DataError;
                return null;
            }
            
            try
            {
                var response = _kafkaProtocol.ReadResponse(state.Request, responseBuffer, 0, responseBuffer.Length);
                if (response == null)
                {
                    _receiveError = KafkaBrokerStateErrorCode.DataError;
                    state.Error = KafkaBrokerErrorCode.DataError;
                    return null;
                }
                return response;
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerStateErrorCode.DataError;
                state.Error = KafkaBrokerErrorCode.DataError;
                return null;
            }
        }

        #endregion Async receive      

        #region State classes

        private sealed class RequestState
        {            
            [NotNull] public readonly IKafkaRequest Request;            
            public readonly TimeSpan? Timeout;

            public readonly DateTime CreateTimestampUtc;
            public DateTime? SentTimestampUtc;

            [CanBeNull] public IKafkaResponse Response;
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
            [NotNull] public readonly byte[] ResponseHeaderBuffer;            

            public ResponseState([NotNull] KafkaProtocol kafkaProtocol)
            {                
                ResponseHeaderBuffer = new byte[kafkaProtocol.ResponseHeaderSize];
            }
        }

        #endregion State classes
    }
}