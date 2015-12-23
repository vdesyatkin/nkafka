using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Metadata;
using NKafka.Protocol;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Connection
{
    internal sealed class KafkaBroker
    {
        [PublicAPI]
        public bool IsOpenned => _isOpenned;

        [PublicAPI]
        public bool IsEnabled => _isOpenned && !_isConnectionMaintenance;

        [PublicAPI]
        public KafkaBrokerStateErrorCode? Error => _sendError ?? _receiveError;


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
                if (request.Response == null && request.Error == null)
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
            var reconnectionPeriod = Error != null
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
                var heartbeatResult = GetTopicMetadata(heartbeatRequestId.Value);
                if (heartbeatResult.HasData || heartbeatResult.HasError)
                {
                    _heartbeatRequestId = null;
                }
            }

            //hearbeat: send request if needed (and haven't sent already)
            var heartbeatPeriod = Error != null && _heartbeatRequestId == null
                ? _settings?.HeartbeatPeriod
                : null;

            if (heartbeatPeriod != null && DateTime.UtcNow - _lastActivityTimestampUtc >= heartbeatPeriod.Value)
            {                
                _heartbeatRequestId = RequestTopicMetadata("_hearbeat", heartbeatPeriod.Value).Data;
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

        public KafkaBrokerResult<int?> Send<TRequest>(TRequest request, TimeSpan timeout, int? dataCapacity = null) where TRequest : class, IKafkaRequest
        {
            if (request == null) return KafkaBrokerErrorCode.BadRequest;
            if (!IsEnabled) return KafkaBrokerErrorCode.InvalidState;

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
            
            var requestState = new RequestState(request, stream, DateTime.UtcNow, timeout);
            _requests[requestId] = requestState;

            _lastActivityTimestampUtc = DateTime.UtcNow;
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

        #region Metadata

        public KafkaBrokerResult<int?> RequestTopicMetadata(string topicName, TimeSpan timeout)
        {
            return Send(new KafkaTopicMetadataRequest(new[] { topicName }), timeout);
        }

        public KafkaBrokerResult<KafkaTopicMetadata> GetTopicMetadata(int requestId)
        {
            var response = Receive<KafkaTopicMetadataResponse>(requestId);
            return ConvertMetadata(response);
        }

        private static KafkaBrokerResult<KafkaTopicMetadata> ConvertMetadata(KafkaBrokerResult<KafkaTopicMetadataResponse> response)
        {
            if (!response.HasData) return response.Error;

            var responseData = response.Data;            
            var responseBrokers = responseData.Brokers ?? new KafkaTopicMetadataResponseBroker[0];
            var responseTopics = responseData.Topics ?? new KafkaTopicMetadataResponseTopic[0];

            if (responseTopics.Count < 1) return KafkaBrokerErrorCode.DataError;
            var responseTopic = responseTopics[0];
            if (string.IsNullOrEmpty(responseTopic?.TopicName)) return KafkaBrokerErrorCode.DataError;

            //todo (E009) handling standard errors (responseTopic.ErrorCode)
            var responsePartitons = responseTopic.Partitions ?? new KafkaTopicMetadataResponseTopicPartition[0];

            var brokers = new List<KafkaBrokerMetadata>(responseBrokers.Count);
            foreach (var responseBroker in responseBrokers)
            {
                if (responseBroker == null) continue;                
                brokers.Add(new KafkaBrokerMetadata(responseBroker.BrokerId, responseBroker.Host, responseBroker.Port));
            }

            var partitions = new List<KafkaTopicPartitionMetadata>(responsePartitons.Count);
            foreach (var responsePartition in responsePartitons)
            {
                if (responsePartition == null) continue;
                //todo (E009) handling standard errors (responsePartition.ErrorCode)
                partitions.Add(new KafkaTopicPartitionMetadata(responsePartition.PartitionId, responsePartition.LeaderId));
            }

            return new KafkaTopicMetadata(responseTopic.TopicName, brokers, partitions);
        }

        #endregion Metadata

        #region State classes

        private sealed class RequestState
        {            
            [NotNull] public readonly IKafkaRequest Request;            
            [NotNull] public readonly Stream Stream;
            public readonly TimeSpan Timeout;

            public readonly DateTime CreateTimestampUtc;
            public DateTime? SentTimestampUtc;

            [CanBeNull] public IKafkaResponse Response;
            public KafkaBrokerErrorCode? Error;

            public RequestState([NotNull] IKafkaRequest request, [NotNull] Stream stream, DateTime createTimestampUtc, TimeSpan timeout)
            {
                Request = request;                
                Stream = stream;
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