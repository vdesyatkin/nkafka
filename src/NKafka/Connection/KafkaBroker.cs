using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Protocol;

namespace NKafka.Connection
{
    internal sealed class KafkaBroker
    {
        [PublicAPI]
        public bool IsOpenned { get; private set; }

        [PublicAPI]
        public KafkaBrokerErrorCode? Error => _sendError ?? _receiveError;
        [PublicAPI]
        public string Name { get; }

        [NotNull] private readonly KafkaConnection _connection;
        [NotNull] private readonly KafkaProtocol _kafkaProtocol;        

        [NotNull] private readonly ConcurrentDictionary<int, RequestState> _requests;
        [NotNull] private readonly ResponseState _responseState;

        private KafkaBrokerErrorCode? _sendError;
        private KafkaBrokerErrorCode? _receiveError;

        private int _currentRequestId;        

        public KafkaBroker([NotNull] KafkaConnection connection, [NotNull] KafkaProtocol kafkaProtocol,
            [CanBeNull] string name)
        {
            _connection = connection;
            _kafkaProtocol = kafkaProtocol;
            Name = name;
            
            _requests = new ConcurrentDictionary<int, RequestState>();
            _responseState = new ResponseState(kafkaProtocol);
            
            IsOpenned = false;            
        }

        public void Open()
        {            
            _sendError = TryOpenConnection();            
            IsOpenned = true;            
        }

        public void Close()
        {
            IsOpenned = false;
            CloseConnection();
            _sendError = null;
            _receiveError = null;            
        }

        public void Maintenance()
        {
            if (!IsOpenned) return;
            //todo set timeout error as response
            //todo reconnect on error
            //todo heartbeat
            //todo clear old requests?
        }

        public int? Send<TRequest>(TRequest request, int? dataCapacity = null) where TRequest : class, IKafkaRequest
        {
            if (request == null) return null;            

            var requestId = Interlocked.Increment(ref _currentRequestId);

            byte[] data;
            try
            {
                data = _kafkaProtocol.WriteRequest(request, requestId, dataCapacity);
                if (data == null) return null;
            }
            catch (Exception)
            {
                //todo errors
                return null;
            }

            var stream = _connection.GetStream();
            if (stream == null)
            {
                _sendError = KafkaBrokerErrorCode.ConnectionError;
                return null;
            }
            
            var requestState = new RequestState(request, stream);
            _requests[requestId] = requestState;

            try
            {
                stream.BeginWrite(data, 0, data.Length, OnSent, requestState);
            }
            catch (Exception)
            {
                _sendError = KafkaBrokerErrorCode.IOError;
            }
            return requestId;
        }

        private void OnSent(IAsyncResult result)
        {
            var requestState = result?.AsyncState as RequestState;
            try
            {
                requestState?.Stream.EndWrite(result);
                _sendError = null;
            }
            catch (Exception)
            {
                _sendError = KafkaBrokerErrorCode.IOError;
            }            
        }

        public TResponse Receive<TResponse>(int requestId) where TResponse : class, IKafkaResponse
        {
            RequestState requestState;
            if (!_requests.TryGetValue(requestId, out requestState))
            {
                return null;
            }

            var response = requestState.Response as TResponse;
            if (response != null)
            {
                
            }
            return response;
        }

        private void BeginRead()
        {            
            var stream = _connection.GetStream();
            if (stream == null)
            {
                _receiveError = KafkaBrokerErrorCode.ConnectionError;
                return;
            }

            var headerBuffer = _responseState.ResponseHeaderBuffer;            

            try
            {
                stream.BeginRead(headerBuffer, 0, headerBuffer.Length, OnReceived, stream);
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerErrorCode.IOError;
            }
        }

        private void OnReceived(IAsyncResult result)
        {
            try
            {                
                var stream = result?.AsyncState as Stream;
                if (stream == null) return;

                var responseHeader = ReadResponseHeader(stream, result);
                if (responseHeader == null)
                {
                    return;
                }                

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
                            _receiveError = KafkaBrokerErrorCode.IOError;
                        }
                    }
                    return;
                }

                var response = ReadResponse(stream, requestState.Request, responseHeader);
                if (response == null)
                {
                    return;
                }

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
                _receiveError = KafkaBrokerErrorCode.IOError;
                return null;
            }

            if (responseHeaderSize == 0)
            {
                return null;
            }

            if (responseHeaderSize != _responseState.ResponseHeaderBuffer.Length)
            {
                _receiveError = KafkaBrokerErrorCode.DataError;
                return null;
            }
            
            try
            {
                var responseHeader = _kafkaProtocol.ReadResponseHeader(_responseState.ResponseHeaderBuffer, 0, responseHeaderSize);
                if (responseHeader == null)
                {
                    _receiveError = KafkaBrokerErrorCode.DataError;
                    return null;
                }
                return responseHeader;                
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerErrorCode.DataError;
                return null;
            }
        }

        [CanBeNull]
        private IKafkaResponse ReadResponse([NotNull] Stream stream, [NotNull] IKafkaRequest request, [NotNull]KafkaResponseHeader responseHeader)
        {
            var responseBuffer = new byte[responseHeader.DataSize];
            int responseSize;
            try
            {
                responseSize = stream.Read(responseBuffer, 0, responseBuffer.Length);
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerErrorCode.IOError;
                return null;
            }

            if (responseSize != responseBuffer.Length)
            {
                _receiveError = KafkaBrokerErrorCode.DataError;
                return null;
            }
            
            try
            {
                var response = _kafkaProtocol.ReadResponse(request, responseBuffer, 0, responseBuffer.Length);
                if (response == null)
                {
                    _receiveError = KafkaBrokerErrorCode.DataError;
                    return null;
                }
                return response;
            }
            catch (Exception)
            {
                _receiveError = KafkaBrokerErrorCode.DataError;
                return null;
            }
        }

        private KafkaBrokerErrorCode? TryOpenConnection()
        {            
            if (_connection.TryOpen() != true)
            {
                return KafkaBrokerErrorCode.ConnectionError;
            }
            
            BeginRead();
            return null;
        }

        private void CloseConnection()
        {
            _connection.Close();
        }

        private sealed class RequestState
        {            
            [NotNull] public readonly IKafkaRequest Request;            
            [NotNull] public readonly Stream Stream;            

            [CanBeNull] public IKafkaResponse Response;            

            public RequestState([NotNull] IKafkaRequest request, [NotNull] Stream stream)
            {
                Request = request;                
                Stream = stream;                
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
    }
}
