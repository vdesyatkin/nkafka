using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using JetBrains.Annotations;

namespace NKafka.Protocol.Serialization
{
    [PublicAPI]
    public class KafkaBinaryReader : IDisposable
    {
        private const int NullValue = -1;

        [NotNull] private readonly KafkaProtocolSettings _settings;

        [NotNull] private MemoryStream _stream;
        [NotNull] private readonly Stack<long> _beginPositions = new Stack<long>();
        [NotNull] private readonly Stack<int> _sizeValues = new Stack<int>();
        [NotNull] private readonly Stack<uint> _crc32Values = new Stack<uint>();
        [NotNull] private readonly Stack<MemoryStream> _gzipStoredStreams = new Stack<MemoryStream>();

        private readonly DateTime _unixTimeUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public KafkaBinaryReader(byte[] data, int offset, int count, [NotNull] KafkaProtocolSettings settings)
        {
            _settings = settings;
            _stream = new MemoryStream(data ?? new byte[0], offset, count, false, true);
        }

        public bool CanRead()
        {
            return _stream.Position < _stream.Length;
        }
        
        /// <exception cref="KafkaProtocolException"/>
        public IReadOnlyList<T> ReadCollection<T>(Func<KafkaBinaryReader, T> itemReadMethod)
        {
            var size = ReadInt32();
            if (size == NullValue) return null;
            if (size < 0 || size > _settings.CollectionItemCountLimit)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidItemCount);
            }

            var items = new List<T>(size);
            if (itemReadMethod != null)
            {
                for (var i = 0; i < size; i++)
                {
                    var item = itemReadMethod(this);
                    if (item == null) continue;
                    items.Add(item);
                }
            }

            return items;
        }

        public IReadOnlyList<T> ReadCollection<T>(Func<T> itemReadMethod)
        {
            var size = ReadInt32();
            if (size == NullValue) return null;
            if (size < 0 || size > _settings.CollectionItemCountLimit)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidItemCount);
            }

            var items = new List<T>(size);
            if (itemReadMethod != null)
            {
                for (var i = 0; i < size; i++)
                {
                    var item = itemReadMethod();
                    if (item == null) continue;
                    items.Add(item);
                }
            }

            return items;
        }

        public int BeginReadSize()
        {
            var size = ReadInt32();
            _beginPositions.Push(_stream.Position);
            _sizeValues.Push(size);
            
            return size;
        }
        
        public int EndReadSize()
        {
            if (_sizeValues.Count == 0) return 0;

            var requiredSize = _sizeValues.Peek();
            var beginPosition = _beginPositions.Peek();
            var endPosition = _stream.Position;
            var actualSize = endPosition - beginPosition;

            if (actualSize >= requiredSize || endPosition >= _stream.Length)
            {
                _sizeValues.Pop();
                _beginPositions.Pop();                
            }
            return (int)actualSize;
        }

        public uint BeginReadCrc32()
        {
            var crc32 = ReadUInt32();
            _beginPositions.Push(_stream.Position);
            _crc32Values.Push(crc32);
            return crc32;
        }

        public uint EndReadCrc32()
        {
            if (_crc32Values.Count == 0) return 0;

            _crc32Values.Pop();
            var beginPosition = _beginPositions.Pop();
            var endPosition = _stream.Position;

            var actualCrc32 = KafkaCrc32.Compute(_stream.GetBuffer(), beginPosition, endPosition - beginPosition);

            return actualCrc32;
        }

        public int BeginReadCollection()
        {
            var count = ReadInt32();
            return count;
        }

        public int BeginReadGZipData()
        {            
            var size = ReadInt32();
            var beginPosition = _stream.Position;

            var gzipStream = new MemoryStream(size * 2);

            // ReSharper disable once AssignNullToNotNullAttribute
            using (var source = new MemoryStream(_stream.GetBuffer(), (int)_stream.Position, size))
            {                
                using (var gzip = new GZipStream(source, CompressionMode.Decompress, false))
                {
                    gzip.CopyTo(gzipStream);
                    gzip.Flush();
                    gzip.Close();                    
                }
            }

            _beginPositions.Push(beginPosition + size);
            _gzipStoredStreams.Push(_stream);
            gzipStream.Position = 0;            
            _stream = gzipStream;      
            return (int)gzipStream.Length;
        }

        public int EndReadGZipData()
        {
            if (_gzipStoredStreams.Count == 0) return 0;
            if (_beginPositions.Count == 0) return 0;

            if (_stream.Position < _stream.Length)
            {
                return (int)_stream.Position;
            }

            var result = _stream.Position;
            _stream.Dispose();
            // ReSharper disable once AssignNullToNotNullAttribute
            _stream = _gzipStoredStreams.Pop();
            // ReSharper disable once PossibleNullReferenceException
            _stream.Position = _beginPositions.Pop();

            return (int)result;
        }

        public bool ReadBool()
        {
            return ReadInt8() == 1;
        }

        public byte ReadInt8()
        {
            var data = _stream.ReadByte();
            return data >= 0 ? (byte)data : (byte)0;
        }

        public short ReadInt16()
        {
            var bytes = new byte[2];
            _stream.Read(bytes, 0, 2);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt16(bytes, 0);
        }       

        public uint ReadUInt32()
        {
            var bytes = new byte[4];
            _stream.Read(bytes, 0, 4);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToUInt32(bytes, 0);
        }

        public int ReadInt32()
        {
            var bytes = new byte[4];
            _stream.Read(bytes, 0, 4);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt32(bytes, 0);
        }

        public long ReadInt64()
        {
            var bytes = new byte[8];
            _stream.Read(bytes, 0, 8);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            return BitConverter.ToInt64(bytes, 0);
        }

        public DateTime ReadTimestampUtc()
        {
            var milliseconds = ReadInt64();
            return _unixTimeUtc.AddMilliseconds(milliseconds);
        }

        public DateTime? ReadNulalbleTimestampUtc()
        {
            var milliseconds = ReadInt64();
            return milliseconds == NullValue ? (DateTime?)null :_unixTimeUtc.AddMilliseconds(milliseconds);
        }

        public string ReadString()
        {
            var size = ReadInt16();
            if (size == NullValue) return null;
            if (size < 0 || size > _settings.StringSizeByteCountLimit)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidStringSize);
            }

            var bytes = new byte[size];
            _stream.Read(bytes, 0, size);

            return Encoding.UTF8.GetString(bytes);
        }

        public void SkipData(int length)
        {
            if (length <= 0) return;
            _stream.Position = Math.Min(_stream.Position + length, _stream.Length);
            _sizeValues.Pop();
            _beginPositions.Pop();
        }

        public byte[] ReadByteArray()
        {
            var size = ReadInt32();
            if (size == NullValue) return null;
            if (size < 0 || size > _settings.StringSizeByteCountLimit)
            {
                throw new KafkaProtocolException(KafkaProtocolErrorCode.InvalidDataSize);
            }

            var bytes = new byte[size];
            _stream.Read(bytes, 0, size);

            return bytes;
        }        

        public void Dispose()
        {
            _stream.Dispose();

            while (_gzipStoredStreams.Count > 0)
            {
                var storedStream = _gzipStoredStreams.Pop();
                storedStream?.Dispose();
            }            
        }
    }
}