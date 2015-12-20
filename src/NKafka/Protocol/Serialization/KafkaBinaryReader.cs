using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace NKafka.Protocol.Serialization
{
    internal class KafkaBinaryReader : IDisposable
    {
        private const int NullValue = -1;

        private MemoryStream _stream;
        private readonly Stack<long> _beginPositions = new Stack<long>();
        private readonly Stack<int> _sizeValues = new Stack<int>();
        private readonly Stack<uint> _crc32Values = new Stack<uint>();
        private readonly Stack<MemoryStream> _gzipStoredStreams = new Stack<MemoryStream>();

        public KafkaBinaryReader(byte[] data, int offset, int count)
        {
            _stream = new MemoryStream(data, offset, count);
        }      

        public IReadOnlyList<T> ReadCollection<T>(Func<KafkaBinaryReader, T> itemReadMethod)
        {
            var size = ReadInt32();
            if (size == NullValue || size < 0) return null; //todo upper limit

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
            if (size == NullValue || size < 0) return null; //todo upper limit

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

        public bool EndReadSize()
        {
            if (_sizeValues.Count == 0) return true;

            var size = _sizeValues.Pop();
            var beginPosition = _beginPositions.Pop();
            var endPosition = _stream.Position;
            var actualSize = endPosition - beginPosition;

            return size == actualSize;
        }

        public uint BeginReadCrc32()
        {
            var crc32 = ReadUInt32();
            _beginPositions.Push(_stream.Position);
            _crc32Values.Push(crc32);
            return crc32;
        }

        public bool EndReadCrc32()
        {
            if (_crc32Values.Count == 0) return true;

            var crc32 = _crc32Values.Pop();
            var beginPosition = _beginPositions.Pop();
            var endPosition = _stream.Position;

            var actualCrc32 = KafkaCrc32.Compute(_stream.GetBuffer(), beginPosition, endPosition);

            return crc32 == actualCrc32;
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
            return size;
        }

        public bool EndReadGZipData()
        {
            if (_gzipStoredStreams.Count == 0) return true;
            if (_beginPositions.Count == 0) return true;

            if (_stream.Position < _stream.Length)
            {
                return false;
            }

            _stream.Dispose();
            _stream = _gzipStoredStreams.Pop();
            _stream.Position = _beginPositions.Pop();

            return true;
        }

        public byte ReadInt8()
        {
            return (byte)_stream.ReadByte();
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

        public string ReadString()
        {
            var size = ReadInt16();
            if (size == NullValue) return null;

            var bytes = new byte[size];
            _stream.Read(bytes, 0, size);

            return Encoding.UTF8.GetString(bytes);
        }

        public byte[] ReadByteArray()
        {
            var size = ReadInt32();
            if (size == NullValue || size < 0) return null; //todo upper limit

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
                storedStream.Dispose();
            }            
        }
    }
}