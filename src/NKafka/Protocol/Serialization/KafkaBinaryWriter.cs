using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using JetBrains.Annotations;

namespace NKafka.Protocol.Serialization
{
    [PublicAPI]
    internal class KafkaBinaryWriter : IDisposable
    {        
        [NotNull] private readonly MemoryStream _stream;
        [NotNull] private readonly Stack<long> _beginPositions = new Stack<long>();

        private const int NullValue = -1;   
          
        public KafkaBinaryWriter(int? capacity = null)
        {
            _stream = capacity.HasValue ? new MemoryStream(capacity.Value) : new MemoryStream();            
        }

        public byte[] ToByteArray()
        {            
            return _stream.ToArray();
        }

        public void WriteCollection<T>(IReadOnlyList<T> collection, Action<KafkaBinaryWriter, T> itemWriteMethod)
        {
            if (collection == null || itemWriteMethod == null)
            {
                WriteInt32(-1);
                return;
            }

            var countPosition = _stream.Position;
            WriteInt32(0);

            var count = 0;
            
            foreach (var item in collection)
            {
                if (item == null) continue;
                itemWriteMethod.Invoke(this, item);
                count++;
            }           

            var endPosition = _stream.Position;
            _stream.Position = countPosition;
            WriteInt32(count);
            _stream.Position = endPosition;
        }

        public void WriteCollection<T>(IReadOnlyList<T> collection, Action<T> itemWriteMethod)
        {
            if (collection == null || itemWriteMethod == null)
            {
                WriteInt32(-1);
                return;
            }

            var countPosition = _stream.Position;
            WriteInt32(0);

            var count = 0;            
            foreach (var item in collection)
            {
                if (item == null) continue;
                itemWriteMethod.Invoke(item);
                count++;
            }
            
            var endPosition = _stream.Position;
            _stream.Position = countPosition;
            WriteInt32(count);
            _stream.Position = endPosition;
        }

        public void BeginWriteSize()
        {
            WriteInt32(0); // size
            _beginPositions.Push(_stream.Position);
        }

        public void EndWriteSize()
        {
            var beginPosition = _beginPositions.Pop();
            var endPosition = _stream.Position;

            var size = (int)(endPosition - beginPosition);

            _stream.Position = beginPosition - 4;
            WriteInt32(size);
            _stream.Position = endPosition;
        }

        public void BeginWriteCrc2()
        {
            WriteUInt32(32); // crc32s
            _beginPositions.Push(_stream.Position);
        }

        public void EndWriteCrc2()
        {
            var beginPosition = _beginPositions.Pop();
            var endPosition = _stream.Position;

            var crc32 = KafkaCrc32.Compute(_stream.GetBuffer(), beginPosition, endPosition - beginPosition);

            _stream.Position = beginPosition - 4;
            WriteUInt32(crc32);
            _stream.Position = endPosition;
        }    
        
        public void BeginWriteGZipData()
        {
            WriteUInt32(0);
            _beginPositions.Push(_stream.Position);            
        } 
        
        public void EndWriteGZipData()
        {
            var beginPosition = _beginPositions.Pop();
            var endPosition = _stream.Position;
            var size = (int)(endPosition - beginPosition);

            byte[] gzipData;
            using (var destination = new MemoryStream(size))
            {
                using (var gzip = new GZipStream(destination, CompressionLevel.Fastest, false))
                {
                    // ReSharper disable once AssignNullToNotNullAttribute
                    gzip.Write(_stream.GetBuffer(), (int)beginPosition, size);
                    gzip.Flush();
                    gzip.Close();
                }
                gzipData = destination.ToArray();
            }

            _stream.Position = beginPosition - 4;
            WriteInt32(gzipData.Length);

            _stream.Position = beginPosition;
            WriteByteArray(gzipData);
            if (_stream.Position < _stream.Length)
            {
                _stream.SetLength(_stream.Position);
            }
        } 

        public void WriteInt8(byte data)
        {            
            _stream.WriteByte(data);            
        }

        public void WriteNullableInt16(short? data)
        {
            WriteInt16(data ?? NullValue);
        }

        public void WriteInt16(short data)
        {            
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            _stream.Write(bytes, 0, bytes.Length);
        }

        public void WriteNullableInt32(int? data)
        {
            WriteInt32(data ?? NullValue);
        }

        public void WriteInt32(int data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            _stream.Write(bytes, 0, bytes.Length);
        }      

        public void WriteUInt32(uint data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            _stream.Write(bytes, 0, bytes.Length);
        }

        public void WriteNullableInt64(long? data)
        {
            WriteInt64(data ?? NullValue);
        }

        public void WriteInt64(long data)
        {
            var bytes = BitConverter.GetBytes(data);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            _stream.Write(bytes, 0, bytes.Length);
        }

        public void WriteString(string data)
        {
            if (data == null)
            {
                WriteInt16(NullValue);
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(data);
            WriteInt16((short)bytes.Length);
            _stream.Write(bytes, 0, bytes.Length);
        }

        public void WriteByteArray(byte[] data)
        {
            if (data == null)
            {
                WriteInt32(NullValue);
                return;
            }

            WriteInt32(data.Length);
            _stream.Write(data, 0, data.Length);
        }

        public void Dispose()
        {            
            _stream.Dispose();
        }
    }
}