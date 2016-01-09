using System.IO;
using System.IO.Compression;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol
{
    [PublicAPI]
    public static class Compression
    {
        public static byte[] Zip(byte[] bytes)
        {
            using (var destination = new MemoryStream())
            using (var gzip = new GZipStream(destination, CompressionLevel.Fastest, false))
            {
                gzip.Write(bytes, 0, bytes.Length);
                gzip.Flush();
                gzip.Close();
                return destination.ToArray();
            }
        }

        public static byte[] Unzip(byte[] bytes)
        {
            using (var source = new MemoryStream(bytes))
            using (var destination = new MemoryStream())
            using (var gzip = new GZipStream(source, CompressionMode.Decompress, false))
            {
                gzip.CopyTo(destination);
                gzip.Flush();
                gzip.Close();
                return destination.ToArray();
            }
        }
    }
}
