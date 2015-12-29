namespace NKafka.Protocol.Serialization
{
    /// <summary>
    /// Implements a 32-bit CRC hash algorithm compatible with Zip etc.
    /// </summary>
    /// <remarks>
    /// Crc32 should only be used for backward compatibility with older file formats
    /// and algorithms. It is not secure enough for new applications.
    /// If you need to call multiple times for the same data either use the HashAlgorithm
    /// interface or remember that the result of one Compute call needs to be ~ (XOR) before
    /// being passed in as the seed for the next Compute call.
    /// </remarks>
    internal static class KafkaCrc32
    {
        private const uint DefaultPolynomial = 0xedb88320u;
        private const uint DefaultSeed = 0xffffffffu;

        static readonly uint[] DefaultTable;

        static KafkaCrc32()
        {
            DefaultTable = InitializeTable(DefaultPolynomial);
        }

        public static uint Compute(byte[] buffer, long offset, long count)
        {
            var crc = DefaultSeed;
            for (var i = offset; i < offset + count; i++)
            {
                unchecked
                {
                    crc = (crc >> 8) ^ DefaultTable[buffer[i] ^ crc & 0xff];
                }
            }
            return ~crc;
        }

        static uint[] InitializeTable(uint polynomial)
        {            
            var createTable = new uint[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (uint)i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry = entry >> 1;
                createTable[i] = entry;
            }            

            return createTable;
        }
    }
}

