using System;

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
        public const UInt32 DefaultPolynomial = 0xedb88320u;
        public const UInt32 DefaultSeed = 0xffffffffu;

        static UInt32[] _defaultTable;

        static KafkaCrc32()
        {
            _defaultTable = InitializeTable(DefaultPolynomial);
        }

        public static UInt32 Compute(byte[] buffer, long offset, long count)
        {
            var crc = DefaultSeed;
            for (var i = offset; i < offset + count; i++)
            {
                unchecked
                {
                    crc = (crc >> 8) ^ _defaultTable[buffer[i] ^ crc & 0xff];
                }
            }
            return ~crc;
        }

        static UInt32[] InitializeTable(UInt32 polynomial)
        {            
            var createTable = new UInt32[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (UInt32)i;
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

