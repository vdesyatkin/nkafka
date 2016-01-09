using System;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol
{
    [PublicAPI]
    public struct OffsetTime
    {
        public OffsetMode? Mode { get; private set; }
        public TimeSpan? Time { get; private set; }
        public long TimeMs { get; private set; }

        public OffsetTime(OffsetMode mode) : this()
        {
            Mode = mode;
            TimeMs = (long)mode;
        }

        public OffsetTime(TimeSpan time): this()
        {
            Time = time;
            TimeMs = (long)Math.Round(time.TotalMilliseconds);
        }

        public readonly static OffsetTime TheLatest = new OffsetTime(OffsetMode.TheLatest);
        public readonly static OffsetTime TheEarliest = new OffsetTime(OffsetMode.TheEarliest);
    }
}
