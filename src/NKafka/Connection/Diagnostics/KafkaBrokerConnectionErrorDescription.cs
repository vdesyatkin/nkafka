using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    [PublicAPI]
    public enum KafkaBrokerConnectionErrorDescription
    {
        Maintenance = 0,
        OpenConnection = 1,
        CloseConnection = 2,        
        CheckDataAvailability = 3,
        SendRequest = 4,
        ReceiveResponseHeader = 5,
        ReceiveResponse = 6,
        ReceiveUnexpectedResponse = 7
    }
}
