namespace SentinelIngestion;

public class Transaction
{
    public string AccountId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "USD";
    public string MerchantCategory { get; set; } = string.Empty;
}