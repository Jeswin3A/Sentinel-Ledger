using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;
using System.Text.Json;

namespace SentinelIngestion.Controllers;

[ApiController]
[Route("api/ingestion")] // Forced explicit lowercase route
public class IngestionController : ControllerBase
{
    private readonly IDatabase _redisDb;
    private const string QueueName = "transaction_queue";

    // Injecting Redis Connection
    public IngestionController(IConnectionMultiplexer redis)
    {
        _redisDb = redis.GetDatabase();
    }

    [HttpPost]
    public async Task<IActionResult> IngestTransaction([FromBody] Transaction transaction)
    {
        // 1. Basic validation guardrail
        if (string.IsNullOrEmpty(transaction.AccountId) || transaction.Amount <= 0)
        {
            return BadRequest(new { message = "Invalid transaction payload parameters." });
        }

        // 2. Serialize the object to JSON string
        string payload = JsonSerializer.Serialize(transaction);

        // 3. Push to Redis List (acting as our message queue FIFO)
        // ListRightPush returns instantly without blocking
        await _redisDb.ListRightPushAsync(QueueName, payload);

        // 4. Return HTTP 202 Accepted (Non-blocking handoff completed)
        return Accepted(new { message = "Transaction received and queued for compliance processing." });
    }
}