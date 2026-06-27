using System.Text.Json;
using Npgsql;
using Microsoft.AspNetCore.SignalR;

var builder = WebApplication.CreateBuilder(args);

// 1. Configure CORS to allow our future Angular App to establish WebSockets smoothly
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy => policy
        .WithOrigins("http://localhost:4200") // Default Angular Dev Port
        .AllowAnyMethod()
        .AllowAnyHeader()
        .AllowCredentials()); // Crucial for SignalR WebSockets authentication handshake
});

// 2. Register SignalR Services
builder.Services.AddSignalR();

var app = builder.Build();
app.UseCors();

string upstashUrl = "https://flying-wombat-79982.upstash.io/rpush/transaction_queue";
string upstashToken = "gQAAAAAAAThuAAIgcDIyNGYyNjcyNzgyOGI0OTZiYTZjMTc3ZWVhMDY1YTE2NQ";
string dbConnectionString = "postgresql://neondb_owner:npg_se27jIEhkWwz@ep-withered-mouse-aobvm0q3-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require";

// Map SignalR WebSocket Route
app.MapHub<FraudAlertHub>("/stream/alerts");

// 1. Transaction Ingestion Endpoint (Fixed Response Structure)
app.MapPost("/api/ingestion", async (HttpContext context) =>
{
    using var reader = new StreamReader(context.Request.Body);
    string bodyText = await reader.ReadToEndAsync();

    using var client = new HttpClient();
    client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", upstashToken);

    var response = await client.PostAsync(upstashUrl, new StringContent(JsonSerializer.Serialize(bodyText)));
    
    if (response.IsSuccessStatusCode)
    {
        // Fixed: Use Results.Json to return an anonymous object payload with an Accepted (202) status code
        return Results.Json(new { message = "Transaction received and queued via REST successfully." }, statusCode: 202);
    }
    return Results.BadRequest(new { error = "Upstash communication failure." });
});

// 2. Fetch Unreviewed Alerts for Dashboard
app.MapGet("/api/compliance/alerts", async () =>
{
    var audits = new List<object>();
    using var conn = new NpgsqlConnection(dbConnectionString);
    await conn.OpenAsync();

    string selectQuery = @"
        SELECT a.audit_id, t.account_id, t.amount, t.merchant_category, a.ai_summary, a.structured_mitigation
        FROM compliance_audits a
        JOIN transactions t ON a.transaction_id = t.transaction_id
        WHERE a.reviewed_by_human = false
        ORDER BY a.created_at DESC;";

    using var cmd = new NpgsqlCommand(selectQuery, conn);
    using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        audits.Add(new
        {
            AuditId = reader.GetGuid(0),
            AccountId = reader.GetString(1),
            Amount = reader.GetDecimal(2),
            MerchantCategory = reader.GetString(3),
            AiSummary = reader.GetString(4),
            MitigationBlueprint = reader.GetString(5)
        });
    }

    return Results.Ok(audits);
});

// 3. Dashboard Action (Allows human reviewer to resolve flags)
app.MapPost("/api/compliance/review/{id}", async (string id) =>
{
    using var conn = new NpgsqlConnection(dbConnectionString);
    await conn.OpenAsync();

    string updateQuery = "UPDATE compliance_audits SET reviewed_by_human = true WHERE audit_id = @id::uuid;";
    using var cmd = new NpgsqlCommand(updateQuery, conn);
    cmd.Parameters.AddWithValue("id", id);
    await cmd.ExecuteNonQueryAsync();

    return Results.Ok(new { message = "Audit status updated successfully." });
});

// 4. Webhook Action Receiver (Invoked by Python Risk Engine)
app.MapPost("/api/worker/action", async (HttpContext context, IHubContext<FraudAlertHub> hubContext) =>
{
    using var reader = new StreamReader(context.Request.Body);
    string bodyText = await reader.ReadToEndAsync();
    
    var payload = JsonSerializer.Deserialize<JsonElement>(bodyText);
    
    // Broadcast the raw alert via SignalR straight to our future Angular front-end clients
    await hubContext.Clients.All.SendAsync("ReceiveRiskAlert", payload);
    
    Console.WriteLine($"📢 Real-time alert streamed via SignalR WebSocket for account: {payload.GetProperty("account_id")}");
    return Results.Ok(new { broadcasted = true });
});

app.Run();

// --- TYPE/CLASS DECLARATIONS MUST GO AT THE ABSOLUTE BOTTOM ---
class FraudAlertHub : Hub 
{
    // Keeping this empty allows arbitrary real-time client subscriptions
}