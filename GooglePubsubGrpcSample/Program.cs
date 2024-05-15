using Common.Infrastructure.Pubsub;
using Common.Infrastructure.Pubsub.Google;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

//Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", "localhost:8085");
Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", "development");

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddInMemoryCollection(
    [
        new KeyValuePair<string, string>("Logging:LogLevel:Default","Trace")
    ]);

var services = builder.Services;

builder.Logging.AddSimpleConsole(options =>
{
    options.IncludeScopes = true;
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
});

services.AddOptions<PubSubSettings>().Configure(settings => settings.ProjectId = "YOUR PROJECT ID");

services.AddSingleton<GoogleChannelPublisher>();
services.AddSingleton<GoogleChannelSubscriber>();

IHost host = builder.Build();

var logger = host.Services.GetRequiredService<ILogger<Program>>();

var topicName = "configuration";

var subscriber = host.Services.GetRequiredService<GoogleChannelSubscriber>();
var subscription = await subscriber.SubscribeStringAsync((message, ct) =>
{
    logger.LogInformation("Received message: {Message}", message);
    return Task.FromResult(true);
}, topicName, "test-subscription");

// not even necessary to publish any message to get the warnings, just wait 1 minute

//var publisher = host.Services.GetRequiredService<GoogleChannelPublisher>();
//await publisher.PublishAsync("test message", topicName);
//await Task.Delay(1000);
//await publisher.PublishAsync("test message2", topicName);
//await Task.Delay(1000);
//await publisher.PublishAsync("test message3", topicName);
//await Task.Delay(1000);

host.Run();