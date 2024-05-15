using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Text.Json;
using Encoding = System.Text.Encoding;

namespace Common.Infrastructure.Pubsub.Google;

public class GoogleChannelPublisher(
    ILogger<GoogleChannelPublisher> logger,
    IHostEnvironment hostEnvironment,
    IOptions<PubSubSettings> settings)
    : GooglePubsubBase(settings.Value.ProjectId)
{
    readonly ConcurrentDictionary<string, PublisherClient> _clients = new();
    readonly ConcurrentDictionary<string, Topic> _topics = new();

    public override IHostEnvironment HostEnvironment => hostEnvironment;
    public override ILogger Logger => logger;

    public async Task PublishAsync<T>(T message, string topicName)
    {
        try
        {
            var publisher = _clients.GetOrAdd(topicName, (key) =>
            {
                var topic = _topics.GetOrAdd(topicName, (name) =>
                {
                    TopicName topicNameToCreate = new(settings.Value.ProjectId, name);
                    return CreateTopic(topicNameToCreate);
                });
                PublisherClientBuilder builder = new()
                {
                    TopicName = topic.TopicName
                };

                return builder.Build();
            });

            PubsubMessage pubsubMessage = new()
            {
                Data = message is string msgString
                    ? ByteString.CopyFrom(msgString, Encoding.UTF8)
                    : ByteString.CopyFromUtf8(JsonSerializer.Serialize(message))
            };

            var messageId = await publisher.PublishAsync(pubsubMessage);

            logger.LogDebug(
                "Published message to Google PubSub topic {TopicName} with message id {MessageId}",
                topicName, messageId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish Google PubSub message");
            throw;
        }
    }
}
