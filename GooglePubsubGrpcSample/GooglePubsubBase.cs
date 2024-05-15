using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Common.Infrastructure.Pubsub.Google;

public abstract class GooglePubsubBase(string projectId)
{
    public abstract IHostEnvironment HostEnvironment { get; }
    public abstract ILogger Logger { get; }

    protected SubscriberClient CreateSubscriberClient(SubscriptionName subscriptionName)
    {
        SubscriberClientBuilder clientBuilder = new()
        {
            GrpcAdapter = GrpcNetClientAdapter.Default.WithAdditionalOptions(ExactVersionHandler.ModifyGrpcChannelOptions),
            SubscriptionName = subscriptionName,
            Logger = Logger
        };

        return clientBuilder.Build();
    }

    protected Topic CreateTopic(TopicName topicName)
    {
        PublisherServiceApiClient publisherApiClient = new PublisherServiceApiClientBuilder().Build();

        try
        {
            var existing = publisherApiClient.GetTopic(new GetTopicRequest { TopicAsTopicName = topicName });
            Logger.LogDebug("[GooglePubSub] Topic already exists: {TopicName}", topicName);
            return existing;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            Logger.LogInformation("[GooglePubSub] Creating Topic {TopicName}", topicName);
            return publisherApiClient.CreateTopic(topicName);
        }
    }
}
