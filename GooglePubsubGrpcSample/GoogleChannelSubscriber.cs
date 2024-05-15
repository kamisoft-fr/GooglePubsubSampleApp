using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Meziantou.Framework;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using Encoding = System.Text.Encoding;

namespace Common.Infrastructure.Pubsub.Google;

public class GoogleChannelSubscriber(
    ILogger<GoogleChannelSubscriber> logger,
    IHostEnvironment hostEnvironment,
    IOptions<PubSubSettings> settings)
    : GooglePubsubBase(settings.Value.ProjectId)
{
    readonly ConcurrentDictionary<string, SubscriberClient> _clients = new();
    private readonly string _processName = hostEnvironment.ApplicationName.ToSlug();
    readonly ConcurrentDictionary<string, Subscription> _subscriptions = new();
    readonly ConcurrentDictionary<string, Topic> _topics = new();

    public override IHostEnvironment HostEnvironment => hostEnvironment;
    public override ILogger Logger => logger;

    public Task<string> SubscribeStringAsync(Func<string, CancellationToken, Task<bool>> onMessageReceived,
       string topicName, string subscriptionName)
    {
        InitPubsubSubscription(topicName, subscriptionName,
            out var subscriptionId, out var subscriber);
        StartSubscriberListening(onMessageReceived, subscriber);
        return Task.FromResult(subscriptionId);
    }

    private void StartSubscriberListening(
        Func<string, CancellationToken, Task<bool>> messageHandler,
        SubscriberClient subscriber)
    {
        subscriber.StartAsync(async (message, cancellationToken) =>
            {
                try
                {
                    var text = Encoding.UTF8.GetString(message.Data.ToArray());
                    logger.LogDebug("[GooglePubSub] Received : {PubsubText}", text);
                    var processed = await messageHandler(text, cancellationToken);
                    if (!processed)
                    {
                        logger.LogWarning("[GooglePubSub] Message not processed, sending explicit Nack reply");
                        return SubscriberClient.Reply.Nack;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[GooglePubSub] Error processing message");
                }

                return SubscriberClient.Reply.Ack;
            })
            .ContinueWith(ManageSubscribeError)
            .Forget();

        logger.LogInformation("Machine {MachineName} has subscribed to {SubscriptionName}", Environment.MachineName,
            subscriber.SubscriptionName);

        void ManageSubscribeError(Task listenTask)
        {
            if (listenTask.IsFaulted)
            {
                var baseEx = listenTask.Exception.GetBaseException();
                if (baseEx is OperationCanceledException)
                {
                    logger.LogInformation("Closed client {PubSubClient} : cancellation occured",
                        subscriber.SubscriptionName);
                }
                else
                {
                    logger.LogCritical(listenTask.Exception, "Google Pubsub Failure on {PubSubClient}",
                        subscriber.SubscriptionName);
                }
            }
            else
            {
                logger.LogInformation("Closed client {PubSubClient}", subscriber.SubscriptionName);
            }
        }
    }

    private void InitPubsubSubscription(
        string topicName, string subscriptionName,
         out string subscriptionId,
        out SubscriberClient subscriber)
    {
        var topic = GetOrCreateTopic(topicName);

        subscriptionId = $"{subscriptionName}-{hostEnvironment.EnvironmentName}";
        subscriber = GetOrCreateSubscriber(subscriptionId, topic);
    }

    private Topic GetOrCreateTopic(string topicName) =>
        _topics.GetOrAdd(topicName, name =>
        {
            TopicName topicNameToCreate = new(settings.Value.ProjectId, name);
            return CreateTopic(topicNameToCreate);
        });

    private SubscriberClient GetOrCreateSubscriber(string subscriptionId, Topic topic)
    {
        Subscription subscription = _subscriptions.GetOrAdd(subscriptionId, (name) =>
        {
            SubscriptionName subscriptionName = new(settings.Value.ProjectId, name);
            return CreateSubscription(hostEnvironment.EnvironmentName, topic.TopicName, subscriptionName);
        });

        var subscriber = _clients.GetOrAdd(subscriptionId,
            name => CreateSubscriberClient(subscription.SubscriptionName));
        return subscriber;
    }

    protected Subscription CreateSubscription(string env, TopicName topicName, SubscriptionName subscriptionName)
    {
        var subscriberService = new SubscriberServiceApiClientBuilder().Build();

        try
        {
            Logger.LogDebug("[GooglePubSub] Topic {TopicName} : Get or Create Subscription {SubscriptionName}",
                topicName, subscriptionName);
            var existing =
                subscriberService.GetSubscription(new GetSubscriptionRequest
                {
                    SubscriptionAsSubscriptionName = subscriptionName
                });
            Logger.LogDebug("[GooglePubSub] Subscription already exists: {SubscriptionName}", subscriptionName);
            return existing;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            Logger.LogInformation("[GooglePubSub] Topic {TopicName} : Creating Subscription {SubscriptionName}",
                topicName, subscriptionName);

            Subscription subscriptionRequest = new()
            {
                SubscriptionName = subscriptionName,
                TopicAsTopicName = topicName
            };
            return subscriberService.CreateSubscription(subscriptionRequest);
        }
    }
}
