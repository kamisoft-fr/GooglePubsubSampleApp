namespace Common.Infrastructure.Pubsub.Google;

/// <summary>
/// Delegating handler which enforces that messages are sent with HttpVersionPolicy.RequestVersionExact.
/// </summary>
internal class ExactVersionHandler : DelegatingHandler
{
    internal ExactVersionHandler(HttpMessageHandler handler) : base(handler)
    {
    }

    /// <summary>
    /// Convenience method to be used from GrpcNetClientAdapter.WithAdditionalOptions.
    /// </summary>
    internal static void ModifyGrpcChannelOptions(Grpc.Net.Client.GrpcChannelOptions options) =>
        options.HttpHandler = new ExactVersionHandler(new SocketsHttpHandler { EnableMultipleHttp2Connections = true });

    // Note: gRPC never calls the synchronous method.
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        return base.SendAsync(request, cancellationToken);
    }
}
