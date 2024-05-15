using Assi.SlugifyExtension.Core;

namespace Common.Infrastructure.Pubsub.Google;

public static class GooglePubsubExtensions
{
    public static string ToSlug(this string textToSlug, int maxLength = 1000) => textToSlug.Slugify(maxLength);
}
