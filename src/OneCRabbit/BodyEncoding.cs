using System.Text;

namespace OneCRabbit;

internal static class BodyEncoding
{
    static BodyEncoding() => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    internal static Encoding Utf8 { get; } = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

    internal static Encoding Resolve(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Не задана кодировка.");

        var key = name!.Trim().ToLowerInvariant().Replace("_", "");
        return key switch
        {
            "utf-8" or "utf8" => Utf8,
            "windows-1251" or "win1251" or "cp1251" or "1251" => Encoding.GetEncoding(1251),
            _ => throw new ArgumentException("Кодировка должна быть utf-8 или windows-1251.")
        };
    }

    internal static string CanonicalName(Encoding encoding) =>
        encoding.CodePage == 1251 ? "windows-1251" : "utf-8";
}
