using System.Reflection;

namespace OneCRabbit;

internal static class DependencyResolver
{
    private static bool _registered;

    internal static void Register()
    {
        if (_registered)
            return;

        _registered = true;
        var directory = Path.GetDirectoryName(typeof(DependencyResolver).Assembly.Location);
        if (string.IsNullOrEmpty(directory))
            return;

        AppDomain.CurrentDomain.AssemblyResolve += (_, args) =>
        {
            var name = new AssemblyName(args.Name).Name;
            if (string.IsNullOrEmpty(name))
                return null;

            var path = Path.Combine(directory, name + ".dll");
            return File.Exists(path) ? Assembly.LoadFrom(path) : null;
        };
    }
}
