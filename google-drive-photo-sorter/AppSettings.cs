public class AppSettings
{
    public string ApplicationName { get; set; }
    public string DriveFolderId { get; set; }
    public int MaxRetries { get; set; }
    public int RetryDelayMs { get; set; }
    public string[] DCIMPaths { get; set; }
    public string[] ExcludedPrefixes { get; set; }
}