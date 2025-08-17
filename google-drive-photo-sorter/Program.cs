using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using MediaDevices;
using Microsoft.Extensions.Configuration;
using Google.Apis.Upload;
using Google;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace GoogleDrivePhotoSorter
{
    class Program
    {
        // Global configuration object.
        static IConfigurationRoot _config;
        private static AppSettings _appSettings;

        // Use full Drive scope so you can create folders and upload files.
        static readonly string[] Scopes = { DriveService.Scope.Drive };

        // Constants
        private const string CredentialsPath = "credentials.json";
        private const string TokenPath = "token.json";
        private const int DefaultPageSize = 1000;
        private const string FolderMimeType = "application/vnd.google-apps.folder";

        static async Task Main(string[] args)
        {
            ConfigureLogger();
            LoadConfiguration();

            var globalStopwatch = System.Diagnostics.Stopwatch.StartNew();
            Log.Information("Starting Google Drive Photo Sorter...");

            await SortPhotosAsync(CancellationToken.None).ConfigureAwait(false);

            globalStopwatch.Stop();
            Log.Information($"Total time taken: {FormatElapsed(globalStopwatch.Elapsed)}");
            Log.Information("Press any key to exit");
            Console.ReadKey();
        }

        private static void ConfigureLogger()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(theme: SystemConsoleTheme.Colored)
                .CreateLogger();
        }

        private static string FormatElapsed(TimeSpan elapsed) =>
            $"{elapsed.Hours} hours, {elapsed.Minutes} minutes, {elapsed.Seconds} seconds";

        static async Task SortPhotosAsync(CancellationToken cancellationToken)
        {
            // Locate the device.
            using var device = GetDevice();
            if (device == null)
            {
                Log.Error("No device found or device selection was cancelled.");
                return;
            }
            device.Connect();

            // Authenticate with Google Drive.
            var driveService = await AuthenticateDriveServiceAsync(cancellationToken).ConfigureAwait(false);
            var driveRootDisplayName = await GetDriveRootDisplayNameAsync(driveService, cancellationToken).ConfigureAwait(false);
            var allFiles = await GetAllFilesRecursivelyAsync(driveService, _appSettings.DriveFolderId, cancellationToken).ConfigureAwait(false);
            var files = GetFiles(_config, device);
            Log.Information($"Total phone files to process: {files.Count}");

            // Cache for created folder IDs to avoid redundant Drive API calls
            var folderIdCache = new ConcurrentDictionary<(string ParentId, string FolderName), string>(EqualityComparer<(string ParentId, string FolderName)>.Default);

            // Determine files to upload.
            var filesToUpload = GetFilesToUpload(files, allFiles, driveService, folderIdCache, cancellationToken).Result;

            Log.Information($"Total files to upload: {filesToUpload.Count}");

            // Counters.
            int filesUploaded = 0;
            var failedFiles = new ConcurrentBag<string>();

            // Get retry settings.
            int maxRetries = _appSettings.MaxRetries;
            int baseRetryDelayMs = _appSettings.RetryDelayMs;

            int maxParallelism = Math.Max(2, Environment.ProcessorCount); // Or make configurable

            // Process files concurrently.
            await Parallel.ForEachAsync(filesToUpload, new ParallelOptions { MaxDegreeOfParallelism = maxParallelism, CancellationToken = cancellationToken },
                async (file, token) =>
                {
                    var uploaded = await ProcessFileAsync(
                        file, device, driveService, allFiles, folderIdCache, driveRootDisplayName,
                        maxRetries, baseRetryDelayMs, filesToUpload.Count, failedFiles, token
                    ).ConfigureAwait(false);
                    if (uploaded)
                        Interlocked.Increment(ref filesUploaded);
                }).ConfigureAwait(false);

            Log.Information($"Processed {files.Count} files.");
            Log.Information($"Total files successfully uploaded: {filesUploaded}");
            if (filesToUpload.Count != filesUploaded)
            {
                Log.Error("Tally mismatch: Some files were not uploaded.");
            }
            if (failedFiles.Any())
            {
                Log.Warning("The following files failed to process:");
                foreach (var f in failedFiles)
                {
                    Log.Warning(f);
                }
            }
        }

        private static async Task<string> GetDriveRootDisplayNameAsync(DriveService driveService, CancellationToken cancellationToken)
        {
            var rootFolder = await driveService.Files.Get(_appSettings.DriveFolderId).ExecuteAsync(cancellationToken).ConfigureAwait(false);
            return rootFolder.Name;
        }

        private static async Task<List<MediaFileInfo>> GetFilesToUpload(
            List<MediaFileInfo> files,
            ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>> allFiles,
            DriveService driveService,
            ConcurrentDictionary<(string ParentId, string FolderName), string> folderIdCache,
            CancellationToken cancellationToken)
        {
            var filesToUpload = new List<MediaFileInfo>();
            foreach (var file in files)
            {
                if (!file.LastWriteTime.HasValue)
                {
                    Log.Warning($"File {file.Name} does not have a valid LastWriteTime. Skipping.");
                    continue;
                }
                var dateTaken = file.LastWriteTime.Value;
                var yearFolderName = dateTaken.Year.ToString();
                var monthFolderName = dateTaken.Month.ToString("00");

                var targetFolderId = _appSettings.DriveFolderId;
                targetFolderId = await GetOrCreateFolderCachedAsync(driveService, allFiles, folderIdCache, yearFolderName, targetFolderId, cancellationToken).ConfigureAwait(false);
                targetFolderId = await GetOrCreateFolderCachedAsync(driveService, allFiles, folderIdCache, monthFolderName, targetFolderId, cancellationToken).ConfigureAwait(false);

                if (!allFiles.ContainsKey(targetFolderId) || !allFiles[targetFolderId].ContainsKey(file.Name))
                {
                    filesToUpload.Add(file);
                }
            }
            return filesToUpload;
        }

        private static async Task<bool> ProcessFileAsync(
            MediaFileInfo file,
            MediaDevice device,
            DriveService driveService,
            ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>> allFiles,
            ConcurrentDictionary<(string ParentId, string FolderName), string> folderIdCache,
            string driveRootDisplayName,
            int maxRetries,
            int baseRetryDelayMs,
            int totalFiles,
            ConcurrentBag<string> failedFiles,
            CancellationToken token)
        {
            try
            {
                if (!file.LastWriteTime.HasValue)
                {
                    Log.Warning($"File {file.Name} does not have a valid LastWriteTime. Skipping.");
                    failedFiles.Add(file.Name);
                    return false;
                }
                var dateTaken = file.LastWriteTime.Value;
                Log.Information($"Processing file {file.Name} - date taken: {dateTaken}");

                var yearFolderName = dateTaken.Year.ToString();
                var monthFolderName = dateTaken.Month.ToString("00");

                var targetFolderId = _appSettings.DriveFolderId;
                targetFolderId = await GetOrCreateFolderCachedAsync(driveService, allFiles, folderIdCache, yearFolderName, targetFolderId, token).ConfigureAwait(false);
                targetFolderId = await GetOrCreateFolderCachedAsync(driveService, allFiles, folderIdCache, monthFolderName, targetFolderId, token).ConfigureAwait(false);

                var tempFilePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}_{file.Name}");

                // Pass device to RetryDownloadAsync
                bool downloadSuccess = await RetryDownloadAsync(device, file, tempFilePath, maxRetries, baseRetryDelayMs, token).ConfigureAwait(false);
                if (!downloadSuccess)
                {
                    Log.Error($"Skipping file {file.Name} after {maxRetries} attempts due to resource lock.");
                    failedFiles.Add(file.Name);
                    return false;
                }

                // Upload file.
                await UploadFileAsync(driveService, tempFilePath, targetFolderId, token).ConfigureAwait(false);
                Log.Information($"Uploaded file {file.Name} to Drive folder: {driveRootDisplayName}/{yearFolderName}/{monthFolderName}.");

                Log.Information($"Uploading file (progress unknown in parallel loop)");

                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);

                return true;
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Error processing file {file.Name}");
                failedFiles.Add(file.Name);
                return false;
            }
        }

        static async Task<DriveService> AuthenticateDriveServiceAsync(CancellationToken cancellationToken)
        {
            if (!File.Exists(CredentialsPath))
            {
                throw new FileNotFoundException(
                    $"Missing Google API credentials file: {CredentialsPath}\n" +
                    $"Ensure you have a valid '{CredentialsPath}' in the project root.\n" +
                    $"Refer to 'credentials.json.example' for format.");
            }

            using var stream = new FileStream(CredentialsPath, FileMode.Open, FileAccess.Read);
            var credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
                GoogleClientSecrets.Load(stream).Secrets,
                Scopes,
                "user",
                cancellationToken,
                new FileDataStore(TokenPath, true)).ConfigureAwait(false);

            return new DriveService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = _appSettings.ApplicationName,
            });
        }

        static async Task UploadFileAsync(DriveService service, string filePath, string parentFolderId, CancellationToken cancellationToken)
        {
            var fileMetadata = new Google.Apis.Drive.v3.Data.File
            {
                Name = Path.GetFileName(filePath),
                Parents = new List<string> { parentFolderId }
            };

            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            var request = service.Files.Create(fileMetadata, stream, "application/octet-stream");
            request.Fields = "id,name,mimeType,parents";

            try
            {
                var uploadProgress = await request.UploadAsync(cancellationToken).ConfigureAwait(false);
                if (uploadProgress.Status != UploadStatus.Completed)
                {
                    if (uploadProgress.Exception is GoogleApiException gEx)
                    {
                        var errorJson = JsonSerializer.Serialize(gEx.Error, new JsonSerializerOptions { WriteIndented = true });
                        throw new Exception($"Upload failed with status {uploadProgress.Status}. Error: {errorJson}");
                    }
                    else
                    {
                        throw new Exception($"Upload failed with status {uploadProgress.Status}. Exception: {uploadProgress.Exception}");
                    }
                }

                var uploadedFile = request.ResponseBody;
                if (uploadedFile == null || string.IsNullOrEmpty(uploadedFile.Id))
                {
                    throw new Exception("Upload completed but response did not contain a valid file Id.");
                }
                Log.Information($"Uploaded file {Path.GetFileName(filePath)} to folder ID {parentFolderId}.");
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Upload failed for {Path.GetFileName(filePath)}");
                throw;
            }
        }

        static async Task<ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>>> GetAllFilesRecursivelyAsync(
            DriveService service, string rootFolderId, CancellationToken cancellationToken)
        {
            var allFiles = new ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>>(StringComparer.OrdinalIgnoreCase);
            int foldersProcessed = 0;

            async Task FetchFolderAsync(string folderId)
            {
                var folderDict = new ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>(StringComparer.OrdinalIgnoreCase);
                string pageToken = null;
                do
                {
                    var request = service.Files.List();
                    request.Q = $"'{folderId}' in parents and trashed=false";
                    request.Fields = "nextPageToken, files(id, name, mimeType, parents)";
                    request.PageSize = DefaultPageSize;
                    request.PageToken = pageToken;

                    var result = await request.ExecuteAsync(cancellationToken).ConfigureAwait(false);
                    foreach (var file in result.Files)
                    {
                        folderDict[file.Name] = file;
                    }
                    pageToken = result.NextPageToken;
                } while (!string.IsNullOrEmpty(pageToken));

                allFiles[folderId] = folderDict;
                Interlocked.Increment(ref foldersProcessed);
                Log.Information($"Processed folder: {folderId} (Total folders processed: {foldersProcessed})");

                var subfolderTasks = folderDict.Values
                    .Where(file => file.MimeType == FolderMimeType)
                    .Select(file => FetchFolderAsync(file.Id))
                    .ToList();

                await Task.WhenAll(subfolderTasks).ConfigureAwait(false);
            }

            await FetchFolderAsync(rootFolderId).ConfigureAwait(false);
            return allFiles;
        }

        static async Task<string> GetOrCreateFolderAsync(
            DriveService service,
            ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>> allFiles,
            string folderName, string parentFolderId, CancellationToken cancellationToken)
        {
            if (allFiles.TryGetValue(parentFolderId, out var parentDict) &&
                parentDict.TryGetValue(folderName, out var existingFolder) &&
                existingFolder.MimeType == FolderMimeType)
            {
                return existingFolder.Id;
            }

            var newFolder = new Google.Apis.Drive.v3.Data.File
            {
                Name = folderName,
                MimeType = FolderMimeType,
                Parents = new List<string> { parentFolderId }
            };

            var request = service.Files.Create(newFolder);
            request.Fields = "id, name, mimeType, parents";
            var createdFolder = await request.ExecuteAsync(cancellationToken).ConfigureAwait(false);

            if (!allFiles.ContainsKey(parentFolderId))
                allFiles[parentFolderId] = new ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>(StringComparer.OrdinalIgnoreCase);
            allFiles[parentFolderId][createdFolder.Name] = createdFolder;

            Log.Information($"Created folder '{folderName}' under parent ID {parentFolderId}.");
            return createdFolder.Id;
        }

        static MediaDevice? GetDevice()
        {
            var devices = MediaDevice.GetDevices().ToList();
            if (devices == null || devices.Count == 0)
            {
                return null;
            }
            else if (devices.Count == 1)
            {
                Log.Information($"Found device {devices.First().FriendlyName}");
                return devices.First();
            }
            else
            {
                Log.Information("Please select a device to connect to:");
                for (int i = 0; i < devices.Count; i++)
                {
                    Log.Information($"{i + 1}. {devices[i].FriendlyName}");
                }
                Log.Information("Enter device number (or press Enter to cancel):");
                var deviceNumber = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(deviceNumber))
                {
                    Log.Warning("Device selection cancelled by user.");
                    return null;
                }
                if (int.TryParse(deviceNumber, out int deviceIndex) && deviceIndex > 0 && deviceIndex <= devices.Count)
                {
                    return devices[deviceIndex - 1];
                }
                else
                {
                    Log.Error("Invalid device number");
                    return null;
                }
            }
        }

        // Parallelize file discovery for each camera path
        static List<MediaFileInfo> GetFiles(IConfigurationRoot config, MediaDevice device)
        {
            var dcimPaths = config.GetSection("DCIMPaths").Get<string[]>();
            var excludedPrefixes = config.GetSection("ExcludedPrefixes").Get<string[]>() ?? Array.Empty<string>();
            var files = new ConcurrentBag<MediaFileInfo>();
            int fileCount = 0;

            var directories = device.EnumerateDirectories("/").ToList();

            Parallel.ForEach(directories, dir =>
            {
                var cameraPath = Path.Combine(dir, "DCIM", "Camera");
                if (device.DirectoryExists(cameraPath))
                {
                    var photoDir = device.GetDirectoryInfo(cameraPath);
                    foreach (var file in photoDir.EnumerateFiles("*.*", SearchOption.AllDirectories))
                    {
                        if (excludedPrefixes.Any(prefix => file.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                            continue;
                        files.Add(file);
                        int count = Interlocked.Increment(ref fileCount);
                        if (count % 50 == 0)
                        {
                            Log.Information($"Discovered {count} files so far...");
                        }
                    }
                }
            });

            Log.Information($"Total discovered files: {fileCount}");
            return files.ToList();
        }

        // Helper: cache folder IDs locally to avoid redundant Drive API calls
        private static async Task<string> GetOrCreateFolderCachedAsync(
            DriveService service,
            ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>> allFiles,
            ConcurrentDictionary<(string ParentId, string FolderName), string> folderIdCache,
            string folderName, string parentFolderId, CancellationToken cancellationToken)
        {
            var key = (parentFolderId, folderName);
            if (folderIdCache.TryGetValue(key, out var cachedId))
                return cachedId;

            var id = await GetOrCreateFolderAsync(service, allFiles, folderName, parentFolderId, cancellationToken).ConfigureAwait(false);
            folderIdCache[key] = id;
            return id;
        }

        private static void LoadConfiguration()
        {
            _appSettings = new AppSettings();
            _config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();
            _config.Bind(_appSettings);

            //make sure every property of AppSettings is set using reflection
            foreach (var prop in typeof(AppSettings).GetProperties())
            {
                if (prop.GetValue(_appSettings) == null)
                {
                    throw new Exception($"Missing configuration value for {prop.Name}");
                }
            }
        }

        private static async Task<bool> RetryDownloadAsync(MediaDevice device, MediaFileInfo file, string tempFilePath, int maxRetries, int baseRetryDelayMs, CancellationToken token)
        {
            int retryCount = 0;
            while (retryCount < maxRetries)
            {
                try
                {
                    await Task.Run(() => device.DownloadFile(file.FullName, tempFilePath), token);
                    return true;
                }
                catch (System.Runtime.InteropServices.COMException ex) when ((uint)ex.ErrorCode == 0x800700AA)
                {
                    retryCount++;
                    int delayMs = baseRetryDelayMs * (int)Math.Pow(2, retryCount);
                    Log.Warning($"File {file.Name} is in use (attempt {retryCount}/{maxRetries}). Retrying in {delayMs}ms...");
                    await Task.Delay(delayMs, token);
                }
            }
            return false;
        }
    }
}
