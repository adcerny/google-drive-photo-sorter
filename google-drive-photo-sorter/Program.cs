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
        // Values read from configuration.
        static string ApplicationName;
        static string DriveFolderId;

        // Use full Drive scope so you can create folders and upload files.
        static readonly string[] Scopes = { DriveService.Scope.Drive };

        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(theme: SystemConsoleTheme.Colored)
                .CreateLogger();

            // Load configuration from appsettings.json.
            _config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            // Validate and load required config values.
            ValidateAndLoadConfig(_config);

            // Start the global timer.
            var globalStopwatch = System.Diagnostics.Stopwatch.StartNew();

            Log.Information("Starting Google Drive Photo Sorter...");

            await SortPhotosAsync(CancellationToken.None);

            globalStopwatch.Stop();
            Log.Information($"Total process time: {globalStopwatch.Elapsed}");
            Log.Information("Press any key to exit");
            Console.ReadKey();
        }

        /// <summary>
        /// Validates that required configuration values are present.
        /// Throws an exception with a clear error message if any are missing.
        /// Also loads global variables from config.
        /// </summary>
        private static void ValidateAndLoadConfig(IConfiguration config)
        {
            // Validate required string values.
            foreach (var key in new[] { "ApplicationName", "DriveFolderId", "MaxRetries", "RetryDelayMs" })
            {
                if (string.IsNullOrWhiteSpace(config[key]))
                {
                    throw new Exception($"Missing required configuration value: {key}");
                }
            }
            // Validate required array.
            if (config.GetSection("DCIMPaths").Get<string[]>() == null)
            {
                throw new Exception("Missing required configuration value: DCIMPaths");
            }

            // Load global config values.
            ApplicationName = config["ApplicationName"];
            DriveFolderId = config["DriveFolderId"];
        }

        static async Task SortPhotosAsync(CancellationToken cancellationToken)
        {

            // Locate the device.
            var device = GetDevice();
            if (device == null)
            {
                Log.Error("No device found.");
                return;
            }
            device.Connect();

            // Authenticate with Google Drive.
            var driveService = await AuthenticateDriveServiceAsync(cancellationToken);

            // Get destination folder details.
            var rootFolder = await driveService.Files.Get(DriveFolderId).ExecuteAsync(cancellationToken);
            string driveRootDisplayName = rootFolder.Name;

            // Build a lookup dictionary for files already in Drive.
            var allFiles = await GetAllFilesRecursivelyAsync(driveService, DriveFolderId, cancellationToken);

            // Retrieve list of photo files from the device.
            List<MediaFileInfo> files = GetFiles(_config, device);
            Log.Information($"Total phone files to process: {files.Count}");

            // Counters.
            int filesToUpload = 0;
            int filesUploaded = 0;
            ConcurrentBag<string> failedFiles = new ConcurrentBag<string>();

            // Get retry settings.
            int maxRetries = int.TryParse(_config["MaxRetries"], out int retries) ? retries : 3;
            int baseRetryDelayMs = int.TryParse(_config["RetryDelayMs"], out int delay) ? delay : 500;

            // Process files concurrently.
            await Parallel.ForEachAsync(files, new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = cancellationToken },
                async (file, token) =>
                {
                    try
                    {
                        var dateTaken = file.LastWriteTime.Value;
                        Log.Information($"Processing file {file.Name} - date taken: {dateTaken}");

                        // Folder structure: Year/Month.
                        string yearFolderName = dateTaken.Year.ToString();
                        string monthFolderName = dateTaken.Month.ToString("00");

                        string targetFolderId = DriveFolderId;
                        targetFolderId = await GetOrCreateFolderAsync(driveService, allFiles, yearFolderName, targetFolderId, token);
                        targetFolderId = await GetOrCreateFolderAsync(driveService, allFiles, monthFolderName, targetFolderId, token);

                        // Skip if file exists.
                        if (allFiles.ContainsKey(targetFolderId) &&
                            allFiles[targetFolderId].ContainsKey(file.Name))
                        {
                            Log.Information($"File {file.Name} already exists at {driveRootDisplayName}/{yearFolderName}/{monthFolderName}. Skipping.");
                            return;
                        }

                        Interlocked.Increment(ref filesToUpload);
                        string tempFilePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}_{file.Name}");

                        // Exponential backoff for downloading.
                        int retryCount = 0;
                        bool downloadSuccess = false;
                        while (!downloadSuccess && retryCount < maxRetries)
                        {
                            try
                            {
                                await Task.Run(() => device.DownloadFile(file.FullName, tempFilePath), token);
                                downloadSuccess = true;
                            }
                            catch (System.Runtime.InteropServices.COMException ex) when ((uint)ex.ErrorCode == 0x800700AA)
                            {
                                retryCount++;
                                int delayMs = baseRetryDelayMs * (int)Math.Pow(2, retryCount);
                                Log.Warning($"File {file.Name} is in use (attempt {retryCount}/{maxRetries}). Retrying in {delayMs}ms...");
                                await Task.Delay(delayMs, token);
                            }
                        }
                        if (!downloadSuccess)
                        {
                            Log.Error($"Skipping file {file.Name} after {maxRetries} attempts due to resource lock.");
                            failedFiles.Add(file.Name);
                            return;
                        }

                        // Upload file.
                        await UploadFileAsync(driveService, tempFilePath, targetFolderId, token);
                        Log.Information($"Uploaded file {file.Name} to Drive folder: {driveRootDisplayName}/{yearFolderName}/{monthFolderName}.");

                        Interlocked.Increment(ref filesUploaded);

                        if (File.Exists(tempFilePath))
                            File.Delete(tempFilePath);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, $"Error processing file {file.Name}");
                        failedFiles.Add(file.Name);
                    }
                });

            Log.Information($"Processed {files.Count} files.");
            Log.Information($"Total files marked for upload: {filesToUpload}");
            Log.Information($"Total files successfully uploaded: {filesUploaded}");
            if (filesToUpload != filesUploaded)
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

        static async Task<DriveService> AuthenticateDriveServiceAsync(CancellationToken cancellationToken)
        {
            UserCredential credential;

            string credentialsPath = "credentials.json";

            if (!File.Exists(credentialsPath))
            {
                throw new FileNotFoundException(
                    $"Missing Google API credentials file: {credentialsPath}\n" +
                    $"Ensure you have a valid 'credentials.json' in the project root.\n" +
                    $"Refer to 'credentials.json.example' for format.");
            }

            using (var stream = new FileStream(credentialsPath, FileMode.Open, FileAccess.Read))
            {
                string credPath = "token.json";
                credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    Scopes,
                    "user",
                    cancellationToken,
                    new FileDataStore(credPath, true));
            }

            return new DriveService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = ApplicationName,
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
                var uploadProgress = await request.UploadAsync(cancellationToken);
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
                Log.Information($"Uploaded file {Path.GetFileName(filePath)} successfully.");
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
                    request.PageSize = 1000;
                    request.PageToken = pageToken;

                    var result = await request.ExecuteAsync(cancellationToken);
                    foreach (var file in result.Files)
                    {
                        folderDict[file.Name] = file;
                    }
                    pageToken = result.NextPageToken;
                } while (!string.IsNullOrEmpty(pageToken));

                allFiles[folderId] = folderDict;
                Interlocked.Increment(ref foldersProcessed);
                Log.Information($"Processed folder: {folderId} (Total folders processed: {foldersProcessed})");

                var subfolderTasks = new List<Task>();
                foreach (var file in folderDict.Values)
                {
                    if (file.MimeType == "application/vnd.google-apps.folder")
                    {
                        subfolderTasks.Add(FetchFolderAsync(file.Id));
                    }
                }
                await Task.WhenAll(subfolderTasks);
            }

            await FetchFolderAsync(rootFolderId);
            return allFiles;
        }

        static async Task<string> GetOrCreateFolderAsync(
            DriveService service,
            ConcurrentDictionary<string, ConcurrentDictionary<string, Google.Apis.Drive.v3.Data.File>> allFiles,
            string folderName, string parentFolderId, CancellationToken cancellationToken)
        {
            if (allFiles.TryGetValue(parentFolderId, out var parentDict) &&
                parentDict.TryGetValue(folderName, out var existingFolder) &&
                existingFolder.MimeType == "application/vnd.google-apps.folder")
            {
                return existingFolder.Id;
            }

            var newFolder = new Google.Apis.Drive.v3.Data.File
            {
                Name = folderName,
                MimeType = "application/vnd.google-apps.folder",
                Parents = new List<string> { parentFolderId }
            };

            var request = service.Files.Create(newFolder);
            request.Fields = "id, name, mimeType, parents";
            var createdFolder = await request.ExecuteAsync(cancellationToken);

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
                var deviceNumber = Console.ReadLine();
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

        static List<MediaFileInfo> GetFiles(IConfigurationRoot config, MediaDevice device)
        {
            var dcimPaths = config.GetSection("DCIMPaths").Get<string[]>();
            var excludedPrefixes = config.GetSection("ExcludedPrefixes").Get<string[]>() ?? Array.Empty<string>();
            List<MediaFileInfo> files = new();
            int fileCount = 0;

            foreach (var path in device.EnumerateDirectories("/"))
            {
                var cameraPath = Path.Combine(path, "DCIM", "Camera");
                if (device.DirectoryExists(cameraPath))
                {
                    var photoDir = device.GetDirectoryInfo(cameraPath);
                    foreach (var file in photoDir.EnumerateFiles("*.*", SearchOption.AllDirectories))
                    {
                        if (excludedPrefixes.Any(prefix => file.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                            continue;
                        files.Add(file);
                        fileCount++;
                        if (fileCount % 50 == 0)
                        {
                            Log.Information($"Discovered {fileCount} files so far...");
                        }
                    }
                }
            }
            Log.Information($"Total discovered files: {fileCount}");
            return files;
        }
    }
}
