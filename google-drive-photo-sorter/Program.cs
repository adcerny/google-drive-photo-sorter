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
            LoadConfiguration();

            // Start the global timer.
            var globalStopwatch = System.Diagnostics.Stopwatch.StartNew();

            Log.Information("Starting Google Drive Photo Sorter...");

            await SortPhotosAsync(CancellationToken.None).ConfigureAwait(false);

            globalStopwatch.Stop();


            //display the time taken to process all files formatted in a readable way
            Log.Information($"Total time taken: {globalStopwatch.Elapsed.Hours} hours, {globalStopwatch.Elapsed.Minutes} minutes, {globalStopwatch.Elapsed.Seconds} seconds");
            Log.Information("Press any key to exit");
            Console.ReadKey();
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
            var driveService = await AuthenticateDriveServiceAsync(cancellationToken).ConfigureAwait(false);

            // Get destination folder details.
            var rootFolder = await driveService.Files.Get(_appSettings.DriveFolderId).ExecuteAsync(cancellationToken).ConfigureAwait(false);
            string driveRootDisplayName = rootFolder.Name;

            // Build a lookup dictionary for files already in Drive.
            var allFiles = await GetAllFilesRecursivelyAsync(driveService, _appSettings.DriveFolderId, cancellationToken).ConfigureAwait(false);

            // Retrieve list of photo files from the device.
            List<MediaFileInfo> files = GetFiles(_config, device);
            Log.Information($"Total phone files to process: {files.Count}");

            // Determine files to upload.
            var filesToUpload = new List<MediaFileInfo>();
            foreach (var file in files)
            {
                var dateTaken = file.LastWriteTime.Value;
                string yearFolderName = dateTaken.Year.ToString();
                string monthFolderName = dateTaken.Month.ToString("00");

                string targetFolderId = _appSettings.DriveFolderId;
                targetFolderId = await GetOrCreateFolderAsync(driveService, allFiles, yearFolderName, targetFolderId, cancellationToken).ConfigureAwait(false);
                targetFolderId = await GetOrCreateFolderAsync(driveService, allFiles, monthFolderName, targetFolderId, cancellationToken).ConfigureAwait(false);

                if (!allFiles.ContainsKey(targetFolderId) || !allFiles[targetFolderId].ContainsKey(file.Name))
                {
                    filesToUpload.Add(file);
                }
            }

            Log.Information($"Total files to upload: {filesToUpload.Count}");

            // Counters.
            int filesUploaded = 0;
            ConcurrentBag<string> failedFiles = new ConcurrentBag<string>();

            // Get retry settings.
            int maxRetries = _appSettings.MaxRetries;
            int baseRetryDelayMs = _appSettings.RetryDelayMs;

            // Process files concurrently.
            await Parallel.ForEachAsync(filesToUpload, new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = cancellationToken },
                async (file, token) =>
                {
                    try
                    {
                        var dateTaken = file.LastWriteTime.Value;
                        Log.Information($"Processing file {file.Name} - date taken: {dateTaken}");

                        // Folder structure: Year/Month.
                        string yearFolderName = dateTaken.Year.ToString();
                        string monthFolderName = dateTaken.Month.ToString("00");

                        string targetFolderId = _appSettings.DriveFolderId;
                        targetFolderId = await GetOrCreateFolderAsync(driveService, allFiles, yearFolderName, targetFolderId, token).ConfigureAwait(false);
                        targetFolderId = await GetOrCreateFolderAsync(driveService, allFiles, monthFolderName, targetFolderId, token).ConfigureAwait(false);

                        string tempFilePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}_{file.Name}");

                        // Exponential backoff for downloading.
                        bool downloadSuccess = await RetryDownloadAsync(device, file, tempFilePath, maxRetries, baseRetryDelayMs, token).ConfigureAwait(false);
                        if (!downloadSuccess)
                        {
                            Log.Error($"Skipping file {file.Name} after {maxRetries} attempts due to resource lock.");
                            failedFiles.Add(file.Name);
                            return;
                        }

                        // Upload file.
                        await UploadFileAsync(driveService, tempFilePath, targetFolderId, token).ConfigureAwait(false);
                        Log.Information($"Uploaded file {file.Name} to Drive folder: {driveRootDisplayName}/{yearFolderName}/{monthFolderName}.");

                        Interlocked.Increment(ref filesUploaded);
                        Log.Information($"Uploading file {filesUploaded} of {filesToUpload.Count}");

                        if (File.Exists(tempFilePath))
                            File.Delete(tempFilePath);
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex, $"Error processing file {file.Name}");
                        failedFiles.Add(file.Name);
                    }
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
                    new FileDataStore(credPath, true)).ConfigureAwait(false);
            }

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
                    request.PageSize = 1000;
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

                var subfolderTasks = new List<Task>();
                foreach (var file in folderDict.Values)
                {
                    if (file.MimeType == "application/vnd.google-apps.folder")
                    {
                        subfolderTasks.Add(FetchFolderAsync(file.Id));
                    }
                }
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

        private static void LoadConfiguration()
        {
            _appSettings = new AppSettings();
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
