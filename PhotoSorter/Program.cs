// See https://aka.ms/new-console-template for more information
using CommandLine;
using PhotoSorter;
using System.Text;
using System.Media;
using System.Drawing;
using System.Text.RegularExpressions;
using System.Drawing.Imaging;
using MediaDevices;
using Microsoft.Extensions.Configuration;

Regex r = new Regex(":");
IConfigurationRoot _config;


Parser.Default.ParseArguments<Options>(args)
    .WithNotParsed(HandleParseError)
    .WithParsed(SortPhotos);

void SortPhotos(Options options)
{

    _config = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional: false)
        .Build();

    //find phone
    var device = GetDevice();
    if (device == null)
    {
        Console.WriteLine("No device found");
        return;
    }

    device.Connect();

    List<MediaFileInfo> files = GetFiles(_config, device);

    //for each file, get the month and year of the date taken
    foreach (var file in files)
    {
        //get the date taken from the file
        var dateTaken = file.LastWriteTime.Value;
        //output "found file {file} - date taken: {dateTaken}"
        Console.WriteLine($"found file {file.Name} - date taken: {dateTaken}");

        //set the new file path in the destination folder using YYYY/MM/file format
        var newFileName = Path.Combine(options.Destination,
                                       dateTaken.Year.ToString(),
                                       dateTaken.Month.ToString("00"),
                                       file.Name);

        //check if file exists
        if (File.Exists(newFileName))
        {
            //output "file {newFileName} already exists"
            Console.WriteLine($"file {newFileName} already exists");
            continue;
        }

        //if the file path doesn't exist, create it
        if (!Directory.Exists(Path.GetDirectoryName(newFileName)))
        {
            //output "creating directory {Path.GetDirectoryName(newFileName)}"
            Console.WriteLine($"creating directory {Path.GetDirectoryName(newFileName)}");
            Directory.CreateDirectory(Path.GetDirectoryName(newFileName));
        }
        device.DownloadFile(file.FullName, newFileName);

        //output "copied file {file} to {newFileName}"
        Console.WriteLine($"copied file {file} to {newFileName}");
    }

    //write summary of number of files copied to output and prompt for key press to close console
    Console.WriteLine($"copied {files.Count()} files");
    Console.WriteLine("Press any key to exit");
    Console.ReadKey();
}

DateTime GetDateTakenFromFile(string path)
{
    //get the extension of the file
    var extension = Path.GetExtension(path);

    //check if the extension is jpg
    if (extension == ".jpg")
    {
        //get the date taken from the image
        return GetDateTakenFromImage(path);
    }
    else
    {
        //return date file was written to
        FileInfo fileInfo = new FileInfo(path);
        return fileInfo.LastWriteTime;

    }
}

DateTime GetDateTakenFromImage(string path)
{
    using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read))
    using (Image photo = Image.FromStream(fs, false, false))
    {
        PropertyItem propItem = photo.GetPropertyItem(36867);
        string dateTaken = r.Replace(Encoding.UTF8.GetString(propItem.Value), "-", 2);
        return DateTime.Parse(dateTaken);
    }
}

static void HandleParseError(IEnumerable<Error> errs)
{
    //handle errors
}

MediaDevice? GetDevice()
{
    var devices = MediaDevice.GetDevices().ToList();
    //if no devices found, log message and exit program
    if (devices is null || devices.Count == 0)
    {
        return null;
    }

    else if (devices.Count == 1)
    {
        //log the name of the device found
        Console.WriteLine($"Found device {devices.First().FriendlyName}");
        return devices.First();
    }
    else
    {
        Console.WriteLine("Please select a device to connect to:");
        for (int i = 0; i < devices.Count; i++)
        {
            Console.WriteLine($"{i + 1}. {devices[i].FriendlyName}");
        }
        var deviceNumber = Console.ReadLine();
        int.TryParse(deviceNumber, out int deviceIndex);
        if (deviceIndex > 0 && deviceIndex <= devices.Count)
        {
            return devices[deviceIndex - 1];
        }
        else
        {
            Console.WriteLine("Invalid device number");
            return null;
        }
    }
}

static List<MediaFileInfo> GetFiles(IConfigurationRoot _config, MediaDevice? device)
{
    //load an array of strings from the DCIMPaths setting in config
    var dcimPaths = _config.GetSection("DCIMPaths").Get<string[]>();
    List<MediaFileInfo> files = new();

    foreach(var path in device.EnumerateDirectories("/"))
    {
        var cameraPath = Path.Combine(path, "DCIM", "Camera");

        //add the path to the array if it exists
        if (device.DirectoryExists(cameraPath))
        {
            //get all files in the source folder
            var photoDir = device.GetDirectoryInfo(cameraPath);
            var filesInPath = photoDir.EnumerateFiles("*.*", SearchOption.AllDirectories);

            //log how many files were found
            Console.WriteLine($"found {filesInPath.Count()} files in {path}");

            files.AddRange(filesInPath);
        }
    }
    return files;
}