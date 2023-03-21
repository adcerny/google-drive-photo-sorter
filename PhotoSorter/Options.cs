using CommandLine.Text;
using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PhotoSorter
{
    public class Options
    {
        [Option('s', Required = false, HelpText = "Source path.", Default = "\\DCIM\\Camera")]
        public string Source { get; set; }
        
        [Option('d', Required = true, HelpText = "Destination path.")]
        public string Destination { get; set; }




    }
}
