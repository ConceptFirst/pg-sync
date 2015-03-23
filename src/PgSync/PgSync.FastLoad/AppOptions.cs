using System;
using System.Collections.Generic;
using System.IO;

using PgSync.Common;

using Nito.KitchenSink.OptionParsing;

namespace PgSync.FastLoad
{
	public sealed class AppOptions : OptionArgumentsBase
	{
        internal ISet<string> Files { get; set; }

        private DatabaseDefinition _source;
        private DatabaseDefinition _target;

        public DatabaseDefinition Source { get { return _source; } }
        public DatabaseDefinition Target { get { return _target; } }

        public AppOptions()
        {
            _source = new DatabaseDefinition();
            _target = new DatabaseDefinition();
            Files = new HashSet<string>();
        }

        [Option("include", OptionArgument.Required)]
        public string Include
        {
            set
            {
                if (File.Exists(value))
                {
                    Files.Add(value);
                }
                else if (Directory.Exists(value))
                {
                    Directory.EnumerateFiles(value, "*.csv", SearchOption.AllDirectories)
                        .ForEach(v => Files.Add(v));
                    Directory.EnumerateFiles(value, "*.csv.gz", SearchOption.AllDirectories)
                        .ForEach(v => Files.Add(v));
                }
            }
        }

        [Option("timeout",  OptionArgument.Required)]
        public int Timeout { get; set; }
        [Option("update-interval", OptionArgument.Required)]
        public int UpdateInterval { get; set; }

		[Option("verbose", 'v', OptionArgument.None)]
		public bool Verbose { get; set; }
		[Option("threads")]
		public int Threads { get; set; }

        [Option("src-user")]
        public string SrcDatabaseUser { set { _source.DatabaseUser = value; } }
        [Option("src-pass")]
        public string SrcDatabasePassword { set { _source.DatabasePassword = value; } }
        [Option("src-host")]
        public string SrcDatabaseHost { set { _source.DatabaseHost = value; } }
        [Option("src-name")]
        public string SrcDatabaseName { set { _source.DatabaseName = value; } }

        [Option("dst-user")]
        public string DstDatabaseUser { set { _target.DatabaseUser = value; } }
        [Option("dst-pass")]
        public string DstDatabasePassword { set { _target.DatabasePassword = value; } }
        [Option("dst-host")]
        public string DstDatabaseHost { set { _target.DatabaseHost = value; } }
        [Option("dst-name")]
        public string DstDatabaseName { set { _target.DatabaseName = value; } }

        [Option("scriptdir")]
        public string ScriptDir { get; set; }

        [Option("script", OptionArgument.None)]
        public bool Script { get; set; }
        
	    public static int Usage()
		{
			Console.Error.WriteLine("Usage: app [OPTIONS] ...");
			return -1;		 	
		}
	}
}
