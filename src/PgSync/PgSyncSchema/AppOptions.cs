using System;
using System.Collections.Generic;

using PgSync.Common;

using Nito.KitchenSink.OptionParsing;

namespace PgSync.Schema
{
	public sealed class AppOptions : OptionArgumentsBase
	{
        private DatabaseDefinition _source;
        private DatabaseDefinition _target;

        public DatabaseDefinition Source { get { return _source; } }
        public DatabaseDefinition Target { get { return _target; } }

        public ISet<string> ExcludeSchemas { get; set; } 

        public AppOptions()
        {
            _source = new DatabaseDefinition();
            _target = new DatabaseDefinition();
            ExcludeSchemas = new HashSet<string>();
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
        
        [Option("max")]
        public int MaxRecords { get; set; }
        [Option("skip")]
        public int SkipRecords { get; set; }

        [Option("dryrun", OptionArgument.None)]
        public bool DryRun { get; set; }

	    [Option("exclude", OptionArgument.Required)]
	    public string Exclude
	    {
            set { ExcludeSchemas.Add(value); }
	    }

	    public static int Usage()
		{
			Console.Error.WriteLine("Usage: app [OPTIONS] ...");
			return -1;		 	
		}
	}
}
