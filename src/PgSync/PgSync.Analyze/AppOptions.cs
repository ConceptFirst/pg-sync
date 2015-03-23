using System;

using PgSync.Common;

using Nito.KitchenSink.OptionParsing;

namespace PgSync.Analyze
{
	public sealed class AppOptions : OptionArgumentsBase
	{
        private DatabaseDefinition _source;

        public DatabaseDefinition Source { get { return _source; } }

        public AppOptions()
        {
            _source = new DatabaseDefinition();
        }

        [Option("timeout",  OptionArgument.Required)]
        public int Timeout { get; set; }

		[Option("verbose", 'v', OptionArgument.None)]
		public bool Verbose { get; set; }
		[Option("threads")]
		public int Threads { get; set; }

        [Option("user")]
        public string SrcDatabaseUser { set { _source.DatabaseUser = value; } }
        [Option("pass")]
        public string SrcDatabasePassword { set { _source.DatabasePassword = value; } }
        [Option("host")]
        public string SrcDatabaseHost { set { _source.DatabaseHost = value; } }
        [Option("name")]
        public string SrcDatabaseName { set { _source.DatabaseName = value; } }

	    public static int Usage()
		{
			Console.Error.WriteLine("Usage: app [OPTIONS] ...");
			return -1;		 	
		}
	}
}
