using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

using Nito.KitchenSink.OptionParsing;

using PgSync.Common;

namespace PgSync.FastLoad
{
    public class PgFastLoad : ILogger
    {
        /// <summary>
        /// Application options
        /// </summary>
        private AppOptions _options;

        private IDataCache _cache;

        private MsDatabaseInstance _srcDatabase;
        private PgDatabaseInstance _dstDatabase;

        private ISet<string> _dstSchemas;
        private IDictionary<string, TableDefinition> _dstTables;

        private IList<ForeignReference> _foreignReferences;
        private IDictionary<string, ISet<string>> _foreignReferencesTable;

        private ConcurrentQueue<string> _fileQueue =
            new ConcurrentQueue<string>();

        /// <summary>
        /// Displays a verbose message if verbose messaging is enabled.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        private void Verbose(string format, params object[] args)
        {
            if (_options.Verbose)
            {
                var message = string.Format(format, args);
                Console.Error.WriteLine("{0}: {1}", Thread.CurrentThread.ManagedThreadId, message);
                Console.Error.Flush();
            }
        }

        public void Info(string format, params object[] args)
        {
            Verbose(format, args);
        }

        public void Debug(string format, params object[] args)
        {
            Verbose(format, args);
        }

        /// <summary>
        /// Initialize the foreign references
        /// </summary>
        private void InitForeignReferences()
        {
            _foreignReferences = _dstDatabase.GetForeignReferences();
            _foreignReferencesTable = new Dictionary<string, ISet<string>>();

            foreach(var foreignReference in _foreignReferences) {
                _foreignReferencesTable
                    .GetOrAdd(foreignReference.SourceFullyQualifiedName, fqn => new HashSet<string>())
                    .Add(foreignReference.ReferenceTable);
            }
        }

        internal enum LoadStateEnum
        {
            Unloaded,
            Loading,
            Loaded
        }

        internal class LoadState
        {
            internal LoadStateEnum State { get; set; }
            internal Thread Origin { get; set; }

            internal LoadState()
            {
                State = LoadStateEnum.Unloaded;
                Origin = Thread.CurrentThread;
            }
        }

        private IDictionary<string, LoadState> _tableLoadState =
            new Dictionary<string, LoadState>();


        private IDictionary<string, string> _tableToFileMapping =
            new Dictionary<string, string>();

        private void LoadTable(string tableName)
        {
            var fileWithPath = _tableToFileMapping.Find(tableName);
            if (fileWithPath == null)
            {
                // log a warning that a data table was loaded but its dependency
                // was not included in the dataset
                Verbose("data for dependent table {0} not included in dataset", tableName);
                return;
            }

            _dstDatabase.TruncateTable(tableName);
            _dstDatabase.InsertIntoTable(tableName, fileWithPath);
        }

        private void RecurseLoadTable(string tableName)
        {
            // do not process tables that do not exist in the target schema
            if (!_dstTables.ContainsKey(tableName))
            {
                return;
            }

            bool waiting = false;
            LoadState loadState;

            lock (_tableLoadState)
            {
                if (_tableLoadState.TryGetValue(tableName, out loadState))
                {
                    while (true)
                    {
                        // if the table is loaded, then ignore the request
                        if (loadState.State == LoadStateEnum.Loaded)
                        {
                            if (waiting)
                            {
                                Verbose("Notified that {0} loaded", tableName);
                            }
                            return;
                        }
                        // if the table is unloaded, then set it to loading and just
                        // break to the next portion of code which loads the table
                        if (loadState.State == LoadStateEnum.Unloaded)
                        {
                            loadState.State = LoadStateEnum.Loading;
                            break;
                        }
                        // if the table is loading, then it may have originated on
                        // this thread or another thread.  if the origination is
                        // on this thread, then it's a table which has a circular
                        // dependency (bad) - we will just return immediately.
                        if (loadState.State == LoadStateEnum.Loading)
                        {
                            if (loadState.Origin == Thread.CurrentThread)
                            {
                                return;
                            }
                            else
                            {
                                Verbose("Waiting for {0} to complete", tableName);
                                waiting = true;
                                Monitor.Wait(_tableLoadState);
                            }
                        }
                    }
                }
                else
                {
                    _tableLoadState[tableName] = loadState = new LoadState()
                    {
                        State = LoadStateEnum.Loading
                    };
                }
            }

            // Recursively load all tables that this table depends upon
            var references = _foreignReferencesTable.Find(tableName);
            if (references != null)
            {
                foreach (var reference in references.Where(r => r != tableName))
                {
                    RecurseLoadTable(reference);
                }
            }

            // load the table
            LoadTable(tableName);

            // notify other threads
            lock (_tableLoadState)
            {
                loadState.State = LoadStateEnum.Loaded;
                Monitor.PulseAll(_tableLoadState);
            }
        }

        /// <summary>
        /// Recursively loads all tables
        /// </summary>
        private void RecurseLoadTables()
        {
            using (_dstDatabase.IsolatedConnections())
            {
                string fileWithPath;
                while(_fileQueue.TryDequeue(out fileWithPath))
                {
                    var file = Path.GetFileName(fileWithPath);
                    if (file.EndsWith(".csv.gz"))
                    {
                        var tableName = file.Substring(0, file.Length - 7).ToLower();
                        RecurseLoadTable(tableName);
                    }
                    else if (file.EndsWith(".csv"))
                    {
                        var tableName = file.Substring(0, file.Length - 4).ToLower();
                        RecurseLoadTable(tableName);
                    }
                }
            }
        }

        private void ScriptTable(string tableName)
        {
            var fileWithPath = _tableToFileMapping.Find(tableName);
            if (fileWithPath == null)
            {
                // log a warning that a data table was loaded but its dependency
                // was not included in the dataset
                Verbose("data for dependent table {0} not included in dataset", tableName);
                return;
            }

            Console.WriteLine("TRUNCATE TABLE {0};", tableName);
            Console.WriteLine("COPY {0} FROM '{1}\\{2}.csv' WITH CSV HEADER ESCAPE '\\';",
                tableName, _options.ScriptDir, tableName);
        }

        private void ScriptLoadTable(string tableName)
        {
            // do not process tables that do not exist in the target schema
            if (!_dstTables.ContainsKey(tableName))
            {
                return;
            }

            bool waiting = false;
            LoadState loadState;

            lock (_tableLoadState)
            {
                if (_tableLoadState.TryGetValue(tableName, out loadState))
                {
                    while (true)
                    {
                        // if the table is loaded, then ignore the request
                        if (loadState.State == LoadStateEnum.Loaded)
                        {
                            if (waiting)
                            {
                                Verbose("Notified that {0} loaded", tableName);
                            }
                            return;
                        }
                        // if the table is unloaded, then set it to loading and just
                        // break to the next portion of code which loads the table
                        if (loadState.State == LoadStateEnum.Unloaded)
                        {
                            loadState.State = LoadStateEnum.Loading;
                            break;
                        }
                        // if the table is loading, then it may have originated on
                        // this thread or another thread.  if the origination is
                        // on this thread, then it's a table which has a circular
                        // dependency (bad) - we will just return immediately.
                        if (loadState.State == LoadStateEnum.Loading)
                        {
                            return;
                        }
                    }
                }
                else
                {
                    _tableLoadState[tableName] = loadState = new LoadState()
                    {
                        State = LoadStateEnum.Loading
                    };
                }
            }

            // Recursively load all tables that this table depends upon
            var references = _foreignReferencesTable.Find(tableName);
            if (references != null)
            {
                foreach (var reference in references.Where(r => r != tableName))
                {
                    ScriptLoadTable(reference);
                }
            }

            // load the table
            ScriptTable(tableName);

            // notify other threads
            lock (_tableLoadState)
            {
                loadState.State = LoadStateEnum.Loaded;
            }
        }

        /// <summary>
        /// Scripts loads all tables
        /// </summary>
        private void ScriptLoadTables()
        {
            string fileWithPath;
            while (_fileQueue.TryDequeue(out fileWithPath))
            {
                var file = Path.GetFileName(fileWithPath);
                if (file.EndsWith(".csv.gz"))
                {
                    var tableName = file.Substring(0, file.Length - 7).ToLower();
                    ScriptLoadTable(tableName);
                }
                else if (file.EndsWith(".csv"))
                {
                    var tableName = file.Substring(0, file.Length - 4).ToLower();
                    ScriptLoadTable(tableName);
                }
            }
        }

        /// <summary>
        /// Initializes the map that associates tables with files.
        /// </summary>
        private void InitTableToFileMapping()
        {
            foreach (var fileWithPath in _options.Files)
            {
                var file = Path.GetFileName(fileWithPath);
                if (file.EndsWith(".csv.gz"))
                {
                    var tableName = file.Substring(0, file.Length - 7).ToLower();
                    _tableToFileMapping[tableName] = fileWithPath;
                    _fileQueue.Enqueue(fileWithPath);
                }
                else if (file.EndsWith(".csv"))
                {
                    var tableName = file.Substring(0, file.Length - 4).ToLower();
                    _tableToFileMapping[tableName] = fileWithPath;
                    _fileQueue.Enqueue(fileWithPath);
                }
            }
        }

        /// <summary>
        /// Initialize the tables prior to synchronization
        /// </summary>
        private void InitTables()
        {
            _dstSchemas = _dstDatabase.GetSchemas();
            _dstTables = _dstDatabase.GetTables(true)
                .Where(
                    keyValuePair => _dstSchemas.Contains(keyValuePair.Value.Schema))
                .ToSortedDictionary(
                    keyValuePair => keyValuePair.Key,
                    KeyValuePair => KeyValuePair.Value);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public PgFastLoad(AppOptions options)
        {
            _options = options;
            _cache = new SimpleFileCache();
            _srcDatabase = new MsDatabaseInstance(options.Source, _cache);
            _dstDatabase = new PgDatabaseInstance(options.Target, _cache);
            _dstDatabase.Logger = this;
        }

        static void Main(string[] args)
        {
            try
            {
                AppOptions options = OptionParser.Parse<AppOptions>();
                // determine the number of threads to use while processing
                if (options.Threads == 0)
                    options.Threads = Environment.ProcessorCount * 4;
                if (options.Timeout == 0)
                    options.Timeout = 300;
                if (options.UpdateInterval == 0)
                    options.UpdateInterval = 10000;

                options.Source.MaxPoolSize = options.Threads * 4;
                options.Target.MaxPoolSize = options.Threads * 4;
                options.Source.Timeout = options.Timeout;
                options.Target.Timeout = options.Timeout;

                // determine what data files to load

                // initialize the fast load
                var fastLoad = new PgFastLoad(options);
                fastLoad.InitTables();
                fastLoad.InitTableToFileMapping();
                fastLoad.InitForeignReferences();

                if (options.Script)
                {
                    fastLoad.ScriptLoadTables();
                }
                else
                {
                    var threads = new List<Thread>();
                    for (int ii = 0; ii < options.Threads; ii++)
                    {
                        var thread = new Thread(() => fastLoad.RecurseLoadTables());
                        thread.Start();
                        threads.Add(thread);
                    }

                    threads.ForEach(t => t.Join());
                }
            }
            catch (OptionParsingException e)
            {
                Console.Error.WriteLine(e.Message);
                AppOptions.Usage();
            }
        }
    }
}
