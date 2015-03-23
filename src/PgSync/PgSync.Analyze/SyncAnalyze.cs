using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Newtonsoft.Json;

using Nito.KitchenSink.OptionParsing;

using PgSync.Common;
using PgSync.Tasks;

namespace PgSync.Analyze
{
    public class SyncAnalyze
    {
        /// <summary>
        /// Application options
        /// </summary>
        private AppOptions _options;

        /// <summary>
        /// Dictionary provides binding between a qualified table and its dependencies
        /// </summary>
        private Dictionary<string, ISet<string>> _foreignReferencesTable;

        private IDataCache _cache;

        private MsDatabaseInstance _srcDatabase;

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

        /// <summary>
        /// Initialize the foreign references
        /// </summary>
        private void InitForeignReferences()
        {
            var referencesTable = new Dictionary<string, ISet<string>>();

            foreach (var foreignReference in _srcDatabase.GetForeignReferences(true))
            {
                ISet<string> referenceTable;

                if (!referencesTable.TryGetValue(foreignReference.SourceFullyQualifiedName, out referenceTable))
                    referenceTable = referencesTable[foreignReference.SourceFullyQualifiedName] = new HashSet<string>();

                referenceTable.Add(foreignReference.ReferenceTable);
            }

            _foreignReferencesTable = referencesTable;
        }

        /// <summary>
        /// Gets the dependencies for the given schema and table
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private ISet<string> GetDependenciesFor(string schema, string tableName)
        {
            var fullyQualifiedName = string.Format("{0}.{1}", schema, tableName).ToLowerInvariant();

            ISet<string> referenceTable;

            if (!_foreignReferencesTable.TryGetValue(fullyQualifiedName, out referenceTable))
                return referenceTable;

            return null;
        }

        /// <summary>
        /// Creates a configuration based on the details in the database
        /// </summary>
        private SyncConfiguration CreateConfiguration()
        {
            var srcSchemas = _srcDatabase.GetSchemas(true);
            var srcIndexes = _srcDatabase.GetIndexes(true)
                .Where(index => srcSchemas.Contains(index.Schema))
                .ToList();

            var srcTables = _srcDatabase.GetTables(true)
                .Select(entry => entry.Value)
                .Where(table => srcSchemas.Contains(table.Schema))
                .ToList();

            var tablePlan = new List<SyncTable>();

            foreach (var table in srcTables.OrderBy(table => table.Schema + '.' + table.Name))
            {
                var tableName = table.Schema + '.' + table.Name;
                var tableSyncTasks = new List<SyncTask>();

                // if the table has a singular primary key that is of a sequential
                // nature, then we can add a simple sequential range check to the
                // configuration for the table
                if (table.HasPrimaryKey)
                {
                    if (table.PrimaryKey.Count == 1)
                    {
                        var primaryKeyColumn = table.GetColumn(table.PrimaryKey.First());
                        if (primaryKeyColumn.IsIdentity)
                        {
                            tableSyncTasks.Add(new SyncTaskSequencer() { Column = primaryKeyColumn.Name });
                        }
                        else
                        {
                            // is there anything we can do with a primary key value that is
                            // not a sequential value?  it's a needle in a haystack since its
                            // unbound.  if this is a table of a few million rows, there does
                            // not appear to be an easy check for the value without bounding.
                        }
                    }
                }

                // created items can often be found in a common column that is a
                // timestamp; if such a column exists, then use it to identify
                // new rows.
                var createdDateTimeColumn = table.GetColumn("CreatedDateTime");
                if (createdDateTimeColumn != null)
                {
                    tableSyncTasks.Add(new SyncTaskTimestamp() { Column = "CreatedDateTime" });
                }

                // update timestamps can help identify modifications to the table
                // structure; if such a colum exists, then use it to identify
                // modified rows.
                var updatedDateTimeColumn = table.GetColumn("UpdatedDateTime");
                if (updatedDateTimeColumn != null)
                {
                    tableSyncTasks.Add(new SyncTaskTimestamp() { Column = "UpdatedDateTime" });
                }

                // hopefully we found one or two approaches to getting updates in the
                // table; if not, we need to notify the requestor that we didnt find
                // a good way to synchronize this table.
                if (tableSyncTasks.Count == 0)
                {
                    // how unfortunate, no simple techniques were found that could use range
                    // bound analysis; lets see if we can do a "primary" key only check.
                    if (table.HasPrimaryKey)
                    {
                        tableSyncTasks.Add(new SyncTaskColumnScan(table.PrimaryKey));
                    }
                    else
                    {
                        // see if there is a unique index defined for this table
                        var uniqueIndex = srcIndexes
                            .Where(index => index.Table == table.Name)
                            .Where(index => index.IsUnique)
                            .FirstOrDefault();
                        if (uniqueIndex != null)
                        {
                            tableSyncTasks.Add(new SyncTaskColumnScan(uniqueIndex.Columns));
                        }
                    }
                }

                // 
                if (tableSyncTasks.Count == 0)
                {
                    tableSyncTasks.Add(new SyncTaskColumnScan
                    {
                        IsRainbowScan = true,
                        Columns = new SortedSet<string>(table.Columns.Select(column => column.Name))
                    });
                }

                tablePlan.Add(new SyncTable()
                {
                    Schema = table.Schema,
                    Table = table.Name,
                    Tasks = tableSyncTasks,
                    Dependencies = GetDependenciesFor(table.Schema, table.Name)
                });
            }

            var syncConfig = new SyncConfiguration();
            syncConfig.Tables = tablePlan;

            return syncConfig;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public SyncAnalyze(AppOptions options)
        {
            _options = options;
            _cache = new SimpleFileCache();
            _srcDatabase = new MsDatabaseInstance(options.Source, _cache);
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

                options.Source.MaxPoolSize = options.Threads * 4;
                options.Source.Timeout = options.Timeout;

                var syncAnalyze = new SyncAnalyze(options);
                syncAnalyze.InitForeignReferences();

                var syncConfig = syncAnalyze.CreateConfiguration();

                // create the json version of the configuration
                var jsonSettings = new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.Objects,
                    Formatting = Formatting.Indented
                };

                var jsonConfig = JsonConvert.SerializeObject(syncConfig, jsonSettings);

                Console.WriteLine(jsonConfig);
            }
            catch (OptionParsingException e)
            {
                Console.Error.WriteLine(e.Message);
                AppOptions.Usage();
            }
        }
    }
}
