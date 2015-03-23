using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Nito.KitchenSink.OptionParsing;

using PgSync.Common;

namespace PgSync.Schema
{
    public class SyncSchema
    {
        /// <summary>
        /// Application options
        /// </summary>
        private readonly AppOptions _options;

        private IDataCache _cache;

        private readonly IDatabaseInstance _srcDatabase;
        private readonly IDatabaseInstance _dstDatabase;

        private ISet<string> _srcSchemas;
        private ISet<string> _dstSchemas;

        private IDictionary<string, TableDefinition> _srcTables;
        private IDictionary<string, TableDefinition> _dstTables;

        /// <summary>
        /// Displays a verbose message if verbose messaging is enabled.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        private void Verbose(string format, params object[] args)
        {
            if (_options.Verbose)
            {
                Console.Error.WriteLine(format, args);
                Console.Error.Flush();
            }
        }

        /// <summary>
        /// Initialize the tables prior to synchronization
        /// </summary>
        private void InitTables()
        {
            var taskList = new Task[]
            {
                new Task(() =>
                {
                    _srcTables = _srcDatabase.GetTables(true)
                        .Where(
                            keyValuePair => _srcSchemas.Contains(keyValuePair.Value.Schema))
                        .ToSortedDictionary(
                            keyValuePair => keyValuePair.Key,
                            KeyValuePair => KeyValuePair.Value);
                }),
                new Task(() =>
                {
                    _dstTables = _dstDatabase.GetTables(true)
                        .Where(
                            keyValuePair => _dstSchemas.Contains(keyValuePair.Value.Schema))
                        .ToSortedDictionary(
                            keyValuePair => keyValuePair.Key,
                            KeyValuePair => KeyValuePair.Value);
                })
            };

            foreach (var task in taskList)
            {
                task.Start();
            }

            Task.WaitAll(taskList);
        }

        /// <summary>
        /// Initializes the set of schemas
        /// </summary>
        private void InitSchemas()
        {
            var taskList = new Task[]
            {
                new Task(() => { _srcSchemas = _srcDatabase.GetSchemas().Except(_options.ExcludeSchemas).ToSortedSet(); }),
                new Task(() => { _dstSchemas = _dstDatabase.GetSchemas().Except(_options.ExcludeSchemas).ToSortedSet(); })
            };

            foreach (var task in taskList)
            {
                task.Start();
            }

            Task.WaitAll(taskList);
        }

        /// <summary>
        /// Synchronizes the databases
        /// </summary>
        public void Sync()
        {
            Console.WriteLine("Initializing Schemas");
            InitSchemas();
            Console.WriteLine("Initializing Tables");
            InitTables();

            // create any schemas that exist in source but not target
            CreateSchemasInTarget();
            // create any tables that exist in the source but not target
            Console.WriteLine("Creating Tables In Target Database");
            CreateTablesInTarget();
            // update any tables that exist in the source and target but differ
            Console.WriteLine("Altering Tables In Target Database");
            AlterTablesInTarget();

            Console.WriteLine("Schema Synchronization Complete");
        }

        private void CreateSchemasInTarget()
        {
            SortedSet<string> diffGroup = new SortedSet<string>(_srcSchemas);

            diffGroup.ExceptWith(_dstSchemas);

            foreach (var schemaName in diffGroup)
            {
                Console.WriteLine("Creating schema {0}", schemaName);
                _dstDatabase.CreateSchema(schemaName);
            }
        }

        /// <summary>
        /// Creates tables that exist in the source database but not the target database.
        /// </summary>
        /// <returns></returns>
        private SortedSet<string> CreateTablesInTarget()
        {
            SortedSet<string> diffGroup = new SortedSet<string>(_srcTables.Keys);

            // missing tables
            diffGroup.ExceptWith(_dstTables.Keys);

            var selectGroup = diffGroup
                .Select(tableName => _srcTables.Find(tableName))
                .Where(tableDefinition => _dstSchemas.Contains(tableDefinition.Schema));
            foreach (var tableDefinition in selectGroup)
            {
                Console.WriteLine("Creating table {0}.{1}", tableDefinition.Schema, tableDefinition.Name);
                _dstDatabase.CreateTable(tableDefinition);
            }

            return diffGroup;
        }

        /// <summary>
        /// Alters tables that exist in the source and target database but have
        /// differing schemas.
        /// </summary>
        /// <returns></returns>
        private SortedSet<string> AlterTablesInTarget()
        {
            SortedSet<string> diffGroup = new SortedSet<string>(_srcTables.Keys);

            // look for discrepancies between table definitions
            diffGroup.IntersectWith(_dstTables.Keys);

            foreach (var tableName in diffGroup)
            {
                var srcTableDefinition = _srcTables.Find(tableName);
                var dstTableDefinition = _dstTables.Find(tableName);
                System.Diagnostics.Debug.Assert(srcTableDefinition != null);
                System.Diagnostics.Debug.Assert(dstTableDefinition != null);

                AlterTableInTarget(srcTableDefinition, dstTableDefinition);
            }

            return diffGroup;
        }

        /// <summary>
        /// Alters a single table in the destination database if it does not match
        /// the source definition of the table.
        /// </summary>
        /// <param name="srcTableDefinition"></param>
        /// <param name="dstTableDefinition"></param>
        private void AlterTableInTarget(TableDefinition srcTableDefinition, TableDefinition dstTableDefinition)
        {
            var srcColumns = srcTableDefinition.Columns;
            var dstColumns = dstTableDefinition.Columns;

            var tableHeader = string.Format(" - {0}.{1}\n", srcTableDefinition.Schema, srcTableDefinition.Name);
            for (int ii = 0; ii < srcColumns.Count; ii++)
            {
                var srcColumn = _dstDatabase.GetCommonColumnDefinition(srcColumns[ii]);
                var dstColumn = dstColumns.Where(c => c.Name == srcColumn.Name).FirstOrDefault();
                if (dstColumn == null)
                {
                    // column exists in the source database but not in the target database.
                    //   -> alter the table and add the column
                    Console.Write(tableHeader);
                    Console.WriteLine("\t + {0}", srcColumn.Name);
                    _dstDatabase.AlterTable(dstTableDefinition).AddColumn(srcColumns[ii]); // use the true source column
                    tableHeader = "";
                    continue;
                }

                if (srcColumn.DataType != dstColumn.DataType)
                {
                    Console.Write(tableHeader);
                    Console.WriteLine("\t * {0} | {1} | {2}", srcColumn.Name, srcColumn.DataType, dstColumn.DataType);
                    _dstDatabase.AlterTable(dstTableDefinition).SetColumnDefinition(srcColumns[ii]);
                    tableHeader = "";
                    continue;
                }
            }

            // any columns we need to drop?
            for (int ii = 0; ii < dstColumns.Count; ii++)
            {
                var dstColumn = dstColumns[ii];
                var srcColumn = srcColumns.FirstOrDefault(c => c.Name == dstColumn.Name);
                if (srcColumn == null)
                {
                    Console.Write(tableHeader);
                    Console.WriteLine("\t - {0}", dstColumn.Name);
                    _dstDatabase.AlterTable(dstTableDefinition).DropColumn(dstColumn.Name);
                    tableHeader = "";
                    continue;
                }
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public SyncSchema(AppOptions options)
        {
            _options = options;
            _cache = new SimpleFileCache();
            _srcDatabase = new MsDatabaseInstance(options.Source, _cache);
            _dstDatabase = new PgDatabaseInstance(options.Target, _cache);
        }

        /// <summary>
        /// Application entry point for schema synchronizations
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            try
            {
                AppOptions options = OptionParser.Parse<AppOptions>();
                // determine the number of threads to use while processing
                if (options.Threads == 0)
                    options.Threads = Environment.ProcessorCount;
                if (options.Timeout == 0)
                    options.Timeout = 300;
                if (options.UpdateInterval == 0)
                    options.UpdateInterval = 10000;

                options.Source.MaxPoolSize = options.Threads * 4;
                options.Target.MaxPoolSize = options.Threads * 4;
                options.Source.Timeout = options.Timeout;
                options.Target.Timeout = options.Timeout;

                // create the synchronizer
                var synchronizer = new SyncSchema(options);
                synchronizer.Sync();
            }
            catch (OptionParsingException e)
            {
                Console.Error.WriteLine(e.Message);
                AppOptions.Usage();
            }
        }
    }
}
