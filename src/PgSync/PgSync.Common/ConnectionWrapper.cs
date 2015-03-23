using System;
using System.Data;

namespace PgSync.Common
{
    public class ConnectionWrapper : IDbConnection
    {
        private IDbConnection _subConnection;

        /// <summary>
        /// Returns the wrapped connection
        /// </summary>
        public IDbConnection WrappedConnection
        {
            get { return _subConnection; }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="subConnection"></param>
        public ConnectionWrapper(IDbConnection subConnection)
        {
            _subConnection = subConnection;
        }

        public void Dispose() { }

        public string Database
        {
            get { return _subConnection.Database; }
        }

        public ConnectionState State
        {
            get { return _subConnection.State; }
        }

        public int ConnectionTimeout
        {
            get { return _subConnection.ConnectionTimeout; }
        }

        public string ConnectionString
        {
            get { return _subConnection.ConnectionString; }
            set { throw new NotSupportedException(); }
        }

        public IDbTransaction BeginTransaction()
        {
            return _subConnection.BeginTransaction();
        }

        public IDbTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            return _subConnection.BeginTransaction(isolationLevel);
        }

        public void ChangeDatabase(string database)
        {
            throw new NotSupportedException();
        }

        public IDbCommand CreateCommand()
        {
            return _subConnection.CreateCommand();
        }

        public void Open()
        {
        }

        public void Close()
        {
        }
    }
}
