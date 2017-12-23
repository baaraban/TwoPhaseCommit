using Npgsql;
using System;

namespace TwoPC
{
    public class PostgresProxy
    {
        private const string connectionTemplate = "Server={0}; Port = {1}; User Id = {2}; Password = {3}; Database={4}";
        private string currentTransactionName;
        private string connectionString;
        private NpgsqlConnection connection;

        public PostgresProxy(string server, string port, string userId, string userPassword, string databaseName)
        {
            this.connectionString = String.Format(connectionTemplate, server, port, userId, userPassword, databaseName);
            this.connection = new NpgsqlConnection(this.connectionString);
        }

        public void OpenConnection()
        {
            Console.WriteLine("{0} opened", this.connectionString);
            this.connection.Open();
        }

        public bool PrepareTransaction(string query, string transactionName)
        {
            try
            {
                Console.WriteLine("Trying to prepare {0} transaction on {1}", transactionName, this.connectionString);
                var queryToPerform = String.Format("BEGIN; {0}; PREPARE TRANSACTION '{1}';", query, transactionName);
                this.currentTransactionName = transactionName;
                var command = new NpgsqlCommand(queryToPerform, this.connection);
                command.ExecuteNonQuery();
                Console.WriteLine("Transaction {0} prepared on {1}", this.currentTransactionName, this.connectionString);
                return true;
            }
            catch(Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                return false;
            }
        }

        public void RollbackCurrentTransaction()
        {
            if (String.IsNullOrEmpty(this.currentTransactionName))
            {
                Console.WriteLine("No transaction on {0}", this.connectionString);
                throw new Exception("No current transaction");
            }
            var command = new NpgsqlCommand(String.Format("ROLLBACK PREPARED '{0}'", this.currentTransactionName), this.connection);
            Console.WriteLine("Transaction {0} rollbacked on {1}", this.currentTransactionName, this.connectionString);
            this.currentTransactionName = String.Empty;
            command.ExecuteNonQuery();
        }

        public void CommitCurrentTransaction()
        {
            if (String.IsNullOrEmpty(this.currentTransactionName))
            {
                Console.WriteLine("No transaction on {0}", this.connectionString);
                throw new Exception("No current transaction");
            }
            var command = new NpgsqlCommand(String.Format("COMMIT PREPARED '{0}'", this.currentTransactionName), this.connection);
            Console.WriteLine("Transaction {0} commited on {1}", this.currentTransactionName, this.connectionString);
            this.currentTransactionName = String.Empty;
            command.ExecuteNonQuery();
        }

        public void CloseConnection()
        {
            Console.WriteLine("{0} closed", this.connectionString);
            this.connection.Close();
        }
    }
}
