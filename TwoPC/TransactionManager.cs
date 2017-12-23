using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwoPC
{
    public class TransactionManager
    {
        public void PerformTwoPhaseCommit(List<Tuple<PostgresProxy, string>> nodesWithQueries)
        {
            Console.WriteLine("Opening connections");
            foreach(var node in nodesWithQueries)
            {
                node.Item1.OpenConnection();
            }
            Console.WriteLine("Connections opened");

            var succeded = new List<PostgresProxy>();

            int i = 0;
            Console.WriteLine("Preparing transactions");
            foreach (var node in nodesWithQueries)
            {
                var resultOfStart = node.Item1.PrepareTransaction(node.Item2, i++.ToString());

                if (resultOfStart)
                {
                    succeded.Add(node.Item1);
                }
                else
                {
                    break;
                }
            }

            if(succeded.Count == nodesWithQueries.Count)
            {
                Console.WriteLine("Commiting transactions");
                foreach(var node in succeded)
                {
                    node.CommitCurrentTransaction();
                }
            }
            else
            {
                foreach (var node in succeded)
                {
                    Console.WriteLine("Rollbacking transactions");
                    node.RollbackCurrentTransaction();
                }
            }


            Console.WriteLine("Closing connections");
            foreach(var node in nodesWithQueries)
            {
                node.Item1.CloseConnection();
            }
        }
    }
}
