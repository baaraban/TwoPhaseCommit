using System;
using System.Collections.Generic;

namespace TwoPC
{
    class Program
    {
        private static QueryBuilder queryBuilder = new QueryBuilder();
        public static KeyValuePair<PostgresProxy, string> getFirstAction()
        {
            var firstPostgresProxy = new PostgresProxy("localhost", "5432", "postgres", "69916010", "FlyBooking");
            var insertForFirst = queryBuilder.GetInsertScript("flybooking", new List<string>()
            {
                "clientname",
                "flynumber",
                @"""From""",
                @"""To""",
                @"""Date"""
            },
           new List<string>()
           {
                "Jambo",
                "Tup 12",
                "Madrid",
                "Barcelona",
                "2017-07-01"
           });
            return new KeyValuePair<PostgresProxy, string>(firstPostgresProxy, insertForFirst);
        }

        public static KeyValuePair<PostgresProxy, string> getSecondAction()
        {
            var secondPostgresConnection = new PostgresProxy("localhost", "5432", "postgres", "69916010", "HotelBooking");
            var insertForSecond = queryBuilder.GetInsertScript("HotelBooking", new List<string>()
            {
                "clientname",
                "hotelname",
                "arrival",
                "departure"
            },
            new List<string>()
            {
                "Jambo",
                "Nadia",
                "2017-08-04",
                "2017-10-01"
            });

            return new KeyValuePair<PostgresProxy, string>(secondPostgresConnection, insertForSecond);
        }

        public static KeyValuePair<PostgresProxy, string> getThirdAction()
        {
            var thirdPostgresConnection = new PostgresProxy("localhost", "5432", "postgres", "69916010", "Account");
            var insertForThird = "UPDATE accounts SET amount = amount - 6";
            return new KeyValuePair<PostgresProxy, string>(thirdPostgresConnection, insertForThird);
        }

        static void Main(string[] args)
        {
            var manager = new TransactionManager();
            var param = new List<KeyValuePair<PostgresProxy, string>>();
            param.Add(getFirstAction());
            param.Add(getSecondAction());
            param.Add(getThirdAction());
            manager.PerformTwoPhaseCommit(param);
            Console.ReadLine();
        }
    }
}
