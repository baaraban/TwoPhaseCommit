using System;
using System.Collections.Generic;
using System.Linq;

namespace TwoPC
{
    public class QueryBuilder
    {
        public string GetInsertScript(string tableName, List<string> columns, List<string> values)
        {
            var columnsUnited = columns.Aggregate((a, b) => string.Format("{0}, {1}", a, b));
            string valuesUnited = "";
            foreach(var value in values)
            {
                valuesUnited += String.Format("'{0}', ", value);
            }

            valuesUnited = valuesUnited.Remove(valuesUnited.Length-2);

            return String.Format("INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnsUnited, valuesUnited);
        }
    }
}
