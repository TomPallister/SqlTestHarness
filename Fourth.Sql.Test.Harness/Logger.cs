using System;
using System.Diagnostics;

namespace Fourth.Sql.Test.Harness
{
    public static class Logger
    {
        public static void WriteLine(string message, params object[] args)
        {
            message = string.Format(message, args);
            Debug.WriteLine("{0} | {1}", DateTime.Now.ToLongTimeString(), message);
        }
    }
}