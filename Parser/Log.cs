using System;
using System.IO;

namespace Parser
{
    public static class Log
    {
        private static StreamWriter _sw;

        public static void Add(string message)
        {
            using (_sw = new StreamWriter("logs.txt", true))
            {
                _sw.WriteLine($"{DateTime.Now} | {message}");
                Console.WriteLine(message);
            }
        }
    }
}
