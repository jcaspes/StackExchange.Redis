using System;
using System.Linq;

namespace MemoryTester
{
    internal static class StringsHelpers
    {
        public static string RandomString(int length, int seed)
        {
            Random random = new Random(seed);
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
