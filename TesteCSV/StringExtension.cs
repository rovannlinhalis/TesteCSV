using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MillionInsertCSV
{
    public static class StringExtension
    {
        public static string Sanitize(this string value)
        {
            return value.Replace("'", "");
        }
    }
}
