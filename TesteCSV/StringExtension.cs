using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TesteCSV
{
    public static class StringExtension
    {
        public static string SanitizarValue(this string value)
        {
            return value.Replace("'", "");
        }
    }
}
