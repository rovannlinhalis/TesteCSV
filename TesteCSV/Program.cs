using Npgsql;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using TesteCSV;

namespace TesteCSV
{
    internal class Program
    {
        static ConcurrentQueue<byte[]> fila = new ConcurrentQueue<byte[]>();
        static string sql = "INSERT INTO public.sales (region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit) VALUES ";
        static int loteSize = 500;
        static int recordsRead = 0;
        static int recordsWrite = 0;
        static int recordsInsert = 0;
        static bool readingFile = false;

     
        static async Task Main(string[] args)
        {
            DateTime inicio = DateTime.Now;
            string file = "D:\\CSVMillion\\50m.csv";

            List<Task> allTasks = new List<Task>();
            readingFile = true;
            Task TFileReader = Task.Factory.StartNew(() => {
                string line;
                using (TextReader reader = new StreamReader(file, Encoding.Default))
                {
                    while ((line = reader.ReadLine()) != null)
                    {
                        fila.Enqueue(Encoding.Default.GetBytes(line));
                        recordsRead++;
                    }
                    readingFile = false;
                }
            });

            int DbTasks = 6;

            for (int i = 0; i < DbTasks; i++)
            {
                allTasks.Add(Task.Factory.StartNew(() =>
                {
                    try
                    {

                        StringBuilder sb = new StringBuilder();
                        string line;
                        string insert;
                        int loteCount = 0;
                        using (NpgsqlConnection conexao = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=teste_csv;User Id=postgres;Password=Senha123;"))
                        {
                            conexao.Open();
                            {
                                while (!fila.IsEmpty || readingFile)
                                {
                                    if (fila.TryDequeue(out byte[] lineData))
                                    {
                                        line = Encoding.Default.GetString(lineData);
                                        string[] fields = line.Split(',');
                                        sb.Append("('" + fields[0].SanitizarValue() + "', '" + fields[1].SanitizarValue() + "', '" + fields[2].SanitizarValue() + "', '" + fields[3].SanitizarValue() + "', '" + fields[4].SanitizarValue() + "', '" + fields[5].SanitizarValue() + "', '" + fields[6].SanitizarValue() + "', '" + fields[7].SanitizarValue() + "', '" + fields[8].SanitizarValue() + "', '" + fields[9].SanitizarValue() + "', '" + fields[10].SanitizarValue() + "', '" + fields[11].SanitizarValue() + "', '" + fields[12].SanitizarValue() + "', '" + fields[13].SanitizarValue() + "'),");
                                        loteCount++;
                                    }


                                    if (loteCount >= loteSize)
                                    {
                                        recordsWrite += loteCount;
                                        loteCount = 0;
                                        insert = sql + sb.ToString();
                                        insert = insert.Remove(insert.Length - 1) + ";";

                                        using (NpgsqlCommand cmd = conexao.CreateCommand())
                                        {
                                            cmd.CommandText = insert;
                                            cmd.ExecuteNonQuery();
                                        }
                                        sb.Clear();
                                    }

                                }

                                Console.WriteLine("Saiu do loop " + fila.Count + " / " + readingFile);


                                if (loteCount > 0)
                                {
                                    recordsWrite += loteCount;
                                    loteCount = 0;
                                    insert = sql + sb.ToString();
                                    insert = insert.Remove(insert.Length - 1) + ";";

                                    using (NpgsqlCommand cmd = conexao.CreateCommand())
                                    {
                                        cmd.CommandText = insert;
                                        cmd.ExecuteNonQuery();
                                    }
                                    sb.Clear();
                                }
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("ERRO: "+ ex.Message);
                    }
                }));
            }

            await Task.WhenAll(allTasks);
            Console.WriteLine("FIM");
            Console.WriteLine("Registros lidos: " + recordsRead);
            Console.WriteLine("Registros escritos: " + recordsWrite);
            Console.WriteLine("Duração (segundos): " + (DateTime.Now - inicio).TotalSeconds);
            Console.ReadKey();


        }
    }
}