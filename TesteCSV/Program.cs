using Npgsql;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text;
using MillionInsertCSV;

namespace MillionInsertCSV
{
    internal class Program
    {
        static ConcurrentQueue<byte[]> rowsQueue = new ConcurrentQueue<byte[]>();
        static ConcurrentQueue<byte[]> cmdQueue = new ConcurrentQueue<byte[]>();
        static string sql = "INSERT INTO public.sales (region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit) VALUES ";
        static int batchLimit = 1000;
        static int recordsRead = 0;
        static bool readingFile = false;

     
        static async Task Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            string file = "D:\\CSVMillion\\10m.csv";

            List<Task> allTasks = new List<Task>();
            readingFile = true;
            Task TFileReader = Task.Factory.StartNew(() => {
                string line;
                using (TextReader reader = new StreamReader(file, Encoding.Default))
                {
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (line.IndexOf("'") > 0)
                            line = line.Replace("'", "");

                        rowsQueue.Enqueue(Encoding.Default.GetBytes(line));
                        recordsRead++;
                    }
                    readingFile = false;
                }
            });

            int queueTask = 4;

            for (int i =0; i < queueTask; i++)
            {
                allTasks.Add(Task.Factory.StartNew(() =>
                {
                    try
                    {
                        StringBuilder sb = new StringBuilder();
                        string line;
                        string insert;
                        int batchSize = 0;

                        while (!rowsQueue.IsEmpty || readingFile)
                        {
                            if (rowsQueue.TryDequeue(out byte[] lineData))
                            {
                              
                                line = Encoding.Default.GetString(lineData);
                                string[] fields = line.Split(',');
                                sb.Append($"('{fields[0]}', '{fields[1]}', '{fields[2]}', '{fields[3]}', '{fields[4]}', '{fields[5]}', '{fields[6]}', '{fields[7]}', '{fields[8]}', '{fields[9]}', '{fields[10]}', '{fields[11]}', '{fields[12]}', '{fields[13]}'),");
                                batchSize++;
                            }
                            else
                            {
                                Task.Delay(100).Wait();
                            }

                            if (batchSize >= batchLimit)
                            {
                                batchSize = 0;
                                insert = sql + sb.ToString();
                                insert = insert.Remove(insert.Length - 1) + ";";
                                cmdQueue.Enqueue(Encoding.Default.GetBytes(insert));
                                sb.Clear();
                            }

                        }

                        if (batchSize > 0)
                        {
                            batchSize = 0;
                            insert = sql + sb.ToString();
                            insert = insert.Remove(insert.Length - 1) + ";";
                            cmdQueue.Enqueue(Encoding.Default.GetBytes(insert));
                            sb.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("ERRO: " + ex.Message);
                    }
                }));
            }

            int DbTasks = 16;

            for (int i = 0; i < DbTasks; i++)
            {
                allTasks.Add(Task.Factory.StartNew(() =>
                {
                    try
                    {
                        string line;
                        //using (NpgsqlConnection conexao = new NpgsqlConnection("Server=127.0.0.1;Port=5432;Database=teste_csv;User Id=teste_csv;Password=Senha123;"))
                        using (NpgsqlConnection conexao = new NpgsqlConnection("Server=192.168.15.221;Port=5432;Database=teste_csv;User Id=usuario;Password=Senha123;"))
                        {
                            conexao.Open();
                            {
                                while (!rowsQueue.IsEmpty || !cmdQueue.IsEmpty || readingFile)
                                {
                                    if (cmdQueue.TryDequeue(out byte[] lineData))
                                    {
                                        line = Encoding.Default.GetString(lineData);
                                        using (NpgsqlCommand cmd = conexao.CreateCommand())
                                        {
                                            cmd.CommandText = line;
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                    else
                                    {
                                        Task.Delay(300).Wait();
                                    }
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
            Console.WriteLine("Registros: " + recordsRead.ToString("000.000.000"));
            Console.WriteLine("Duração (segundos): " + stopwatch.Elapsed.TotalSeconds);
            Console.ReadKey();


        }
    }
}