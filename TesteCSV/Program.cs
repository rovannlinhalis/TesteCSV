
using MySqlConnector;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using TesteCSV;

namespace TesteCSV
{
    internal class Program
    {
        static ConcurrentQueue<byte[]> fila = new ConcurrentQueue<byte[]>();
        static ConcurrentBag<int> countItens = new ConcurrentBag<int>();
        static string sql = "INSERT INTO sales (region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit) VALUES ";
        static int loteSize = 1000; //Tamanho do lote que vai ser enviado no insert
        static int recordsRead = 0; 
        static int nThreads = 8; //Número de threads que vão inserir no banco, utilize uma quantidade menor que o numero de threads da CPU
        static int limiteFila = (nThreads * (loteSize* 10));  //Limita o tamanho da fila pra não comer toda memória disponível
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

                        while (fila.Count > limiteFila)
                        {
                            //fila muito cheia, espera
                            Thread.Sleep(5000);
                        }

                    }
                    Console.WriteLine("Encerrou leitura do arquivo " + (DateTime.Now - inicio).TotalSeconds + " seg.");
                    readingFile = false;
                }
            });

           

            for (int i = 0; i < nThreads; i++)
            {
                allTasks.Add(Task.Factory.StartNew(() =>
                {
                    try
                    {

                        StringBuilder sb = new StringBuilder();
                        string line;
                        string insert;
                        int loteCount = 0;
                        using (MySqlConnection conexao = new MySqlConnection("Server=127.0.0.1;Port=3306;Database=teste_csv;Uid=root;Pwd=Senha123;Pooling=True;UseCompression=True;"))
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
                                        countItens.Add(loteCount);
                                        loteCount = 0;
                                        insert = sql + sb.ToString();
                                        insert = insert.Remove(insert.Length - 1) + ";";

                                        using (MySqlCommand cmd = conexao.CreateCommand())
                                        {
                                            cmd.CommandText = insert;
                                            cmd.ExecuteNonQuery();
                                        }
                                        sb.Clear();
                                    }

                                }

                                if (loteCount > 0)
                                {
                                    countItens.Add(loteCount);
                                    loteCount = 0;
                                    insert = sql + sb.ToString();
                                    insert = insert.Remove(insert.Length - 1) + ";";

                                    using (MySqlCommand cmd = conexao.CreateCommand())
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

                    Console.WriteLine("Encerrou thread DB");
                }));
            }

            await Task.WhenAll(allTasks);
            Console.WriteLine("FIM");
            Console.WriteLine("Registros lidos: " + recordsRead);
            Console.WriteLine("Registros escritos: " + countItens.Sum());
            Console.WriteLine("Duração (segundos): " + (DateTime.Now - inicio).TotalSeconds);
            Console.ReadKey();


        }
    }
}