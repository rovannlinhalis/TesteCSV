using Npgsql;
using System.Collections.Concurrent;

namespace PaginatedParallelSQL
{
    internal class Program
    {
        static string sql = @"INSERT INTO sales2
(region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit, id, norder_id)
SELECT region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit, id, norder_id
FROM sales
order by norder_id offset {0} limit {1}";
        static int batchSize = 100000;
        static ConcurrentQueue<int> batches = new ConcurrentQueue<int>();
        static string connectionString = "Server=192.168.15.221;Port=5432;Database=teste_csv;User Id=usuario;Password=Senha123;CommandTimeout=60;";
        static void Main(string[] args)
        {
            using (NpgsqlConnection conexao = new NpgsqlConnection(connectionString))
            {
                conexao.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand("Select count(*) from sales", conexao))
                {
                    var obj = cmd.ExecuteScalar();
                    double rows = double.Parse(obj.ToString());
                    int pages = (int)Math.Ceiling(rows / batchSize);
                    for (int i = 0; i < pages; i++)
                        batches.Enqueue(i);
                }
            }

            Task[] tasks = new Task[6]; 
            for (int i = 0;i < tasks.Length; i++)
            {
                 tasks[i] = Task.Factory.StartNew(() => {

                     using (NpgsqlConnection conn = new NpgsqlConnection(connectionString))
                     {
                         conn.Open();
                         while (batches.TryDequeue(out var page) || !batches.IsEmpty)
                         {
                             Console.WriteLine("Página " + page );
                             using (NpgsqlCommand cmd = new NpgsqlCommand(string.Format(sql, page * batchSize, batchSize), conn))
                             {
                                 int x = cmd.ExecuteNonQuery();
                                 Console.WriteLine("Página " + page + " inseriu "+ x +" registros");
                             }
                         }
                     }
                });
            }

            Task.WhenAll(tasks).Wait();

            Console.WriteLine("FIM");
            Console.ReadKey();
        }
    }
}
