using app.Models;
using app.Services;
using DbLayer.Models;
using DbLayer.Services;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestMongo.Influx
{
  internal class InfluxTest
  {
    static public async Task RunTest(CancellationToken token)
    {
      IConfiguration config = new ConfigurationBuilder()
      .AddJsonFile("appsettings.json")
      .AddEnvironmentVariables()
      .Build();

      var service = new InfluxDBService(config);
      var list = new List<AltitudeModel>();

      for (int i = 0; i < 10000; i++)
      {
        AltitudeModel line = new AltitudeModel();
        list.Add(line);
      }

      Task task = Task.Run(async () => {
        var max_counter = await service.GetMaxCount();

        bool recover = false;
        Random random= new Random();

        while (!token.IsCancellationRequested)
        {
          try
          {
            if (!recover)
            {
              foreach (AltitudeModel line in list)
              {
                line.counter = ++max_counter;
                line.new_counter = line.counter- random.Next(100000,2000000);
                line.timestamp = DateTime.UtcNow;
                line.some_data = random.Next(0, 100);
              }
            }

            var t1 = DateTime.Now;

            await service.InsertManyAsync(list);

            var t2 = DateTime.Now;
            max_counter = await service.GetMaxCount();
            var t3 = DateTime.Now;

            Console
            .WriteLine(
              $"inflx:{max_counter / 1000000.0}->Insert:{(int)(t2 - t1).TotalMilliseconds}, GetMax:{(int)(t3 - t2).TotalMilliseconds}");

            recover = false;
          }
          catch (Exception ex)
          {
            recover = true;
            await Task.Delay(3000);
            Console.WriteLine(ex.Message.ToString());
            service = new InfluxDBService(config);

            max_counter = await service.GetMaxCount();
          }
        }

      }, token);
    }
  }
}
