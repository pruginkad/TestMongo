// See https://aka.ms/new-console-template for more information
using app.Services;
using DbLayer.Models;
using DbLayer.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using MongoDB.Bson;
using TestMongo;
using TestMongo.Influx;

Console.WriteLine("Hello, World!");



try
{
  CancellationTokenSource tokenSource = new CancellationTokenSource();
  CancellationToken token = tokenSource.Token;
  List<Task> tasks = new List<Task>();

  var task1 = MongoTest.RunTest(tokenSource.Token);
  tasks.Add(task1);

  var task2 = InfluxTest.RunTest(token);
  tasks.Add(task2);


  Console.WriteLine("Press any key to stop emulation\n");
  Console.ReadKey();
  tokenSource.Cancel();

  Task.WaitAll(tasks.ToArray());
}
catch (Exception ex)
{
  Console.WriteLine(ex.Message);
}
