// See https://aka.ms/new-console-template for more information
using app.Services;
using DbLayer.Models;
using DbLayer.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using MongoDB.Bson;
using System.Runtime.CompilerServices;
using TestDatabases.Redis;
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

  //var task3 = RedisTest.RunTest(token);
  //tasks.Add(task3);


  Console.WriteLine("Press any key to stop emulation\n");
  Console.ReadKey();
  tokenSource.Cancel();

  Task.WaitAll(tasks.ToArray());
}
catch (Exception ex)
{
  Console.WriteLine(ex.Message);
}

static class ConsoleWrite
{
  static object _locker = new object();
  static public void WriteConsole(string str, int line)
  {
    lock(_locker)
    {
      Console.SetCursorPosition(0, line);
      Console.Write(str);
    }    
  }
}
