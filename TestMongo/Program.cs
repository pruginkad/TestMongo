// See https://aka.ms/new-console-template for more information
using DbLayer.Models;
using DbLayer.Services;
using MongoDB.Bson;

Console.WriteLine("Hello, World!");

var service = new RoutService();

try
{
  CancellationTokenSource tokenSource = new CancellationTokenSource();
  CancellationToken token = tokenSource.Token;


  var list = new List<DBRoutLine>();

  for (int i = 0; i < 10000; i++)
  {
    DBRoutLine line = new DBRoutLine();
    list.Add(line);
  }

  Task task = Task.Run(async () => {
    int max_counter = await service.GetMaxCount();

    bool recover = false;

    while (!token.IsCancellationRequested)
    {
      try
      {
        if (!recover)
        {
          foreach (DBRoutLine line in list)
          {
            line.meta.counter = ++max_counter;
            line.id = ObjectId.GenerateNewId().ToString();
            line.meta.id = line.id;
          }
        }        

        var t1 = DateTime.Now;

        await service.InsertManyAsync(list);

        var t2 = DateTime.Now;
        max_counter = await service.GetMaxCount();
        var t3 = DateTime.Now;

        Console
        .WriteLine(
          $"{max_counter}->Insert:{(int)(t2 - t1).TotalMilliseconds}, GetMax:{(int)(t3 - t2).TotalMilliseconds}");

        recover = false;
      }
      catch(Exception ex)
      {
        recover = true;
        await Task.Delay(3000);
        Console.WriteLine(ex.Message.ToString());
        service = new RoutService();

        max_counter = await service.GetMaxCount();
      }      
    }
  
  }, token);

  Console.WriteLine("Press any key to stop emulation\n");
  Console.ReadKey();
  tokenSource.Cancel();

  Task.WaitAll(task);
}
catch (Exception ex)
{
  Console.WriteLine(ex.Message);
}