// See https://aka.ms/new-console-template for more information


using TestInflux;

try
{
  CancellationTokenSource tokenSource = new CancellationTokenSource();
  CancellationToken token = tokenSource.Token;
  List<Task> tasks = new List<Task>();
  Console.WriteLine("Press any key to stop emulation\n");

  Task task2 = Task.Run(() =>
  {    
    Console.ReadKey();
    tokenSource.Cancel();
  });

  tasks.Add(task2);

  var task1 = InfluxTest.RunTest(tokenSource.Token);
  tasks.Add(task1);


  

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
