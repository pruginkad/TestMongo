using System;
using System.Threading.Tasks;

namespace TSDBComparison
{
  class Program
  {
    static async Task Main(string[] args)
    {
      //await TestWrite();
      await TestRead();
    }

    static async Task TestWrite()
    {
      var items = DataGenerator.GenerateRawDataForOneYear(
        TestParams.RECORDS_COUNT,
        TestParams.OBJECTS_COUNT
      );

      var postgres = new PostgresHelper();
      postgres.TestWrite(items);

      //Console.WriteLine("\r\n\r\n");
      //var timescale = new TimescaleHelper();
      //timescale.TestWrite(items);

      //Console.WriteLine("\r\n\r\n");

      //var quest = new QuestHelper();
      //await quest.TestWrite(items);
    }

    static async Task TestRead()
    {
      var postgres = new PostgresHelper();
      for (int i = 0; i < 5; i++)
      {
        Console.WriteLine("******************");
        postgres.TestRead();
      }

      Console.WriteLine("\r\n\r\n");

      //var timescale = new TimescaleHelper();
      //for (int i = 0; i < 5; i++)
      //{
      //  Console.WriteLine("******************");
      //  timescale.TestRead();
      //}

      Console.WriteLine("\r\n\r\n");

      
      //var quest = new QuestHelper();
      //for (int i = 0; i < 5; i++)
      //{
      //  Console.WriteLine("******************");
      //  await quest.TestRead();
      //}
        
    }
  }
}
