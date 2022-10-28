using System;
using System.Collections.Generic;
using System.Linq;

namespace TSDBComparison
{
  public static class DataGenerator
  {
    private static Random Random = new Random();

    public static List<MonitoringItem> GenerateRawDataForOneYear(
      int recordsCount,
      int objectsCount
    )
    {
      if (recordsCount <= 0)
        throw new ArgumentException();

      Console.Out.WriteLine($"Data generation of {recordsCount} items for {objectsCount} objects started.");
      var objects = GenerateObjects(objectsCount);
      var result = new List<MonitoringItem>();
      // TODO: unique time?

      while (result.Count < recordsCount)
      {
        for (var i = 0; i < objects.Count; i++)
        {
          for (var j = 0; j < PropNames.Count; j++)
          {
            result.Add(
              new MonitoringItem()
              {
                Timestamp = GetRandomTimestamp(),
                meta = new MonitoringItemMeta()
                {
                  ObjectName = objects[i].Name,
                  ObjectType = objects[i].Type,
                  PropName = PropNames[j],
                  PropValue = GetRandomNumber(10, 300)
                }                
              }
            );

            if (recordsCount == result.Count)
              break;
          }

          if (recordsCount == result.Count)
            break;
        }
      }

      Console.Out.WriteLine($"Data generation of {recordsCount} items for {objectsCount} objects completed.");

      return result.OrderBy(i => i.Timestamp).ToList();
    }

    private static List<MonitoredObject> GenerateObjects(int count)
    {
      if (count <= 0)
        throw new ArgumentException();

      var result = new List<MonitoredObject>();
      var k = 1;

      while (result.Count < count)
      {
        for (var i = 0; i < ObjectNames.Count; i++)
        {
          for (var j = 0; j < ObjectTypes.Count; j++)
          {
            result.Add(
              new MonitoredObject()
              {
                Name = ObjectNames[i] + (k == 1 ? "" : k.ToString()),
                Type = ObjectTypes[j]
              }
            );

            if (count == result.Count)
              break;
          }

          if (count == result.Count)
            break;
        }

        k++;
      }

      return result;
    }

    private static double GetRandomNumber(double minimum, double maximum)
    {
      return Random.NextDouble() * (maximum - minimum) + minimum;
    }

    private static DateTime GetRandomTimestamp()
    {
      return DateTime.Today.AddYears(-1)
        .AddDays(Random.Next(365))
        .AddHours(Random.Next(23))
        .AddMinutes(Random.Next(59))
        .AddSeconds(Random.Next(59))
        .AddMilliseconds(Random.Next(999));
    }

    private static List<string> ObjectNames = new List<string>()
    {
      "Jason",
      "Jan",
      "Mario",
      "Zinaida",
      "Alfred",
      "Mononog",
      "Unicorn",
      "Anjela",
      "It",
      "Chica",
      "Ferdinand",
      "Akira",
      "Alpha",
      "Beta",
      "Zeta",
      "Julio",
      "Richard",
      "Alf",
      "Carl",
      "Baltazar",
      "BoJack",
      "Bolivar",
      "Frodo",
      "Sam",
      "Pippin",
      "Merry",
      "Bilbo",
      "Legolas",
      "Gimli",
      "Gandalf",
      "Galadriel",
      "Arwen",
      "Elrond",
      "Gollum",
      "Sauron",
      "Saruman",
      "Harry",
      "Marvolo",
      "Myrtle",
      "Minerva",
      "Cersei",
      "Tyrion",
      "Varys",
      "Daenerys",
      "Arya",
      "Joffrey",
      "Melisandre",
      "Bathilda",
      "Sirius",
      "Cedric",
      "Albus",
      "Dudley",
      "Petunia",
      "Cornelius",
      "Hermione",
      "Godric",
      "Igor",
      "Neville",
      "Xenophilius",
      "Remus",
      "Draco",
      "Rufus",
      "Dolores"
    }.Distinct().ToList();

    private static List<string> ObjectTypes = new List<string>()
    {
      "Sunshine",
      "Awesome",
      "Mightful",
      "Solid",
      "Yummy",
      "Terrifying",
      "Celebrity",
      "ElTorro",
      "Tomato",
      "Сaramba",
      "Perpendicular",
      "Lovegood",
      "Targaryen",
      "Lannister"
    };

    private static List<string> PropNames = new List<string>()
    {
      "Temperature",
      "CPU",
      "Mood",
      "Coolness"
    };
  }
}
