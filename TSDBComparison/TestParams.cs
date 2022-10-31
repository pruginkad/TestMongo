using System;

namespace TSDBComparison
{
  public static class TestParams
  {
    public const int RECORDS_COUNT = 10000000;
    public const int OBJECTS_COUNT = 5000;

    public static MonitoringItem ONE_DAY_TEST = new MonitoringItem()
    {
      ObjectName = "Jason",
      ObjectType = "Mightful",
      PropName = "Mood",
      Timestamp = new DateTime(2022, 3, 18)
    };

    public static MonitoringItem ONE_MONTH_TEST = new MonitoringItem()
    {
      ObjectName = "Ferdinand",
      ObjectType = "Terrifying",
      PropName = "Temperature",
      Timestamp = new DateTime(2022, 6, 1)
    };

    public static MonitoringItem ONE_YEAR_TEST = new MonitoringItem()
    {
      ObjectName = "Jason",
      ObjectType = "Sunshine",
      PropName = "CPU"
    };
  }
}
