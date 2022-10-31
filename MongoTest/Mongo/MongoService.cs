using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using System.Diagnostics;
using TSDBComparison;

namespace DbLayer.Services
{
  public class MongoService
  {
    private IMongoCollection<MonitoringItem> _coll;
    private readonly IMongoDatabase _db;
    private readonly MongoClient _mongoClient;
    private readonly string CollName = "TSTable";
    private readonly string DBName = "TSTest";
    public MongoService()
    {
      var ConnectionString = "mongodb://mongoservice:27018";
      _mongoClient = new MongoClient(ConnectionString);

      _db = _mongoClient.GetDatabase(DBName);
      CreateCollection().Wait();
    }

    public async Task DeleteCollection()
    {
      await _db.DropCollectionAsync(CollName);
    }

    public async Task CreateCollection()
    {
      var filter = new BsonDocument("name", CollName);
      var options = new ListCollectionNamesOptions { Filter = filter };

      if (!_db.ListCollectionNames(options).Any())
      {
        var createOptions = new CreateCollectionOptions();

        var timeField = nameof(MonitoringItem.Timestamp);
        var metaField = nameof(MonitoringItem.meta);
        createOptions.TimeSeriesOptions =
          new TimeSeriesOptions(timeField, metaField, TimeSeriesGranularity.Seconds);


        await _db.CreateCollectionAsync(
          CollName,
          createOptions);
      }

      _coll =
        _db.GetCollection<MonitoringItem>(
          CollName
        );


      await CreateIndexes();
    }

    private async Task CreateIndexes()
    {
      var stringPropertyNamesAndValues = typeof(MonitoringItemMeta).GetProperties();

      foreach (var prop in stringPropertyNamesAndValues)
      {
        var keys =
          Builders<MonitoringItem>.IndexKeys
          .Ascending($"{nameof(MonitoringItem.meta)}.{prop.Name}");

        var indexModel = new CreateIndexModel<MonitoringItem>(
           keys, new CreateIndexOptions()
           { Name = prop.Name });

        await _coll.Indexes.CreateOneAsync(indexModel);
      }

      //var keys1 =
      //    Builders<MonitoringItem>.IndexKeys
      //    .Ascending( t => t.Timestamp)
      //    .Ascending(t => t.meta.ObjectName)
      //    .Ascending(t => t.meta.ObjectType)
      //    .Ascending(t => t.meta.PropName)
      //    ;

      //var indexModel1 = new CreateIndexModel<MonitoringItem>(
      //   keys1, new CreateIndexOptions()
      //   { Name = "combo" });

      //await _coll.Indexes.CreateOneAsync(indexModel1);
    }

    public async Task InsertItems(List<MonitoringItem> list)
    {
      await _coll.InsertManyAsync(list);
    }

    public double GetMax()
    {
      var ret = _coll
        .AsQueryable()
        .OrderByDescending(t => t.meta.PropValue)
        .First();
      ;

      return ret.meta.PropValue;
    }

    public double GetMin()
    {
      var ret = _coll
        .AsQueryable()
        .OrderBy(t => t.meta.PropValue)
        .First();
      ;

      return ret.meta.PropValue;
    }
    public enum E_GROUPBY
    {
      years,
      months,
      days,
      hours      
    }

    private static void Log(IAggregateFluent<BsonDocument> filter)
    {
      Debug.WriteLine(filter.ToString());
      Debug.WriteLine("");
    }

    public List<BsonDocument> Agregate(
      DateTime startTime,
      DateTime endTime,
      E_GROUPBY groupBy,
      string ObjectName = "Jason",
      string ObjectType = "Sunshine",
      string PropName = "Coolness"
      )
    {
      BsonDocument pipelineStage1 = new BsonDocument{
          {
              "date", new BsonDocument{
                  {"$dateToParts", new BsonDocument{ { "date", "$Timestamp" } } }
              }
          },
          {"meta", 1 }
      };

      BsonDocument pipelineStageTime = new BsonDocument{
                    { "year", "$date.year" }
                  }      ;
      if (groupBy >= E_GROUPBY.months)
      {
        pipelineStageTime.Add(new BsonElement("month", "$date.month"));
      }
      if (groupBy >= E_GROUPBY.days)
      {
        pipelineStageTime.Add(new BsonElement("day", "$date.day"));
      }
      if (groupBy >= E_GROUPBY.hours)
      {
        pipelineStageTime.Add(new BsonElement("hour", "$date.hour"));
      }     
      

      BsonDocument pipelineStage2 = new BsonDocument{
          {
              "_id", new BsonDocument{
                  { "date", pipelineStageTime
                }
              }
          },
        {          
          "avg_val", new BsonDocument{
            {"$avg", "$meta.PropValue" }
          }
        },
        {
          "min_val", new BsonDocument{
            {"$min", "$meta.PropValue" }
          }
        }
        ,
        {
          "max_val", new BsonDocument{
            {"$max", "$meta.PropValue" }
          }
        }
        ,
        {
          "NumberOfDocuments", new BsonDocument{
            {"$count", new BsonDocument{ } }
          }
        }
      };

      var test = _coll
        .Aggregate()
        .Match(o => o.meta.ObjectName == ObjectName
          && o.meta.ObjectType == ObjectType
          && o.meta.PropName == PropName
          && o.Timestamp >= startTime
          && o.Timestamp <= endTime)
        .SortBy(t => t.Timestamp)
        .Project(pipelineStage1)
        .Group(pipelineStage2);

      Log(test);

      var list = test.ToList();
      return list;
    }
  }
}
