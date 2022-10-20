using DbLayer.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using TestMongo;

namespace DbLayer.Services
{
  public class RoutService
  {
    private readonly IMongoCollection<DBRoutLine> _collRouts;
    private readonly MongoClient _mongoClient;
    private readonly string CollName = "TSTable";
    public RoutService(
    )
    {
      var ConnectionString = 
        "mongodb://mongoservice:27018";
      _mongoClient = new MongoClient(
        ConnectionString);

      var mongoDatabase = _mongoClient.GetDatabase(
          "TSTest");

      var filter = new BsonDocument("name", CollName);
      var options = new ListCollectionNamesOptions { Filter = filter };

      if (!mongoDatabase.ListCollectionNames(options).Any())
      {
        var createOptions = new CreateCollectionOptions();

        var timeField = nameof(DBRoutLine.timestamp);
        var metaField = nameof(DBRoutLine.meta);
        createOptions.TimeSeriesOptions =
          new TimeSeriesOptions(timeField, metaField, TimeSeriesGranularity.Seconds);


        mongoDatabase.CreateCollection(
          CollName,
          createOptions);
      }

      _collRouts =
        mongoDatabase.GetCollection<DBRoutLine>(
          CollName
        );


      CreateIndexes();

    }

    private void CreateIndexes()
    {
      {
        IndexKeysDefinition<DBRoutLine> keys =
          new IndexKeysDefinitionBuilder<DBRoutLine>()
          .Descending(d => d.meta.counter);

        var indexModel = new CreateIndexModel<DBRoutLine>(
          keys, new CreateIndexOptions()
          { Name = "counter" }
        );

        _collRouts.Indexes.CreateOneAsync(indexModel);
      }
    }

    public async Task InsertManyAsync(List<DBRoutLine> list)
    {
      InsertManyOptions opt = new InsertManyOptions()
      {
        BypassDocumentValidation = true
      };

      await _collRouts.InsertManyAsync(list);
    }

    public async Task<long> GetMaxCount()
    {
      var last = await _collRouts
        .Find(i=> i.meta.counter > 0)
        .SortByDescending( i => i.meta.counter).FirstOrDefaultAsync();
      if (last == null)
      {
        return 0;
      }
      return last.meta.counter;
    }
  }
}
