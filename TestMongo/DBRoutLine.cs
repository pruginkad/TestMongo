using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbLayer.Models
{
  [BsonIgnoreExtraElements]
  public class DBRoutLine
  {
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string id { get; set; }
    public DBRoutLineMeta meta { get; set; } = new DBRoutLineMeta();

    public DateTime timestamp { get; set; } = DateTime.UtcNow;
    public string some_data { get; set; } = DateTime.Now.ToString();
  }
}
