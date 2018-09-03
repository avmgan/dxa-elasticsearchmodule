using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SI4T.Query.ElasticSearch.Models
{
    public class Bucket
    {
        public string Name { get; private set; }
        public long Count { get; private set; }

        public Bucket(string name, long count)
        {
            Name = name;
            Count = count;
        }
    }
}
