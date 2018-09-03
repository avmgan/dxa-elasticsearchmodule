using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SI4T.Query.ElasticSearch.Models
{
    public class Facet
    {
        public string Name { get; set; }
        public List<Bucket> Buckets { get; set; }
    }
}
