using SI4T.Query.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SI4T.Query.ElasticSearch.Models
{
    /// <summary>
    /// Extended version of SI4T.Query's <see cref="SearchResults"/> returned by the CloudSearch Provider.
    /// </summary>
    /// <remarks>
    /// The ElasticSearch Provider uses this class to provide access to the Facets returned by CloudSearch.
    /// Since regular Solr also supports Facets, this facility should be moved up to <see cref="SearchResults"/>, in which case this class becomes redundant.
    /// </remarks>
    public class ElasticSearchResults : SearchResults
    {
        public List<Facet> Facets { get; set; }

        public ElasticSearchResults()
        {
            Facets = new List<Facet>();
        }
    }
}
