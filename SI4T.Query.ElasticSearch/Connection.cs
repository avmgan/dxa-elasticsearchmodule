using Nest;
using SI4T.Query.ElasticSearch.Models;
using SI4T.Query.Models;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SI4T.Query.ElasticSearch
{
    public class Connection
    {
        /// <summary>
        /// URL of the ElasticSearch Search endpoint
        /// </summary>
        public string ServiceUrl { get; set; }

        /// <summary>
        /// Number of characters for auto-generated summary data
        /// </summary>
        public int AutoSummarySize { get; set; }

        /// <summary>
        /// Default page size
        /// </summary>
        public int DefaultPageSize { get; set; }

        /// <summary>
        /// Maximum number of Facets to return
        /// </summary>
        public int MaxNumberOfFacets { get; set; }

        public Connection(string serviceUrl)
        {
            ServiceUrl = serviceUrl;
            AutoSummarySize = 255;
            DefaultPageSize = 10;
            MaxNumberOfFacets = 100;
        }

        /// <summary>
        /// Run a query
        /// </summary>
        /// <param name="parameters">The query parameters</param>
        /// <returns>matching results</returns>
        public ElasticSearchResults ExecuteQuery(NameValueCollection parameters)
        {
            ElasticSearchResults result = new ElasticSearchResults();

            ElasticClient client = GetCloudSearchClient();

            SearchRequest request = BuildSearchRequest(parameters);

            var response = client.Search<SearchResult>(request);

            result.Items = response.Hits.Select(hit => CreateSearchResult(hit)).ToList();
            //result.Facets = (
            //        from f in response.Aggregations
            //        select new Facet
            //        {
            //            Name = f.Key,
            //            Buckets = f.Value.Meta.Select(b => new SI4T.Query.ElasticSearch.Models.Bucket(b.Value.ToString(), b.Count)).ToList()
            //        }).ToList();

            result.PageSize = Convert.ToInt32(request.Size);
            result.Total = Convert.ToInt32(response.Hits.Count);
            result.Start = Convert.ToInt32(request.From);
            result.QueryText = parameters["q"];
            return result;
        }


        private SearchRequest BuildSearchRequest(NameValueCollection parameters)
        {
            string start = parameters["start"] ?? "1";
            string rows = parameters["rows"] ?? DefaultPageSize.ToString(CultureInfo.InvariantCulture);
            //string facet = parameters["facet"];
            //if (!String.IsNullOrEmpty(facet))
            //{
            //    string facets = string.Join(", ", Array.ConvertAll(facet.Split(',').ToArray(), i => String.Format("\"{0}\":{{\"sort\":\"bucket\",\"size\":" + MaxNumberOfFacets + "}}", i.ToString())));
            //    facet = "{" + facets + "}";
            //}

            QueryContainer query = new MatchQuery
            {
                Field = "body",
                Query = parameters["q"]
            };

            Highlight highlight = new Highlight
            {
                Order = "score",
                Fields = new Dictionary<Field, IHighlightField>
                {
                    { "body", new HighlightField
                        {
                            Type = HighlighterType.Plain,
                            ForceSource = true,
                            FragmentSize = 150,
                            Fragmenter = HighlighterFragmenter.Span,
                            NumberOfFragments = 3,
                            NoMatchSize = 150
                        }
                    }
                }
            };

            return new SearchRequest
            {
                From = Convert.ToInt32(start) - 1, // SI4T uses 1 based indexing, but CloudSearch and Elasticsearch uses 0 based.
                Size = Convert.ToInt32(rows),
                Query = query,
                Highlight = highlight
            };
        }

        private ElasticClient GetCloudSearchClient()
        {
            string IndexName = string.Empty;
            string ElasticInstanceUri = string.Empty;

            if(!string.IsNullOrEmpty(ServiceUrl))
            {
                IndexName = ServiceUrl.Split('/').Last();
                ElasticInstanceUri = ServiceUrl.Substring(0, ServiceUrl.LastIndexOf("/"));
            }

            var node = new Uri(ElasticInstanceUri);
            var settings = new ConnectionSettings(node);
            settings.DefaultIndex(IndexName);
            return new ElasticClient(settings);
        }

        private SI4T.Query.Models.SearchResult CreateSearchResult(IHit<SearchResult> hit)
        {
            SI4T.Query.Models.SearchResult result = new SI4T.Query.Models.SearchResult { Id = hit.Id };

            result.PublicationId = hit.Source.PublicationId;
            result.Title = hit.Source.Title;
            result.Url = hit.Source.Url;
            result.Summary = hit.Source.Summary;

            //if (String.IsNullOrEmpty(result.Summary) && hit.Highlights.ContainsKey("body"))
            if (hit.Highlights.ContainsKey("body"))
            {
                // If no summary field is present in the index, use the highlight fragment from the body field instead.
                string autoSummary = hit.Highlights["body"].Highlights.FirstOrDefault();
                if (autoSummary.Length > AutoSummarySize)
                {
                    // To limit the size of the fragment in the Search Request.
                    // Therefore we truncate it here if needed.
                    autoSummary = autoSummary.Substring(0, AutoSummarySize) + "...";
                }
                result.Summary = autoSummary;
            }

            return result;
        }

    }
}
