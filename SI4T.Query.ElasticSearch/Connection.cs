using Nest;
using Newtonsoft.Json;
using SI4T.Query.ElasticSearch.Models;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;

namespace SI4T.Query.ElasticSearch
{
    public class Connection
    {
        /// <summary>
        /// URL of the ElasticSearch Search endpoint
        /// </summary>
        public string ServiceUrl { get; set; }

        public string UserId { get; set; }

        public string Password { get; set; }

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

        public Connection(string serviceUrl, string userId = null, string password = null)
        {
            ServiceUrl = serviceUrl;
            UserId = userId;
            Password = password;
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

            ISearchResponse<object> response = client.Search<object>(request);

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

            List<QueryContainer> mustClauses = new List<QueryContainer>();
            List<QueryContainer> filterClauses = new List<QueryContainer>();

            if (parameters["q"] != null)
            {
                mustClauses.Add(new QueryStringQuery
                {
                    Query = parameters["q"],
                    AnalyzeWildcard = false,
                    Lenient = false
                });
            }

            if (!string.IsNullOrEmpty(parameters["fq"]))
            {
                string[] filterQueryParameters = parameters["fq"].Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string item in filterQueryParameters)
                {
                    string[] keyValue = item.Split(':');
                    {
                        if (keyValue.Length > 0)
                        {
                            if (keyValue[0] == "date")
                            {
                                string[] range = keyValue[1].Split(',');
                                if (range.Length > 0)
                                {
                                    filterClauses.Add(new DateRangeQuery
                                    {
                                        Field = new Field("date"),
                                        GreaterThanOrEqualTo = DateTime.ParseExact(range[0], "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                        LessThanOrEqualTo = DateTime.ParseExact(range[1], "yyyy-MM-dd", CultureInfo.InvariantCulture)
                                    });
                                }
                                else
                                {
                                    filterClauses.Add(new TermQuery
                                    {
                                        Field = new Field("date"),
                                        Value = keyValue[1]
                                    });
                                }
                            }
                            else
                            {
                                mustClauses.Add(new TermQuery
                                {
                                    Field = new Field(keyValue[0]),
                                    Value = keyValue[1]
                                });
                            }
                        }
                    }

                }
            }

            QueryContainer query = new BoolQuery
            {
                Must = mustClauses,
                Filter = filterClauses
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

            List<ISort> sort = new List<ISort>();

            if (!string.IsNullOrEmpty(parameters["sort"]))
            {
                string[] sortQueryParameter = parameters["sort"].Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                if (sortQueryParameter.Length > 0)
                {
                    sort.Add(new SortField { Field = sortQueryParameter[0], Order = sortQueryParameter[1] == "desc" ? SortOrder.Descending : SortOrder.Ascending });
                }
            }

            return new SearchRequest
            {
                From = Convert.ToInt32(start) - 1, // SI4T uses 1 based indexing, but CloudSearch and Elasticsearch uses 0 based.
                Size = Convert.ToInt32(rows),
                Query = query,
                Highlight = highlight,
                Sort = sort
            };
        }

        private ElasticClient GetCloudSearchClient()
        {
            string IndexName = string.Empty;
            string ElasticInstanceUri = string.Empty;

            if (!string.IsNullOrEmpty(ServiceUrl))
            {
                IndexName = ServiceUrl.Split('/').Last();
                ElasticInstanceUri = ServiceUrl.Substring(0, ServiceUrl.LastIndexOf("/"));
            }

            Uri node = new Uri(ElasticInstanceUri);
            ConnectionSettings settings = new ConnectionSettings(node);
            if (UserId != null && Password != null)
            {
                settings.BasicAuthentication(UserId, Password);
            }
            settings.DefaultIndex(IndexName);
            return new ElasticClient(settings);
        }

        private SI4T.Query.Models.SearchResult CreateSearchResult(IHit<object> hit)
        {
            SI4T.Query.Models.SearchResult result = new SI4T.Query.Models.SearchResult { Id = hit.Id };
            Dictionary<string, object> fields = JsonConvert.DeserializeObject<Dictionary<string, object>>(hit.Source.ToString());

            foreach (KeyValuePair<string, object> field in fields)
            {
                string type = field.Value.GetType().ToString();
                string fieldname = field.Key;

                switch (fieldname)
                {
                    case "publicationid":
                        result.PublicationId = int.Parse(field.Value.ToString());
                        break;
                    case "title":
                        result.Title = field.Value.ToString();
                        break;
                    case "url":
                        result.Url = field.Value.ToString();
                        break;
                    case "summary":
                        result.Summary = field.Value.ToString();
                        break;
                    default:
                        object data = null;
                        switch (type)
                        {
                            case "arr": //TODO: Make smarter
                                data = field.Value;
                                break;
                            default:
                                data = field.Value.ToString();
                                break;
                        }
                        result.CustomFields.Add(fieldname, data);
                        break;
                }
            }

            if (string.IsNullOrEmpty(result.Summary) && hit.Highlights.ContainsKey("body"))
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
