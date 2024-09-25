using SolrNet.Attributes;

namespace SolrFileUploader.Models
{
    public class Book
    {
        [SolrUniqueKey("id")]
        public string Id { get; set; }

        [SolrField("title")]
        public string Title { get; set; }

        [SolrField("author")]
        public string Author { get; set; }

        [SolrField("publish_date")]
        public DateTime PublishDate { get; set; }
    }

}
