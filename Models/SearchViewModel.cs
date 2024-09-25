namespace SolrFileUploader.Models
{
    public class SearchResultViewModel
    {
        public string Id { get; set; }
        public string Filename { get; set; }
        public List<string> Content { get; set; }
        public int PageNumber { get; set; }
    }

}
