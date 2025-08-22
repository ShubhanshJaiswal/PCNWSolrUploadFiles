using SolrNet.Attributes;

namespace PCNWSolrUploadFiles.Data
{
    public class SolrPageDoc
    {
        [SolrUniqueKey("id")]
        public string Id { get; set; }

        [SolrField("projectId_i")]
        public int ProjectId { get; set; }

        [SolrField("fileId_s")]
        public string FileId { get; set; }

        [SolrField("filename_s")]
        public string Filename { get; set; }

        [SolrField("relativePath_s")]
        public string RelativePath { get; set; }

        [SolrField("page_i")]
        public int Page { get; set; }

        [SolrField("doc_type_s")]
        public string DocType { get; set; } // "page"

        [SolrField("content_txt")]
        public string Content { get; set; }

        [SolrField("checksum_s")]
        public string Checksum { get; set; }
    }
}
