using UglyToad.PdfPig;

namespace SolrFileUploader.Models
{
    public static class PdfHelper
    {
        public static string ExtractTextFromPdf(string filePath)
        {
            if (!File.Exists(filePath))
            {
                return string.Empty;
            }

            using (var document = PdfDocument.Open(filePath))
            {
                var text = new System.Text.StringBuilder();

                foreach (var page in document.GetPages())
                {
                    text.Append(page.Text);
                }

                return text.ToString();
            }
        }
    }
}
