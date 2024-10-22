using DocumentFormat.OpenXml.Packaging;
using Microsoft.EntityFrameworkCore;
using PCNWSolrUploadFiles.Data;
using System.Text;
using System.Text.Json;
using UglyToad.PdfPig;
using UglyToad.PdfPig.Util;
using Aspose.Words;


namespace PCNWSolrUploadFiles.Controllers
{
    public class UploadController
    {
        private readonly PcnwprojectDbContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly string _fileUploadPath;
        private readonly string _solrURL;
        private readonly ILogger<UploadController> _logger;

        public UploadController(PcnwprojectDbContext dbContext, IConfiguration configuration, ILogger<UploadController> logger)
        {
            _dbContext = dbContext;
            _configuration = configuration;
            _fileUploadPath = _configuration.GetSection("AppSettings")["FileUploadPath"] ?? string.Empty;
            _solrURL = _configuration.GetSection("AppSettings")["SolrURL"] ?? string.Empty;
            _logger = logger;
        }

        public async Task UploadAllFiles()
        {
            try
            {
                _logger.LogInformation("Started UploadAllFiles at: {time}", DateTime.Now);

                var baseDirectory = _fileUploadPath;
                if (!Directory.Exists(baseDirectory))
                {
                    _logger.LogError("Base directory not found: {baseDirectory}", baseDirectory);
                    throw new DirectoryNotFoundException($"Base directory not found: {baseDirectory}");
                }

                var yearFolders = Directory.GetDirectories(baseDirectory);

                foreach (var yearFolder in yearFolders)
                {
                    _logger.LogInformation("Processing year folder: {yearFolder}", yearFolder);
                    var monthFolders = Directory.GetDirectories(yearFolder);

                    foreach (var monthFolder in monthFolders)
                    {
                        _logger.LogInformation("Processing month folder: {monthFolder}", monthFolder);
                        var projectNumberFolders = Directory.GetDirectories(monthFolder);

                        foreach (var projectFolder in projectNumberFolders)
                        {
                            var projectNumber = Path.GetFileName(projectFolder);

                            _logger.LogInformation("Processing project: {projectNumber}", projectNumber);

                            var projectId = await _dbContext.Projects
                                .Where(p => p.ProjNumber == projectNumber && (bool)p.Publish! /*&& (p.IndexPdffiles == true || p.IndexPdffiles == null)*/)
                                .Select(p => p.ProjId)
                                .FirstOrDefaultAsync();

                            if (projectId == 0 )
                            {
                                _logger.LogWarning("Project ID not found or not eligible for indexing: {projectNumber}", projectNumber);
                                continue;
                            }

                            var lastIndexedDate = await _dbContext.Projects
                                .Where(p => p.ProjId == projectId)
                                .Select(p => p.SolrIndexPdfdt)
                                .FirstOrDefaultAsync();

                            lastIndexedDate = lastIndexedDate ?? DateTime.MinValue;

                            var allowedExtensions = new[] { ".pdf", ".docx",".doc", ".txt" };
                            var allFolders = Directory.GetDirectories(projectFolder, "*", SearchOption.AllDirectories);
                            var success = false;
                            foreach (var folder in allFolders)
                            {
                                //var files = Directory.GetFiles(folder);
                                //foreach (var item in files)
                                //{
                                //    var ext = Path.GetExtension(item).ToLower();
                                //}
                                foreach (var filePath in Directory.GetFiles(folder).Where(file =>
                                            allowedExtensions.Contains(Path.GetExtension(file).ToLower()) &&
                                            File.GetCreationTime(file) > lastIndexedDate))
                                {
                                    _logger.LogInformation("Processing file: {filePath}", filePath);

                                    var folderType = Path.GetFileName(folder); 
                                    success = await ProcessFile(projectId, filePath, folderType); 

                                    if (!success)
                                    {
                                        _logger.LogError("Failed to process file: {filePath}", filePath);
                                        continue;
                                    }
                                }
                            }


                            //foreach (var pdfFilePath in pdfFiles.ToArray())
                            //{
                            //    _logger.LogInformation("Processing PDF file: {pdfFilePath}", pdfFilePath);
                            //    var success = await ProcessPdfFile(projectId, pdfFilePath);
                            //    if (!success)
                            //    {
                            //        _logger.LogError("Failed to process PDF file: {pdfFilePath}", pdfFilePath);
                            //        continue;
                            //    }
                            //}

                            var project = await _dbContext.Projects.FirstOrDefaultAsync(m => m.ProjId == projectId);
                            if (project != null && success)
                            {
                                project.IndexPdffiles = false;
                                project.SolrIndexDt = DateTime.Now;
                                project.SolrIndexPdfdt = DateTime.Now; 
                                await _dbContext.SaveChangesAsync();

                                _logger.LogInformation("Project {projectNumber} successfully marked as indexed", projectNumber);
                            }
                            else if(!success)
                            {
                                _logger.LogInformation("Project {projectNumber} already marked as indexed", projectNumber);
                            }
                        }

                    }
                }

                _logger.LogInformation("Completed UploadAllFiles at: {time}", DateTime.Now);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while uploading files");
                throw;
            }
        }

        private async Task<bool> ProcessFile(int projectId, string filePath, string sourceType)
        {
            try
            {
                var fileName = Path.GetFileName(filePath);
                var guid = Guid.NewGuid().ToString();
                var tempFilePath = Path.Combine(Path.GetTempPath(), $"{guid}_{fileName}");

                _logger.LogInformation("Processing file: {fileName}, projectId: {projectId}, sourceType: {sourceType}", fileName, projectId, sourceType);

                File.Copy(filePath, tempFilePath, true);

                string extractedText = string.Empty;

                var extension = Path.GetExtension(filePath).ToLower();

                switch (extension)
                {
                    case ".pdf":
                        using (var pdf = PdfDocument.Open(tempFilePath))
                        {
                            extractedText = ExtractTextFromPdf(pdf);
                        }
                        break;

                    case ".docx":
                        using (var wordDocument = WordprocessingDocument.Open(tempFilePath, false))
                        {
                            extractedText = ExtractTextFromDocx(wordDocument);
                        }
                        break;

                    case ".doc":
                        Document doc = new Document(tempFilePath);
                        extractedText = doc.ToString(SaveFormat.Text);
                        break;

                    case ".txt":
                        extractedText = await File.ReadAllTextAsync(tempFilePath);
                        break;

                    default:
                        _logger.LogError("Unsupported file type: {fileName}", fileName);
                        return false;
                }


                var solrDocument = new
                {
                    add = new
                    {
                        doc = new
                        {
                            id = $"{projectId}_{guid}",
                            projectId,
                            filename = fileName,
                            content = extractedText,
                            sourceType
                        }
                    }
                };

                var jsonDocument = JsonSerializer.Serialize(solrDocument);

                using (var httpClient = new HttpClient())
                {
                    var content = new StringContent(jsonDocument, Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync(_solrURL + "/update?commit=true", content);

                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogError("Solr indexing failed for file: {filePath}, Status Code: {statusCode}", filePath, response.StatusCode);
                        return false;
                    }
                }

                _logger.LogInformation("Successfully processed file: {fileName}, projectId: {projectId}, sourceType: {sourceType}", fileName, projectId, sourceType);

                if (File.Exists(tempFilePath))
                {
                    File.Delete(tempFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing file: {filePath}", filePath);
                return false;
            }
        }

        private static string ExtractTextFromDocx(WordprocessingDocument wordDocument)
        {
            if (wordDocument.MainDocumentPart == null)
            {
                return string.Empty; 
            }

            var document = wordDocument.MainDocumentPart.Document;

            if (document == null || document.Body == null)
            {
                return string.Empty;
            }

            return document.Body.InnerText;
        }



        private static string ExtractTextFromPdf(PdfDocument pdf)
        {
            var sb = new StringBuilder();
            foreach (var page in pdf.GetPages())
            {
                sb.AppendLine(page.Text);
            }
            return sb.ToString();
        }
    }

}
