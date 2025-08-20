using Aspose.Words;
using DocumentFormat.OpenXml.Packaging;
using Microsoft.EntityFrameworkCore;
using PCNWSolrUploadFiles.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using UglyToad.PdfPig;

namespace PCNWSolrUploadFiles.Controllers
{
    public class UploadController
    {
        private readonly PcnwprojectDbContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly string _fileUploadPath;
        private readonly string _solrURL;
        private readonly ILogger<UploadController> _logger;

        // Reuse HttpClient
        private static readonly HttpClient _http = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(60)
        };

        public UploadController(
            PcnwprojectDbContext dbContext,
            IConfiguration configuration,
            ILogger<UploadController> logger)
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
                                .Where(p => p.ProjNumber == projectNumber && (bool)p.Publish!)
                                .Select(p => p.ProjId)
                                .FirstOrDefaultAsync();

                            if (projectId == 0)
                            {
                                _logger.LogWarning("Project ID not found or not eligible: {projectNumber}", projectNumber);
                                continue;
                            }

                            // Per-project scan marker
                            var scanId = Guid.NewGuid().ToString("N");

                            var allowedExtensions = new[] { ".pdf", ".docx", ".doc", ".txt" };
                            var fileEnum = Directory.EnumerateFiles(projectFolder, "*.*", SearchOption.AllDirectories)
                                                    .Where(f => allowedExtensions.Contains(Path.GetExtension(f).ToLower()));

                            var docsBatch = new List<object>();
                            const int batchSize = 100;
                            var anySent = false;

                            foreach (var file in fileEnum)
                            {
                                var relativePath = GetRelativePath(projectFolder, file);
                                var id = BuildSolrId(projectId, relativePath);
                                var parentDir = Path.GetDirectoryName(file);
                                var folderType = Path.GetFileName(parentDir ?? string.Empty);

                                // Extract content
                                var (ok, extractedText, checksum) = await ExtractFileContent(file);
                                if (!ok)
                                {
                                    _logger.LogError("Skipping file due to extraction failure: {file}", file);
                                    continue;
                                }

                                var info = new FileInfo(file);
                                var lastModifiedUtc = info.LastWriteTimeUtc;

                                // Build Solr doc (use dynamic fields for schema friendliness)
                                var solrDoc = new Dictionary<string, object?>
                                {
                                    ["id"] = id,
                                    ["projectId"] = projectId,
                                    ["filename_s"] = Path.GetFileName(file),
                                    ["relativePath_s"] = relativePath.Replace('\\', '/'),
                                    ["sourceType_s"] = folderType,
                                    ["content_txt"] = extractedText ?? string.Empty,
                                    ["checksum_s"] = checksum,
                                    ["lastModified_dt"] = lastModifiedUtc.ToString("o"),
                                    ["fileSize_l"] = info.Length,
                                    ["scanId_s"] = scanId
                                };

                                // Add to batch as { "doc": { ... } } so we can POST { "add": [ {doc}, {doc} ] }
                                docsBatch.Add(new { doc = solrDoc });

                                if (docsBatch.Count >= batchSize)
                                {
                                    var addPayload = JsonSerializer.Serialize(new { add = docsBatch });
                                    var addOk = await PostSolrJsonAsync($"{_solrURL}/update", addPayload, _logger);
                                    if (!addOk)
                                    {
                                        _logger.LogError("Failed posting batch to Solr (project {projectId}).", projectId);
                                    }
                                    else
                                    {
                                        anySent = true;
                                    }
                                    docsBatch.Clear();
                                }
                            }

                            // Flush remainder
                            if (docsBatch.Any())
                            {
                                var addPayload = JsonSerializer.Serialize(new { add = docsBatch });
                                var addOk = await PostSolrJsonAsync($"{_solrURL}/update", addPayload, _logger);
                                if (!addOk)
                                {
                                    _logger.LogError("Failed posting final batch to Solr (project {projectId}).", projectId);
                                }
                                else
                                {
                                    anySent = true;
                                }
                                docsBatch.Clear();
                            }

                            // Delete anything not touched in this scan (stale docs)
                            // projectId:<id> AND NOT scanId_s:<scanId>
                            var deleteQuery = $"projectId:{projectId} AND NOT scanId_s:{scanId}";
                            var deletePayload = JsonSerializer.Serialize(new { delete = new { query = deleteQuery } });
                            var delOk = await PostSolrJsonAsync($"{_solrURL}/update", deletePayload, _logger);
                            if (!delOk)
                            {
                                _logger.LogError("Failed deleteByQuery cleanup for project {projectId}.", projectId);
                            }

                            // Commit once per project
                            var commitOk = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
                            if (!commitOk)
                            {
                                _logger.LogError("Failed commit for project {projectId}.", projectId);
                            }

                            // Update DB timestamps/flags (if anything happened we still mark timestamps)
                            var project = await _dbContext.Projects.FirstOrDefaultAsync(m => m.ProjId == projectId);
                            if (project != null)
                            {
                                project.IndexPdffiles = false;
                                project.SolrIndexDt = DateTime.Now;
                                project.SolrIndexPdfdt = DateTime.Now;
                                await _dbContext.SaveChangesAsync();
                                _logger.LogInformation("Project {projectNumber} indexed & committed (sent: {sent}).", projectNumber, anySent);
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

        // -----------------------------
        // Helpers
        // -----------------------------

        private static string GetRelativePath(string baseDir, string fullPath)
        {
            var baseUri = new Uri(AppendDirectorySeparatorChar(baseDir));
            var fullUri = new Uri(fullPath);
            var relative = Uri.UnescapeDataString(baseUri.MakeRelativeUri(fullUri).ToString());
            return relative.Replace('/', Path.DirectorySeparatorChar);
        }

        private static string AppendDirectorySeparatorChar(string path)
        {
            if (!path.EndsWith(Path.DirectorySeparatorChar))
                return path + Path.DirectorySeparatorChar;
            return path;
        }

        private static string BuildSolrId(int projectId, string relativePath)
        {
            var norm = relativePath.Replace('\\', '/');
            return $"{projectId}|{norm}";
        }

        private static string ComputeSHA256(string filePath)
        {
            using var sha = SHA256.Create();
            using var fs = File.OpenRead(filePath);
            var hash = sha.ComputeHash(fs);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }

        private async Task<bool> PostSolrJsonAsync(string url, string json, ILogger logger, int maxRetries = 3)
        {
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var content = new StringContent(json, Encoding.UTF8, "application/json");
                    var resp = await _http.PostAsync(url, content);
                    if (resp.IsSuccessStatusCode) return true;

                    logger.LogError("Solr call failed (attempt {attempt}/{max}): {status} {reason}",
                        attempt, maxRetries, resp.StatusCode, resp.ReasonPhrase);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Solr call exception (attempt {attempt}/{max})", attempt, maxRetries);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(300 * attempt * attempt));
            }
            return false;
        }

        private async Task<(bool success, string content, string checksum)> ExtractFileContent(string filePath)
        {
            try
            {
                var fileName = Path.GetFileName(filePath);
                var guid = Guid.NewGuid().ToString();
                var tempFilePath = Path.Combine(Path.GetTempPath(), $"{guid}_{fileName}");

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
                        var doc = new Document(tempFilePath);
                        extractedText = doc.ToString(SaveFormat.Text);
                        break;

                    case ".txt":
                        extractedText = await File.ReadAllTextAsync(tempFilePath);
                        break;

                    default:
                        _logger.LogError("Unsupported file type: {fileName}", fileName);
                        return (false, string.Empty, string.Empty);
                }

                var checksum = ComputeSHA256(tempFilePath);

                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);

                return (true, extractedText, checksum);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error extracting file content: {filePath}", filePath);
                return (false, string.Empty, string.Empty);
            }
        }

        private static string ExtractTextFromDocx(WordprocessingDocument wordDocument)
        {
            if (wordDocument.MainDocumentPart == null) return string.Empty;

            var document = wordDocument.MainDocumentPart.Document;
            if (document?.Body == null) return string.Empty;

            return document.Body.InnerText ?? string.Empty;
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
