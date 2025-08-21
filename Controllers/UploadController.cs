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
        private readonly string _solrURL; // e.g., http://localhost:8983/solr/pcnw
        private readonly ILogger<UploadController> _logger;

        // Reuse HttpClient across the app domain
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
            _solrURL = TrimTrailingSlash(_configuration.GetSection("AppSettings")["SolrURL"] ?? string.Empty);
            _logger = logger;

            if (string.IsNullOrWhiteSpace(_solrURL))
                _logger.LogError("SolrURL is not configured. Set AppSettings:SolrURL to your Solr core/collection URL.");
            if (string.IsNullOrWhiteSpace(_fileUploadPath))
                _logger.LogError("FileUploadPath is not configured. Set AppSettings:FileUploadPath.");
        }

        /// <summary>
        /// One-click reset: deletes ALL documents in the configured Solr core/collection and commits.
        /// </summary>
        public async Task<bool> ClearSolrAllAsync()
        {
            try
            {
                var updateUrl = $"{_solrURL}/update";
                _logger.LogWarning("Clearing ALL documents from Solr at: {url}", updateUrl);

                var deletePayload = JsonSerializer.Serialize(new { delete = new { query = "*:*" } });
                var delOk = await PostSolrJsonAsync(updateUrl, deletePayload, _logger);
                if (!delOk)
                {
                    _logger.LogError("Solr deleteByQuery(*:*) failed.");
                    return false;
                }

                var commitOk = await PostSolrJsonAsync($"{updateUrl}?commit=true", "{}", _logger);
                if (!commitOk)
                {
                    _logger.LogError("Solr commit after clear failed.");
                    return false;
                }

                _logger.LogInformation("Solr core cleared successfully.");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception while clearing Solr.");
                return false;
            }
        }

        /// <summary>
        /// Main indexer: scans folders and sends files to Solr one-by-one with rich logging.
        /// Only posts when files exist; also performs deletion cleanup to reflect removals.
        /// </summary>
        public async Task UploadAllFiles()
        {
            var overallStart = DateTime.UtcNow;

            try
            {
                _logger.LogInformation("Started UploadAllFiles at: {timeLocal} ({timeUtc} UTC)", DateTime.Now, DateTime.UtcNow);

                var baseDirectory = _fileUploadPath;
                if (!Directory.Exists(baseDirectory))
                {
                    _logger.LogError("Base directory not found: {baseDirectory}", baseDirectory);
                    throw new DirectoryNotFoundException($"Base directory not found: {baseDirectory}");
                }

                // ---- Folder selection ----
                // Keep your current constraints or generalize as needed.
                // Currently emulates your sample: 2025 / 07 / 25070103
                var yearFolders = Directory.GetDirectories(baseDirectory).Where(m => Path.GetFileName(m) == "2025");

                foreach (var yearFolder in yearFolders)
                {
                    _logger.LogInformation("Processing year folder: {yearFolder}", yearFolder);
                    var monthFolders = Directory.GetDirectories(yearFolder).Where(m => Path.GetFileName(m) == "07");

                    foreach (var monthFolder in monthFolders)
                    {
                        _logger.LogInformation("Processing month folder: {monthFolder}", monthFolder);
                        var projectNumberFolders = Directory.GetDirectories(monthFolder).Where(m => Path.GetFileName(m) == "25070103");

                        foreach (var projectFolder in projectNumberFolders)
                        {
                            var projectStopwatch = System.Diagnostics.Stopwatch.StartNew();
                            var projectNumber = Path.GetFileName(projectFolder);
                            _logger.LogInformation("Processing project folder: {projectNumber} at {projectFolder}", projectNumber, projectFolder);

                            var projectId = await _dbContext.Projects
                                .Where(p => p.ProjNumber == projectNumber && (bool)p.Publish!)
                                .Select(p => p.ProjId)
                                .FirstOrDefaultAsync();

                            if (projectId == 0)
                            {
                                _logger.LogWarning("Project ID not found or not eligible (Publish=false): {projectNumber}. Skipping.", projectNumber);
                                continue;
                            }

                            // Gather files (supported extensions only)
                            var allowedExtensions = new[] { ".pdf", ".docx", ".doc", ".txt" };
                            var files = Directory.EnumerateFiles(projectFolder, "*.*", SearchOption.AllDirectories)
                                                 .Where(f => allowedExtensions.Contains(Path.GetExtension(f).ToLower()))
                                                 .ToList();

                            int totalFiles = files.Count;
                            int newOrUpdatedCount = 0;
                            int unchangedTouchedCount = 0;
                            int failCount = 0;

                            _logger.LogInformation("Project {projectNumber} (Id: {projectId}): Found {count} files to process.",
                                projectNumber, projectId, totalFiles);

                            // If there are no files, don't post anything; just clean Solr for this project and commit.
                            if (totalFiles == 0)
                            {
                                _logger.LogInformation("Project {projectNumber}: No supported files found. Cleaning all project docs from Solr.", projectNumber);

                                bool cleaned = await DeleteAllDocsForProjectAsync(projectId);
                                if (!cleaned)
                                    _logger.LogError("Project {projectNumber}: Failed to delete existing Solr docs for empty project.", projectNumber);

                                bool commitOkEmpty = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
                                if (!commitOkEmpty)
                                    _logger.LogError("Project {projectNumber}: Commit failed after empty-project cleanup.", projectNumber);

                                projectStopwatch.Stop();
                                _logger.LogInformation(
                                    "Project {projectNumber} SUMMARY (EMPTY): Total={total}, New/Updated={ok}, UnchangedTouched={touched}, Failed={failed}, Elapsed={ms}ms",
                                    projectNumber, totalFiles, newOrUpdatedCount, unchangedTouchedCount, failCount, projectStopwatch.ElapsedMilliseconds);
                                continue;
                            }

                            // Per-project scan marker to identify stale docs
                            var scanId = Guid.NewGuid().ToString("N");

                            int index = 0;
                            foreach (var file in files)
                            {
                                index++;
                                try
                                {
                                    var relativePath = GetRelativePath(projectFolder, file);
                                    var id = BuildSolrId(projectId, relativePath);
                                    var parentDir = Path.GetDirectoryName(file);
                                    var folderType = Path.GetFileName(parentDir ?? string.Empty) ?? string.Empty;

                                    // 1) Compute checksum first (cheap): decide whether we need heavy extraction
                                    var fileChecksum = ComputeSHA256(file);

                                    // 2) Ask Solr if the doc exists and what checksum it has
                                    var existingChecksum = await GetSolrFieldAsync(id, "checksum_s");

                                    if (!string.IsNullOrEmpty(existingChecksum) && string.Equals(existingChecksum, fileChecksum, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // Unchanged: do not re-extract content, just atomic-touch scanId_s
                                        _logger.LogInformation(
                                            "Project {projectNumber}: [{current}/{total}] Unchanged file, atomic touch: {file} (id: {id})",
                                            projectNumber, index, totalFiles, file, id);

                                        var touchOk = await AtomicTouchScanIdAsync(id, scanId);
                                        if (!touchOk)
                                        {
                                            failCount++;
                                            _logger.LogError("Project {projectNumber}: [{current}/{total}] Atomic touch failed for id: {id}",
                                                projectNumber, index, totalFiles, id);
                                        }
                                        else
                                        {
                                            unchangedTouchedCount++;
                                        }

                                        continue;
                                    }

                                    // 3) Extract content only when new/changed
                                    _logger.LogInformation(
                                        "Project {projectNumber}: [{current}/{total}] Extracting content from NEW/CHANGED file: {file} (id: {id})",
                                        projectNumber, index, totalFiles, file, id);

                                    var (ok, extractedText) = await ExtractFileContent(file);
                                    if (!ok)
                                    {
                                        failCount++;
                                        _logger.LogError("Project {projectNumber}: [{current}/{total}] Extraction failed. File skipped: {file}",
                                            projectNumber, index, totalFiles, file);
                                        continue;
                                    }

                                    var info = new FileInfo(file);
                                    var lastModifiedUtc = info.LastWriteTimeUtc;

                                    var solrDoc = new Dictionary<string, object?>
                                    {
                                        ["id"] = id,
                                        ["projectId"] = projectId,
                                        ["filename_s"] = Path.GetFileName(file),
                                        ["relativePath_s"] = relativePath.Replace('\\', '/'),
                                        ["sourceType_s"] = folderType,
                                        ["content_txt"] = extractedText ?? string.Empty,
                                        ["checksum_s"] = fileChecksum,
                                        ["lastModified_dt"] = lastModifiedUtc.ToString("o"),
                                        ["fileSize_l"] = info.Length,
                                        ["scanId_s"] = scanId
                                    };

                                    var addPayload = JsonSerializer.Serialize(new { add = new { doc = solrDoc } });

                                    _logger.LogInformation(
                                        "Project {projectNumber}: [{current}/{total}] Posting NEW/UPDATED doc to Solr: {id}",
                                        projectNumber, index, totalFiles, id);

                                    var addOk = await PostSolrJsonAsync($"{_solrURL}/update", addPayload, _logger);
                                    if (!addOk)
                                    {
                                        failCount++;
                                        _logger.LogError("Project {projectNumber}: [{current}/{total}] Solr add failed for id: {id}",
                                            projectNumber, index, totalFiles, id);
                                        continue;
                                    }

                                    newOrUpdatedCount++;
                                    _logger.LogInformation(
                                        "Project {projectNumber}: [{current}/{total}] Indexed NEW/UPDATED: {id}",
                                        projectNumber, index, totalFiles, id);
                                }
                                catch (Exception ex)
                                {
                                    failCount++;
                                    _logger.LogError(ex, "Project {projectNumber}: [{current}/{total}] Unexpected error for file: {file}",
                                        projectNumber, index, totalFiles, file);
                                }
                            }

                            // Delete anything not touched in this scan (stale docs) for this project
                            _logger.LogInformation("Project {projectNumber}: Deleting stale docs (NOT scanId: {scanId})", projectNumber, scanId);
                            var deleteQuery = $"projectId:{projectId} AND NOT scanId_s:{scanId}";
                            var delOk = await PostSolrJsonAsync($"{_solrURL}/update", JsonSerializer.Serialize(new { delete = new { query = deleteQuery } }), _logger);
                            if (!delOk)
                            {
                                _logger.LogError("Project {projectNumber}: deleteByQuery cleanup failed.", projectNumber);
                            }

                            // Commit once per project
                            _logger.LogInformation("Project {projectNumber}: Committing changes to Solr.", projectNumber);
                            var commitOk = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
                            if (!commitOk)
                            {
                                _logger.LogError("Project {projectNumber}: Commit failed.", projectNumber);
                            }

                            // Update DB timestamps/flags
                            var project = await _dbContext.Projects.FirstOrDefaultAsync(m => m.ProjId == projectId);
                            if (project != null)
                            {
                                project.IndexPdffiles = false;
                                project.SolrIndexDt = DateTime.Now;
                                project.SolrIndexPdfdt = DateTime.Now;
                                await _dbContext.SaveChangesAsync();
                                _logger.LogInformation("Project {projectNumber}: DB timestamps updated.", projectNumber);
                            }

                            projectStopwatch.Stop();
                            _logger.LogInformation(
                                "Project {projectNumber} SUMMARY: Total={total}, New/Updated={ok}, UnchangedTouched={touched}, Failed={failed}, Elapsed={ms}ms",
                                projectNumber, totalFiles, newOrUpdatedCount, unchangedTouchedCount, failCount, projectStopwatch.ElapsedMilliseconds);
                        }
                    }
                }

                var elapsed = DateTime.UtcNow - overallStart;
                _logger.LogInformation("Completed UploadAllFiles at: {timeLocal} ({timeUtc} UTC). Elapsed: {ms} ms",
                    DateTime.Now, DateTime.UtcNow, (long)elapsed.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while uploading files");
                throw;
            }
        }

        // -----------------------------
        // Solr helpers (select, atomic touch, delete)
        // -----------------------------

        /// <summary>
        /// Reads a single stored field from Solr for a given id. Returns empty string if not found.
        /// </summary>
        private async Task<string> GetSolrFieldAsync(string id, string fieldName)
        {
            try
            {
                var selectUrl = $"{_solrURL}/select?q=id:{Uri.EscapeDataString(id)}&fl={Uri.EscapeDataString(fieldName)}&rows=1&wt=json";
                var resp = await _http.GetAsync(selectUrl);
                if (!resp.IsSuccessStatusCode)
                {
                    _logger.LogWarning("Solr select failed for id={id}. Status={status}", id, resp.StatusCode);
                    return string.Empty;
                }

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("response", out var response)) return string.Empty;
                if (!response.TryGetProperty("docs", out var docs) || docs.GetArrayLength() == 0) return string.Empty;

                var first = docs[0];
                if (first.TryGetProperty(fieldName, out var field))
                {
                    // field may be array or string
                    if (field.ValueKind == JsonValueKind.Array && field.GetArrayLength() > 0)
                        return field[0].GetString() ?? string.Empty;
                    if (field.ValueKind == JsonValueKind.String)
                        return field.GetString() ?? string.Empty;
                }

                return string.Empty;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "GetSolrFieldAsync exception for id={id}, field={field}", id, fieldName);
                return string.Empty;
            }
        }

        /// <summary>
        /// Atomic update to set scanId_s for an existing doc without re-sending content.
        /// </summary>
        private async Task<bool> AtomicTouchScanIdAsync(string id, string scanId)
        {
            var payload = JsonSerializer.Serialize(new
            {
                add = new
                {
                    doc = new Dictionary<string, object?>
                    {
                        ["id"] = id,
                        ["scanId_s"] = new Dictionary<string, object?> { ["set"] = scanId }
                    }
                }
            });
            return await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
        }

        /// <summary>
        /// Delete all docs for a project (used when project has no files).
        /// </summary>
        private async Task<bool> DeleteAllDocsForProjectAsync(int projectId)
        {
            var deleteQuery = $"projectId:{projectId}";
            var deletePayload = JsonSerializer.Serialize(new { delete = new { query = deleteQuery } });
            return await PostSolrJsonAsync($"{_solrURL}/update", deletePayload, _logger);
        }

        /// <summary>
        /// Robust POST with retries and backoff to Solr JSON update/select endpoints.
        /// </summary>
        private async Task<bool> PostSolrJsonAsync(string url, string json, ILogger logger, int maxRetries = 3)
        {
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var content = new StringContent(json, Encoding.UTF8, "application/json");
                    var resp = await _http.PostAsync(url, content);

                    if (resp.IsSuccessStatusCode)
                        return true;

                    var reason = $"{(int)resp.StatusCode} {resp.ReasonPhrase}";
                    var body = await SafeReadAsync(resp);
                    logger.LogError("Solr call failed (attempt {attempt}/{max}) to {url}: {reason}. Body: {body}",
                        attempt, maxRetries, url, reason, body);
                }
                catch (TaskCanceledException tex)
                {
                    logger.LogError(tex, "Solr call timeout (attempt {attempt}/{max}) to {url}", attempt, maxRetries, url);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Solr call exception (attempt {attempt}/{max}) to {url}", attempt, maxRetries, url);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(300 * attempt * attempt));
            }
            return false;
        }

        private static async Task<string> SafeReadAsync(HttpResponseMessage resp)
        {
            try
            {
                return await resp.Content.ReadAsStringAsync();
            }
            catch
            {
                return "<unreadable>";
            }
        }

        // -----------------------------
        // File extraction helpers
        // -----------------------------

        /// <summary>
        /// Extracts textual content from a file. Caller decides when to call (after checksum comparison).
        /// </summary>
        private async Task<(bool success, string content)> ExtractFileContent(string filePath)
        {
            try
            {
                var fileName = Path.GetFileName(filePath);
                var guid = Guid.NewGuid().ToString();
                var tempFilePath = Path.Combine(Path.GetTempPath(), $"{guid}_{fileName}");

                if (!File.Exists(filePath))
                {
                    _logger.LogError("Source file not found: {filePath}", filePath);
                    return (false, string.Empty);
                }

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
                        extractedText = await File.ReadAllTextAsync(tempFilePath, Encoding.UTF8);
                        break;

                    default:
                        _logger.LogError("Unsupported file type: {fileName}", fileName);
                        return (false, string.Empty);
                }

                try
                {
                    if (File.Exists(tempFilePath))
                        File.Delete(tempFilePath);
                }
                catch (Exception delEx)
                {
                    _logger.LogWarning(delEx, "Failed to delete temp file: {tempFilePath}", tempFilePath);
                }

                return (true, extractedText ?? string.Empty);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error extracting file content: {filePath}", filePath);
                return (false, string.Empty);
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
                sb.AppendLine(page.Text ?? string.Empty);
            }
            return sb.ToString();
        }

        // -----------------------------
        // General helpers
        // -----------------------------

        private static string TrimTrailingSlash(string url)
        {
            if (string.IsNullOrWhiteSpace(url)) return url;
            return url.EndsWith("/") ? url.TrimEnd('/') : url;
        }

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
    }
}
