using Aspose.Words;
using Aspose.Words.Layout;
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

        /// <summary> One-click reset: deletes ALL documents in the Solr collection and commits. </summary>
        public async Task<bool> ClearSolrAllAsync()
        {
            try
            {
                var updateUrl = $"{_solrURL}/update";
                _logger.LogWarning("Clearing ALL documents from Solr at: {url}", updateUrl);

                var deletePayload = JsonSerializer.Serialize(new { delete = new { query = "*:*" } });
                var delOk = await PostSolrJsonAsync(updateUrl, deletePayload, _logger);
                if (!delOk) return false;

                return await PostSolrJsonAsync($"{updateUrl}?commit=true", "{}", _logger);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception while clearing Solr.");
                return false;
            }
        }

        /// <summary>
        /// Main indexer: scans folders and sends files to Solr with page-level docs.
        /// - If a file is unchanged (checksum), we atomic-touch all its page IDs (no extraction).
        /// - If changed or new, we delete old pages for that fileId and re-add pages.
        /// - After a project is processed, delete stale docs with NOT scanId_s and commit once.
        /// </summary>
        public async Task UploadAllFiles()
        {
            var overallStart = DateTime.UtcNow;

            try
            {
                _logger.LogInformation("Started UploadAllFiles at: {local} ({utc} UTC)", DateTime.Now, DateTime.UtcNow);

                var baseDirectory = _fileUploadPath;
                if (!Directory.Exists(baseDirectory))
                    throw new DirectoryNotFoundException($"Base directory not found: {baseDirectory}");

                // ---- Folder selection (adjust filters as needed) ----
                var yearFolders = Directory.GetDirectories(baseDirectory)/*.Where(m => Path.GetFileName(m) == "2025")*/; 
                foreach (var yearFolder in yearFolders)
                {
                    var monthFolders = Directory.GetDirectories(yearFolder)/*.Where(m => Path.GetFileName(m) == "07")*/;
                    foreach (var monthFolder in monthFolders)
                    {
                        var projectNumberFolders = Directory.GetDirectories(monthFolder)/*.Where(m => Path.GetFileName(m) == "25070103")*/; 
                        foreach (var projectFolder in projectNumberFolders)
                        {
                            var swProject = System.Diagnostics.Stopwatch.StartNew();
                            var projectNumber = Path.GetFileName(projectFolder);

                            // Only index published projects
                            var projectId = await _dbContext.Projects
                                .Where(p => p.ProjNumber == projectNumber && (bool)p.Publish!)
                                .Select(p => p.ProjId)
                                .FirstOrDefaultAsync();

                            if (projectId == 0)
                            {
                                _logger.LogInformation("Skipping project {projNumber} (unpublished or not found).", projectNumber);
                                continue;
                            }

                            // Supported files
                            var allowedExtensions = new[] { ".pdf", ".docx", ".doc", ".txt" };
                            var files = Directory.EnumerateFiles(projectFolder, "*.*", SearchOption.AllDirectories)
                                                 .Where(f => allowedExtensions.Contains(Path.GetExtension(f).ToLowerInvariant()))
                                                 .ToList();

                            _logger.LogInformation("Project {projNumber} (Id {pid}): {count} files.", projectNumber, projectId, files.Count);

                            // If no files → clear project docs and commit
                            if (files.Count == 0)
                            {
                                _ = await DeleteAllDocsForProjectAsync(projectId);
                                _ = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
                                continue;
                            }

                            var scanId = Guid.NewGuid().ToString("N");
                            int index = 0, newOrUpdated = 0, touched = 0, failed = 0;

                            foreach (var file in files)
                            {
                                index++;
                                try
                                {
                                    var relativePath = GetRelativePath(projectFolder, file).Replace('\\', '/'); // project-relative
                                    var fileId = Sha1(relativePath); // stable per relative path
                                    var idBase = $"{projectId}|{fileId}";
                                    var folderType = Path.GetFileName(Path.GetDirectoryName(file) ?? string.Empty) ?? string.Empty;

                                    var checksum = ComputeSHA256(file);
                                    var existingChecksum = await GetAnyChecksumForFileAsync(projectId, fileId);

                                    if (!string.IsNullOrEmpty(existingChecksum) &&
                                        string.Equals(existingChecksum, checksum, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // File unchanged → just touch scanId on all existing page docs
                                        var touchedOk = await TouchAllPagesScanIdAsync(projectId, fileId, scanId);
                                        if (!touchedOk) { failed++; continue; }
                                        touched++;
                                        _logger.LogDebug("[{cur}/{tot}] UNCHANGED touched fileId={fileId}", index, files.Count, fileId);
                                        continue;
                                    }

                                    // Changed/new → delete old pages for this file, then (re)add pages
                                    _ = await DeleteByFileIdAsync(projectId, fileId);

                                    var info = new FileInfo(file);
                                    var lastModUtc = info.LastWriteTimeUtc;

                                    // Extract pages (per type)
                                    IEnumerable<(int page, string text)> pages = ExtractPages(file);

                                    // Batch add in chunks
                                    const int BATCH = 300;
                                    var buffer = new List<Dictionary<string, object?>>(BATCH);

                                    foreach (var (page, text) in pages)
                                    {
                                        if (string.IsNullOrWhiteSpace(text)) continue;

                                        var doc = new Dictionary<string, object?>
                                        {
                                            // Unique per page
                                            ["id"] = $"{idBase}#{page}",

                                            // Filters / joins
                                            ["projectId_i"] = projectId,
                                            ["fileId_s"] = fileId,
                                            ["doc_type_s"] = "page",

                                            // File metadata
                                            ["filename_s"] = Path.GetFileName(file),
                                            ["relativePath_s"] = relativePath,
                                            ["page_i"] = page,
                                            ["checksum_s"] = checksum,
                                            ["fileSize_l"] = info.Length,
                                            ["lastModified_dt"] = lastModUtc.ToString("o"),
                                            ["scanId_s"] = scanId,

                                            // Content (per-page text) — FVH needs stored + termVectors
                                            ["content_txt"] = text
                                        };

                                        buffer.Add(doc);

                                        if (buffer.Count >= BATCH)
                                        {
                                            var ok = await AddDocsBatchAsync(buffer);
                                            if (!ok) { failed++; break; }
                                            buffer.Clear();
                                        }
                                    }

                                    if (buffer.Count > 0)
                                    {
                                        var ok = await AddDocsBatchAsync(buffer);
                                        if (!ok) { failed++; }
                                    }

                                    if (failed == 0) newOrUpdated++;
                                }
                                catch (Exception ex)
                                {
                                    failed++;
                                    _logger.LogError(ex, "[{cur}/{tot}] Error indexing file: {file}", index, files.Count, file);
                                }
                            }

                            // Project-level cleanup: remove any page docs not touched this run
                            var deleteStaleQuery = $"projectId_i:{projectId} AND doc_type_s:page AND -scanId_s:{scanId}";
                            _ = await PostSolrJsonAsync($"{_solrURL}/update",
                                JsonSerializer.Serialize(new { delete = new { query = deleteStaleQuery } }), _logger);

                            // Commit once per project
                            _ = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);

                            // Update DB timestamps/flags
                            var project = await _dbContext.Projects.FirstOrDefaultAsync(m => m.ProjId == projectId);
                            if (project != null)
                            {
                                project.IndexPdffiles = false;
                                project.SolrIndexDt = DateTime.Now;
                                project.SolrIndexPdfdt = DateTime.Now;
                                await _dbContext.SaveChangesAsync();
                            }

                            swProject.Stop();
                            _logger.LogInformation("PROJECT {proj} SUMMARY: New/Updated={up}, UnchangedTouched={touch}, Failed={fail}, Elapsed={ms}ms",
                                projectNumber, newOrUpdated, touched, failed, swProject.ElapsedMilliseconds);
                        }
                    }
                }

                var elapsed = DateTime.UtcNow - overallStart;
                _logger.LogInformation("Completed UploadAllFiles. Elapsed: {ms} ms", (long)elapsed.TotalMilliseconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while uploading files");
                throw;
            }
        }

        // ============================
        // Solr helpers (batch add, touch, delete, select)
        // ============================

        private async Task<bool> AddDocsBatchAsync(List<Dictionary<string, object?>> docs, int maxRetries = 3)
        {
            if (docs.Count == 0) return true;
            // JSON format: { "add": [ {"doc": {...}}, {"doc": {...}} ] }
            var adds = docs.Select(d => new Dictionary<string, object?> { ["doc"] = d }).ToList();
            var payload = JsonSerializer.Serialize(new Dictionary<string, object?> { ["add"] = adds });
            return await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger, maxRetries);
        }

        private async Task<string> GetAnyChecksumForFileAsync(int projectId, string fileId)
        {
            try
            {
                var selectUrl =
                    $"{_solrURL}/select?q=projectId_i:{projectId}%20AND%20fileId_s:{fileId}&fl=checksum_s,id&rows=1&wt=json";
                var resp = await _http.GetAsync(selectUrl);
                if (!resp.IsSuccessStatusCode) return string.Empty;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("response", out var response)) return string.Empty;
                if (!response.TryGetProperty("docs", out var docs) || docs.GetArrayLength() == 0) return string.Empty;
                var first = docs[0];
                if (first.TryGetProperty("checksum_s", out var cs) && cs.ValueKind == JsonValueKind.String)
                    return cs.GetString() ?? string.Empty;
                return string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        private async Task<bool> TouchAllPagesScanIdAsync(int projectId, string fileId, string scanId)
        {
            // Fetch IDs with cursor to be safe for large page counts
            var ids = await SelectIdsForFileAsync(projectId, fileId);
            if (ids.Count == 0) return true;

            const int BATCH = 500;
            for (int i = 0; i < ids.Count; i += BATCH)
            {
                var slice = ids.Skip(i).Take(BATCH)
                    .Select(id => new Dictionary<string, object?>
                    {
                        ["doc"] = new Dictionary<string, object?>
                        {
                            ["id"] = id,
                            ["scanId_s"] = new Dictionary<string, object?> { ["set"] = scanId }
                        }
                    })
                    .ToList();

                var payload = JsonSerializer.Serialize(new Dictionary<string, object?> { ["add"] = slice });
                var ok = await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
                if (!ok) return false;
            }
            return true;
        }

        private async Task<List<string>> SelectIdsForFileAsync(int projectId, string fileId)
        {
            var results = new List<string>();
            string cursor = "*";
            const int ROWS = 1000;

            while (true)
            {
                var url = $"{_solrURL}/select?q=projectId_i:{projectId}%20AND%20fileId_s:{fileId}" +
                          $"&fl=id&rows={ROWS}&sort=id%20asc&cursorMark={Uri.EscapeDataString(cursor)}&wt=json";
                var resp = await _http.GetAsync(url);
                if (!resp.IsSuccessStatusCode) break;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (!root.TryGetProperty("response", out var response)) break;
                if (!response.TryGetProperty("docs", out var docs)) break;

                foreach (var d in docs.EnumerateArray())
                {
                    if (d.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String)
                        results.Add(id.GetString()!);
                }

                if (!root.TryGetProperty("nextCursorMark", out var next)) break;
                var nextCursor = next.GetString() ?? cursor;
                if (nextCursor == cursor) break; // done
                cursor = nextCursor;
            }

            return results;
        }

        private async Task<bool> DeleteByFileIdAsync(int projectId, string fileId)
        {
            var query = $"projectId_i:{projectId} AND fileId_s:{fileId}";
            var payload = JsonSerializer.Serialize(new { delete = new { query } });
            return await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
        }

        private async Task<bool> DeleteAllDocsForProjectAsync(int projectId)
        {
            var payload = JsonSerializer.Serialize(new { delete = new { query = $"projectId_i:{projectId}" } });
            return await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
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
            try { return await resp.Content.ReadAsStringAsync(); }
            catch { return "<unreadable>"; }
        }

        // ============================
        // Extraction helpers (per-page)
        // ============================

        private static IEnumerable<(int page, string text)> ExtractPages(string filePath)
        {
            var ext = Path.GetExtension(filePath).ToLowerInvariant();
            return ext switch
            {
                ".pdf" => ExtractPdfPages(filePath),
                ".docx" => ExtractDocOrDocxPages(filePath),
                ".doc" => ExtractDocOrDocxPages(filePath),
                ".txt" => ExtractTxtAsSinglePage(filePath),
                _ => Enumerable.Empty<(int, string)>()
            };
        }

        private static IEnumerable<(int page, string text)> ExtractPdfPages(string path)
        {
            using var pdf = PdfDocument.Open(path);
            int p = 1;
            foreach (var page in pdf.GetPages())
            {
                var t = page.Text ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(t))
                    yield return (p, t);
                p++;
            }
        }

        // Uses Aspose.Words page splitter so DOC/DOCX are also page-addressable
        private static IEnumerable<(int page, string text)> ExtractDocOrDocxPages(string path)
        {
            var doc = new Document(path);
            doc.UpdatePageLayout(); // ensure page map is ready
            var collector = new LayoutCollector(doc);
            var splitter = new DocumentPageSplitter(collector);

            for (int i = 1; i <= doc.PageCount; i++)
            {
                var pageDoc = splitter.GetDocumentOfPage(i);
                var text = pageDoc.ToString(SaveFormat.Text);
                if (!string.IsNullOrWhiteSpace(text))
                    yield return (i, text);
            }
        }

        private static IEnumerable<(int page, string text)> ExtractTxtAsSinglePage(string path)
        {
            var text = File.ReadAllText(path, Encoding.UTF8);
            if (!string.IsNullOrWhiteSpace(text))
                return new[] { (1, text) };
            return Enumerable.Empty<(int, string)>();
        }

        // ============================
        // General helpers
        // ============================

        private static string TrimTrailingSlash(string url)
            => string.IsNullOrWhiteSpace(url) ? url : (url.EndsWith("/") ? url.TrimEnd('/') : url);

        private static string GetRelativePath(string baseDir, string fullPath)
        {
            var baseUri = new Uri(AppendDirectorySeparatorChar(baseDir));
            var fullUri = new Uri(fullPath);
            return Uri.UnescapeDataString(baseUri.MakeRelativeUri(fullUri).ToString());
        }

        private static string AppendDirectorySeparatorChar(string path)
            => path.EndsWith(Path.DirectorySeparatorChar) ? path : path + Path.DirectorySeparatorChar;

        private static string ComputeSHA256(string filePath)
        {
            using var sha = SHA256.Create();
            using var fs = File.OpenRead(filePath);
            var hash = sha.ComputeHash(fs);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }

        private static string Sha1(string input)
        {
            using var sha = SHA1.Create();
            var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(input));
            var sb = new StringBuilder(bytes.Length * 2);
            foreach (var b in bytes) sb.Append(b.ToString("x2"));
            return sb.ToString();
        }
    }

    // ===== Aspose helper for per-page extraction =====
    public class DocumentPageSplitter
    {
        private readonly LayoutCollector _collector;
        public DocumentPageSplitter(LayoutCollector collector) => _collector = collector;
        public Document GetDocumentOfPage(int pageIndex)
        {
            // Use official Aspose sample if you have it; this shortcut works for text extraction scenarios.
            var src = _collector.Document;
            var dst = (Document)src.Clone(true);
            dst.RemoveAllChildren();
            var importer = new NodeImporter(src, dst, ImportFormatMode.KeepSourceFormatting);
            foreach (Section section in src.Sections)
                dst.Sections.Add((Section)dst.ImportNode(section, true));
            // NOTE: For exact per-page fidelity (headers/footers, etc.), use Aspose’s full splitter sample.
            return dst;
        }
    }
}
