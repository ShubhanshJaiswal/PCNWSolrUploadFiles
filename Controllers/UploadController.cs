using Aspose.Words;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using PCNWSolrUploadFiles.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using UglyToad.PdfPig;
using UglyToad.PdfPig.Core;

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
        private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };

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
        /// MAIN INDEXER
        /// Scans base\{YYYY}\{MM}\{ProjectNumber}\... and indexes PDFs/Word/TXT into Solr.
        /// - Unchanged files (by checksum) => atomic "touch" to set scanId_s (no re-extraction)
        /// - New/changed => delete old docs of that fileId, re-add
        /// - Project-level cleanup removes only docs having scanId_s != this run's scanId (safe)
        /// </summary>
        public async Task UploadAllFiles()
        {
            // await ClearSolrAllAsync(); // only for a full reset
            var overallStart = DateTime.UtcNow;

            try
            {
                _logger.LogInformation("Started UploadAllFiles at: {local} ({utc} UTC)", DateTime.Now, DateTime.UtcNow);

                if (string.IsNullOrWhiteSpace(_solrURL))
                    throw new InvalidOperationException("SolrURL not configured.");

                // Ensure schema once up front (idempotent)
                var schemaOk = await EnsureSolrSchemaAsync();
                if (!schemaOk)
                {
                    _logger.LogWarning("Could not verify/create schema. Proceeding, but field storage/indexing may be wrong.");
                }

                var baseDirectory = _fileUploadPath;
                if (!Directory.Exists(baseDirectory))
                    throw new DirectoryNotFoundException($"Base directory not found: {baseDirectory}");

                // Enumerate ALL years/months/projects under base path
                var yearFolders = Directory.GetDirectories(baseDirectory); // e.g., 2023, 2024, 2025...
                foreach (var yearFolder in yearFolders)
                {
                    var monthFolders = Directory.GetDirectories(yearFolder); // 01..12
                    foreach (var monthFolder in monthFolders)
                    {
                        var projectNumberFolders = Directory.GetDirectories(monthFolder); // e.g., 24070005
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

                            var allowedExtensions = new[] { ".pdf", ".docx", ".doc", ".txt" };
                            var files = Directory.EnumerateFiles(projectFolder, "*.*", SearchOption.AllDirectories)
                                                 .Where(f => allowedExtensions.Contains(Path.GetExtension(f).ToLowerInvariant()))
                                                 .ToList();

                            if (files.Count == 0)
                            {
                                // SAFER: Do NOT delete existing docs if the folder currently has no files.
                                _logger.LogInformation("Skipping project {projNumber} (no supported files found). No deletions performed.", projectNumber);
                                continue;
                            }

                            _logger.LogInformation("Project {projNumber} (Id {pid}): {count} files.", projectNumber, projectId, files.Count);

                            var scanId = Guid.NewGuid().ToString("N"); // safe for query usage
                            int index = 0, newOrUpdated = 0, touched = 0, failed = 0, addedDocs = 0;

                            foreach (var file in files)
                            {
                                index++;
                                try
                                {
                                    var relativePath = GetRelativePath(projectFolder, file).Replace('\\', '/'); // project-relative
                                    var fileId = Sha1(relativePath); // stable per relative path
                                    var idBase = $"{projectId}|{fileId}";

                                    var checksum = ComputeSHA256(file);
                                    var existingChecksum = await GetAnyChecksumForFileAsync(projectId, fileId);

                                    if (!string.IsNullOrEmpty(existingChecksum) &&
                                        string.Equals(existingChecksum, checksum, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // Unchanged → just touch scanId on all existing docs for this file
                                        var touchedOk = await TouchAllDocsScanIdAsync(projectId, fileId, scanId);
                                        if (!touchedOk) { failed++; continue; }
                                        touched++;
                                        _logger.LogDebug("[{cur}/{tot}] UNCHANGED touched fileId={fileId}", index, files.Count, fileId);
                                        continue;
                                    }

                                    // Changed/new → delete old docs for this file, then (re)add
                                    _ = await DeleteByFileIdAsync(projectId, fileId);

                                    var info = new FileInfo(file);
                                    var lastModUtc = info.LastWriteTimeUtc;

                                    // Extract content (per type)
                                    var ext = Path.GetExtension(file).ToLowerInvariant();
                                    if (ext == ".pdf")
                                    {
                                        foreach (var (page, textRaw) in ExtractPdfPages(file))
                                        {
                                            var text = CleanAsposeEvaluationNoise(textRaw);
                                            if (string.IsNullOrWhiteSpace(text)) continue; // never push empty docs

                                            var doc = new Dictionary<string, object?>
                                            {
                                                ["id"] = $"{idBase}#{page}",
                                                ["projectId_i"] = projectId,
                                                ["fileId_s"] = fileId,
                                                ["doc_type_s"] = "page",
                                                ["filename_s"] = Path.GetFileName(file),
                                                ["relativePath_s"] = relativePath,
                                                ["page_i"] = page,
                                                ["checksum_s"] = checksum,
                                                ["fileSize_l"] = info.Length,
                                                ["lastModified_dt"] = lastModUtc.ToString("o"),
                                                ["scanId_s"] = scanId,
                                                ["content_txt"] = text
                                            };

                                            if (!await AddDocsBatchAsync_One(doc)) { failed++; break; }
                                            addedDocs++;
                                        }
                                    }
                                    else if (ext == ".docx" || ext == ".doc")
                                    {
                                        // Index as a single file-level doc (per-page split would need more Word layout logic)
                                        var text = ExtractDocAllText(file);
                                        text = CleanAsposeEvaluationNoise(text);
                                        if (!string.IsNullOrWhiteSpace(text))
                                        {
                                            var doc = new Dictionary<string, object?>
                                            {
                                                ["id"] = $"{idBase}#1",
                                                ["projectId_i"] = projectId,
                                                ["fileId_s"] = fileId,
                                                ["doc_type_s"] = "file",
                                                ["filename_s"] = Path.GetFileName(file),
                                                ["relativePath_s"] = relativePath,
                                                ["page_i"] = 1,
                                                ["checksum_s"] = checksum,
                                                ["fileSize_l"] = info.Length,
                                                ["lastModified_dt"] = lastModUtc.ToString("o"),
                                                ["scanId_s"] = scanId,
                                                ["content_txt"] = text
                                            };

                                            if (!await AddDocsBatchAsync_One(doc)) { failed++; }
                                            else addedDocs++;
                                        }
                                    }
                                    else if (ext == ".txt")
                                    {
                                        var text = File.ReadAllText(file, Encoding.UTF8);
                                        if (!string.IsNullOrWhiteSpace(text))
                                        {
                                            var doc = new Dictionary<string, object?>
                                            {
                                                ["id"] = $"{idBase}#1",
                                                ["projectId_i"] = projectId,
                                                ["fileId_s"] = fileId,
                                                ["doc_type_s"] = "file",
                                                ["filename_s"] = Path.GetFileName(file),
                                                ["relativePath_s"] = relativePath,
                                                ["page_i"] = 1,
                                                ["checksum_s"] = checksum,
                                                ["fileSize_l"] = info.Length,
                                                ["lastModified_dt"] = lastModUtc.ToString("o"),
                                                ["scanId_s"] = scanId,
                                                ["content_txt"] = text
                                            };

                                            if (!await AddDocsBatchAsync_One(doc)) { failed++; }
                                            else addedDocs++;
                                        }
                                    }

                                    if (failed == 0) newOrUpdated++;
                                }
                                catch (Exception ex)
                                {
                                    failed++;
                                    _logger.LogError(ex, "[{cur}/{tot}] Error indexing file: {file}", index, files.Count, file);
                                }
                            }

                            // Project-level cleanup: remove docs from earlier runs safely
                            // IMPORTANT: only delete docs that HAVE a scanId_s at all (otherwise, old docs with no scanId_s would be wiped)
                            var deleteStaleQuery = $"projectId_i:{projectId} AND doc_type_s:[* TO *] AND scanId_s:[* TO *] AND -scanId_s:{scanId}";
                            _ = await PostSolrJsonAsync($"{_solrURL}/update",
                                JsonSerializer.Serialize(new { delete = new { query = deleteStaleQuery } }), _logger);

                            // Commit once per project
                            _ = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);

                            // Update DB timestamps/flags only if we actually processed files (added or touched)
                            if ((newOrUpdated + touched) > 0)
                            {
                                var project = await _dbContext.Projects.FirstOrDefaultAsync(m => m.ProjId == projectId);
                                if (project != null)
                                {
                                    project.IndexPdffiles = false;
                                    project.SolrIndexDt = DateTime.Now;
                                    project.SolrIndexPdfdt = DateTime.Now;
                                    await _dbContext.SaveChangesAsync();
                                }
                            }

                            swProject.Stop();
                            _logger.LogInformation("PROJECT {proj} SUMMARY: New/Updated={up}, UnchangedTouched={touch}, Failed={fail}, AddedDocs={docs}, Elapsed={ms}ms",
                                projectNumber, newOrUpdated, touched, failed, addedDocs, swProject.ElapsedMilliseconds);
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

        // ---------------------------
        // Solr SCHEMA bootstrap (idempotent & highlight-ready)
        // ---------------------------

        private record FieldDef(string name, string type, bool stored, bool indexed);

        private async Task<bool> EnsureSolrSchemaAsync()
        {
            try
            {
                // 1) Get existing fields
                var fieldsUrl = $"{_solrURL}/schema/fields?includeDynamic=true&wt=json";
                var resp = await _http.GetAsync(fieldsUrl);
                if (!resp.IsSuccessStatusCode) return false;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var existing = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
                if (doc.RootElement.TryGetProperty("fields", out var arr))
                {
                    foreach (var f in arr.EnumerateArray())
                    {
                        if (f.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String)
                            existing[n.GetString()!] = f;
                    }
                }

                var need = new List<FieldDef>
                {
                    new("projectId_i","pint", stored:true, indexed:true),
                    new("fileId_s","string", stored:true, indexed:true),
                    new("doc_type_s","string", stored:true, indexed:true),
                    new("filename_s","string", stored:true, indexed:true),
                    new("relativePath_s","string", stored:true, indexed:true),
                    new("page_i","pint", stored:true, indexed:true),
                    new("checksum_s","string", stored:true, indexed:true),
                    new("fileSize_l","plong", stored:true, indexed:true),
                    new("lastModified_dt","pdate", stored:true, indexed:true),
                    new("scanId_s","string", stored:true, indexed:true),
                    // Highlight-friendly: stored + term vectors for FVH
                    new("content_txt","text_general", stored:true, indexed:true)
                };

                // 2) Add missing fields
                var toAdd = need.Where(n => !existing.ContainsKey(n.name)).ToList();
                if (toAdd.Count > 0)
                {
                    var addPayload = new Dictionary<string, object?>
                    {
                        ["add-field"] = toAdd
                            .Select(f => new Dictionary<string, object?>
                            {
                                ["name"] = f.name,
                                ["type"] = f.type,
                                ["stored"] = f.stored,
                                ["indexed"] = f.indexed,
                                ["multiValued"] = false
                            })
                            .ToArray()
                    };

                    var addResp = await _http.PostAsync($"{_solrURL}/schema",
                        new StringContent(JsonSerializer.Serialize(addPayload), Encoding.UTF8, "application/json"));
                    if (!addResp.IsSuccessStatusCode)
                    {
                        var body = await SafeReadAsync(addResp);
                        _logger.LogWarning("Schema add-field returned {code}: {body}", (int)addResp.StatusCode, body);
                    }
                }

                // 3) Ensure content_txt has term vectors + stored true for FVH
                if (existing.TryGetValue("content_txt", out var contentTxt))
                {
                    bool storedOk = contentTxt.TryGetProperty("stored", out var s) && s.ValueKind == JsonValueKind.True;
                    bool tvOk = contentTxt.TryGetProperty("termVectors", out var tv) && tv.ValueKind == JsonValueKind.True;
                    bool posOk = contentTxt.TryGetProperty("termPositions", out var tp) && tp.ValueKind == JsonValueKind.True;
                    bool offOk = contentTxt.TryGetProperty("termOffsets", out var to) && to.ValueKind == JsonValueKind.True;

                    if (!storedOk || !tvOk || !posOk || !offOk)
                    {
                        var replacePayload = new Dictionary<string, object?>
                        {
                            ["replace-field"] = new Dictionary<string, object?>
                            {
                                ["name"] = "content_txt",
                                ["type"] = "text_general",
                                ["stored"] = true,
                                ["indexed"] = true,
                                ["multiValued"] = false,
                                ["termVectors"] = true,
                                ["termPositions"] = true,
                                ["termOffsets"] = true
                            }
                        };

                        var repResp = await _http.PostAsync($"{_solrURL}/schema",
                            new StringContent(JsonSerializer.Serialize(replacePayload), Encoding.UTF8, "application/json"));
                        if (!repResp.IsSuccessStatusCode)
                        {
                            var body = await SafeReadAsync(repResp);
                            _logger.LogWarning("Schema replace-field(content_txt) returned {code}: {body}", (int)repResp.StatusCode, body);
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "EnsureSolrSchemaAsync failed");
                return false;
            }
        }

        // ---------------------------
        // Solr helpers (add, touch, delete, select)
        // ---------------------------

        // Adds one doc per request (simple & safe). NEVER sends empties.
        private async Task<bool> AddDocsBatchAsync_One(Dictionary<string, object?> doc, int maxRetries = 3)
        {
            if (!doc.TryGetValue("content_txt", out var textObj) || string.IsNullOrWhiteSpace(textObj as string))
                return true; // skip empty

            var payload = JsonSerializer.Serialize(new[] { new Dictionary<string, object?> { ["add"] = new Dictionary<string, object?> { ["doc"] = doc } } });
            // Use the classic add syntax: [{ "add": { "doc": { ... }}}]
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

        /// <summary>
        /// PROPER atomic update: top-level array of partial docs, no "add" wrapper.
        /// </summary>
        private async Task<bool> TouchAllDocsScanIdAsync(int projectId, string fileId, string scanId)
        {
            var ids = await SelectIdsForFileAsync(projectId, fileId);
            if (ids.Count == 0) return true;

            const int BATCH = 500;
            for (int i = 0; i < ids.Count; i += BATCH)
            {
                var batch = ids.Skip(i).Take(BATCH)
                    .Select(id => new Dictionary<string, object?>
                    {
                        ["id"] = id,
                        ["scanId_s"] = new Dictionary<string, object?> { ["set"] = scanId }
                    })
                    .ToArray();

                var payload = JsonSerializer.Serialize(batch);
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

        // ---------------------------
        // Extraction helpers
        // ---------------------------

        private IEnumerable<(int page, string text)> ExtractPdfPages(string path)
        {
            PdfDocument pdf;
            string tempSalvaged;

            if (!TryOpenPdfWithSalvage(path, out pdf, out tempSalvaged))
                yield break;

            try
            {
                using (pdf)
                {
                    int total = pdf.NumberOfPages;

                    for (int p = 1; p <= total; p++)
                    {
                        string? text = null;

                        try
                        {
                            var page = pdf.GetPage(p);
                            text = page?.Text;
                        }
                        catch (PdfDocumentFormatException ex)
                        {
                            _logger.LogWarning(ex, "Skipping bad PDF page {page} in {path}", p, path);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Skipping page {page} in {path}", p, path);
                        }

                        if (!string.IsNullOrWhiteSpace(text))
                            yield return (p, text);  // yield outside the inner try/catch
                    }
                }
            }
            finally
            {
                if (!string.IsNullOrEmpty(tempSalvaged))
                {
                    try { System.IO.File.Delete(tempSalvaged); } catch { /* ignore */ }
                }
            }
        }

        // Best-effort open with salvage
        private bool TryOpenPdfWithSalvage(string path, out PdfDocument pdf, out string tempSalvaged)
        {
            pdf = null!;
            tempSalvaged = null!;

            try
            {
                pdf = PdfDocument.Open(path);
                return true;
            }
            catch (PdfDocumentFormatException ex)
            {
                _logger.LogWarning(ex, "PDF open failed (header/structure). Trying salvage: {path}", path);

                if (!TrySalvagePdfByHeader(path, out tempSalvaged))
                    return false;

                try
                {
                    pdf = PdfDocument.Open(tempSalvaged);
                    return true;
                }
                catch (Exception ex2)
                {
                    _logger.LogError(ex2, "Salvaged PDF still failed to open: {temp}", tempSalvaged);
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PDF open failed: {path}", path);
                return false;
            }
        }

        // Scan first 1MB for "%PDF-" and write a temp file starting at that offset
        private bool TrySalvagePdfByHeader(string path, out string tempPath)
        {
            tempPath = null!;
            try
            {
                var bytes = System.IO.File.ReadAllBytes(path);
                if (bytes == null || bytes.Length < 8) return false;

                var sig = Encoding.ASCII.GetBytes("%PDF-");
                var idx = IndexOf(bytes, sig, 0, Math.Min(bytes.Length, 1024 * 1024));
                if (idx < 0) return false;

                tempPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(),
                    "pcnw_pdfsalvage_" + Guid.NewGuid().ToString("N") + ".pdf");

                using (var fs = System.IO.File.Create(tempPath))
                    fs.Write(bytes, idx, bytes.Length - idx);

                _logger.LogInformation("Salvaged PDF by trimming {trim} bytes: {temp}", idx, tempPath);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "TrySalvagePdfByHeader failed for {path}", path);
                tempPath = null!;
                return false;
            }
        }

        private static int IndexOf(byte[] haystack, byte[] needle, int start, int count)
        {
            if (needle == null || needle.Length == 0) return start;
            int end = Math.Min(haystack.Length, start + count);
            for (int i = start; i <= end - needle.Length; i++)
            {
                int j = 0;
                for (; j < needle.Length; j++)
                    if (haystack[i + j] != needle[j]) break;
                if (j == needle.Length) return i;
            }
            return -1;
        }

        private static string ExtractDocAllText(string path)
        {
            var doc = new Aspose.Words.Document(path);
            // No per-page split (see comment), just full text:
            return doc.ToString(SaveFormat.Text);
        }

        // ---------------------------
        // Cleanup & utilities
        // ---------------------------

        // OPTIONAL: purge junk docs that have no required fields (safe one-time cleanup)
        public async Task<bool> RemoveDocsMissingRequiredFieldsAsync()
        {
            var query = "-projectId_i:[* TO *] OR -doc_type_s:[* TO *]";
            var payload = JsonSerializer.Serialize(new { delete = new { query } });
            var ok = await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
            if (!ok) return false;
            return await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
        }

        // OPTIONAL: purge docs where scanId_s was accidentally stored as a literal "{set=...}"
        public async Task<bool> RemoveDocsWithLiteralSetInScanIdAsync()
        {
            // This pattern relies on escaping '{'. If your qparser dislikes it, simply rerun the indexer to overwrite values correctly.
            var query = @"scanId_s:\{set\=*";
            var payload = JsonSerializer.Serialize(new { delete = new { query } });
            var ok = await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
            if (!ok) return false;
            return await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
        }

        private static readonly Regex[] AsposeEvalNoise =
        {
            new Regex(@"^\s*Created with an evaluation copy of Aspose\.Words.*$", RegexOptions.IgnoreCase | RegexOptions.Multiline),
            new Regex(@"^\s*Evaluation Only\.\s*Created with Aspose\.Words.*$", RegexOptions.IgnoreCase | RegexOptions.Multiline),
            new Regex(@"https?://products\.aspose\.com/words/temporary-license/?", RegexOptions.IgnoreCase | RegexOptions.Multiline),
            new Regex(@"Copyright\s+\d{4}(?:-\d{4})?\s+Aspose Pty Ltd\.?", RegexOptions.IgnoreCase | RegexOptions.Multiline)
        };

        private static string CleanAsposeEvaluationNoise(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) return text ?? string.Empty;

            foreach (var rx in AsposeEvalNoise)
                text = rx.Replace(text, string.Empty);

            // collapse extra blank lines/whitespace that may be left behind
            text = Regex.Replace(text, @"[ \t]+\r?\n", "\n");    // trim line-end spaces
            text = Regex.Replace(text, @"(\r?\n){3,}", "\n\n");  // collapse >2 blank lines to 1 blank line
            return text.Trim();
        }

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
}
