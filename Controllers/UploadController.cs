using Aspose.Words;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
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
        private readonly string _solrURL; // e.g., http://host:8983/solr/pcnw_project
        private readonly ILogger<UploadController> _logger;

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

        public async Task UploadAllFiles()
        {
            //await ClearSolrAllAsync();
            // Anywhere in your service:
            //var ok = await SolrRootFieldFixer.EnsureRootNoDocValuesAsync(_solrURL, _logger);

            var overallStart = DateTime.UtcNow;

            try
            {
                _logger.LogInformation("Started UploadAllFiles at: {local} ({utc} UTC)", DateTime.Now, DateTime.UtcNow);
                if (string.IsNullOrWhiteSpace(_solrURL))
                    throw new InvalidOperationException("SolrURL not configured.");

                var schemaOk = await EnsureSolrSchemaAsync();
                if (!schemaOk) _logger.LogWarning("Could not verify/create schema. Proceeding anyway.");

                var baseDir = _fileUploadPath;
                if (!Directory.Exists(baseDir))
                    throw new DirectoryNotFoundException($"Base directory not found: {baseDir}");

                var yearFolders = Directory.GetDirectories(baseDir)/*.Where(m=>Path.GetFileName(m)=="2024")*/;
                foreach (var yearFolder in yearFolders)
                {
                    var monthFolders = Directory.GetDirectories(yearFolder);
                    foreach (var monthFolder in monthFolders)
                    {
                        var projectFolders = Directory.GetDirectories(monthFolder);
                        foreach (var projectFolder in projectFolders)
                        {
                            var sw = System.Diagnostics.Stopwatch.StartNew();
                            var projectNumber = Path.GetFileName(projectFolder);

                            var projectId = await _dbContext.Projects
                                .Where(p => p.ProjNumber == projectNumber && (bool)p.Publish!)
                                .Select(p => p.ProjId)
                                .FirstOrDefaultAsync();

                            if (projectId == 0)
                            {
                                _logger.LogInformation("Skipping project {projNumber} (unpublished or not found).", projectNumber);
                                continue;
                            }

                            var allowedExt = new[] { ".pdf", ".docx", ".doc", ".txt" };
                            var files = Directory.EnumerateFiles(projectFolder, "*.*", SearchOption.AllDirectories)
                                                 .Where(f => allowedExt.Contains(Path.GetExtension(f).ToLowerInvariant()))
                                                 .ToList();

                            if (files.Count == 0)
                            {
                                _logger.LogInformation("Skipping project {projNumber} (no supported files found). No deletions performed.", projectNumber);
                                continue;
                            }

                            _logger.LogInformation("Project {projNumber} (Id {pid}): {count} files.", projectNumber, projectId, files.Count);

                            var scanId = Guid.NewGuid().ToString("N");
                            int index = 0, newOrUpdated = 0, touched = 0, failed = 0, addedDocs = 0;

                            // Track the fileIds we successfully processed so cleanup only affects those
                            var processedFileIds = new HashSet<string>(StringComparer.Ordinal);

                            foreach (var file in files)
                            {
                                index++;
                                bool fileSucceeded = true;

                                try
                                {
                                    var relativePath = GetRelativePath(projectFolder, file).Replace('\\', '/');
                                    var fileId = Sha1(relativePath);
                                    var idBase = $"{projectId}|{fileId}";

                                    var checksum = ComputeSHA256(file);
                                    var existingChecksum = await GetAnyChecksumForFileAsync(projectId, fileId);

                                    if (!string.IsNullOrEmpty(existingChecksum) &&
                                        string.Equals(existingChecksum, checksum, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // UNCHANGED: "touch" scanId_s (safe; no _root_ used)
                                        var touchedOk = await TouchAllDocsScanIdAsync(projectId, fileId, scanId);
                                        if (!touchedOk)
                                        {
                                            // If we couldn't touch (e.g., child docs existed), log but don't fail hard.
                                            _logger.LogInformation("[{cur}/{tot}] Touch partial/no-op for fileId={fileId}", index, files.Count, fileId);
                                        }
                                        else
                                        {
                                            touched++;
                                            _logger.LogDebug("[{cur}/{tot}] UNCHANGED touched fileId={fileId}", index, files.Count, fileId);
                                        }

                                        processedFileIds.Add(fileId);
                                        continue;
                                    }

                                    // CHANGED/NEW: delete then re-add fresh docs
                                    _ = await DeleteByFileIdAsync(projectId, fileId);

                                    var info = new FileInfo(file);
                                    var lastModUtc = info.LastWriteTimeUtc;
                                    var ext = Path.GetExtension(file).ToLowerInvariant();

                                    if (ext == ".pdf")
                                    {
                                        foreach (var (page, textRaw) in ExtractPdfPages(file))
                                        {
                                            var text = CleanAsposeEvaluationNoise(textRaw);
                                            text = SanitizeForSolrText(text);
                                            if (string.IsNullOrWhiteSpace(text)) continue;

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

                                            if (!await AddDocAsync(doc)) { fileSucceeded = false; failed++; break; }
                                            addedDocs++;
                                        }
                                    }
                                    else if (ext == ".docx" || ext == ".doc")
                                    {
                                        var text = ExtractDocAllText(file);
                                        text = CleanAsposeEvaluationNoise(text);
                                        text = SanitizeForSolrText(text);
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

                                            if (!await AddDocAsync(doc)) { fileSucceeded = false; failed++; }
                                            else addedDocs++;
                                        }
                                    }
                                    else if (ext == ".txt")
                                    {
                                        var text = File.ReadAllText(file, Encoding.UTF8);
                                        text = SanitizeForSolrText(text);
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

                                            if (!await AddDocAsync(doc)) { fileSucceeded = false; failed++; }
                                            else addedDocs++;
                                        }
                                    }

                                    if (fileSucceeded) { newOrUpdated++; processedFileIds.Add(fileId); }
                                }
                                catch (Exception ex)
                                {
                                    failed++;
                                    _logger.LogError(ex, "[{cur}/{tot}] Error indexing file: {file}", index, files.Count, file);
                                }
                            }

                            // Cleanup: delete stale docs ONLY for the files we successfully processed this run
                            await DeleteStaleForFilesAsync(projectId, scanId, processedFileIds);

                            // Commit once per project
                            _ = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);

                            if (processedFileIds.Count > 0)
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

                            sw.Stop();
                            _logger.LogInformation("PROJECT {proj} SUMMARY: New/Updated={up}, UnchangedTouched={touch}, Failed={fail}, AddedDocs={docs}, Elapsed={ms}ms",
                                projectNumber, newOrUpdated, touched, failed, addedDocs, sw.ElapsedMilliseconds);
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
        // Schema bootstrap (idempotent; never mutate _root_)
        // ---------------------------

        private record FieldDef(string name, string type, bool stored, bool indexed);

        private async Task<bool> EnsureSolrSchemaAsync()
        {
            try
            {
                var fieldsUrl = $"{_solrURL}/schema/fields?includeDynamic=true&wt=json";
                var resp = await _http.GetAsync(fieldsUrl);
                if (!resp.IsSuccessStatusCode) return false;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (doc.RootElement.TryGetProperty("fields", out var arr))
                {
                    foreach (var f in arr.EnumerateArray())
                    {
                        if (f.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String)
                            existing.Add(n.GetString()!);
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
                    new("content_txt","text_general", stored:true, indexed:true)
                };

                var toAdd = need.Where(n => !existing.Contains(n.name)).ToList();
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

        private async Task<bool> AddDocAsync(Dictionary<string, object?> doc, int maxRetries = 3)
        {
            if (!doc.TryGetValue("content_txt", out var textObj) || string.IsNullOrWhiteSpace(textObj as string))
                return true; // never send empty

            var payload = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["add"] = new[] { doc }
            });
            return await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger, maxRetries);
        }

        private async Task<string> GetAnyChecksumForFileAsync(int projectId, string fileId)
        {
            try
            {
                var url = $"{_solrURL}/select?q=projectId_i:{projectId}%20AND%20fileId_s:{fileId}&fl=checksum_s,id&rows=1&wt=json";
                var resp = await _http.GetAsync(url);
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
            catch { return string.Empty; }
        }

        private async Task<bool> TouchAllDocsScanIdAsync(int projectId, string fileId, string scanId)
        {
            var ids = await SelectIdsForFileAsync(projectId, fileId);
            if (ids.Count == 0) return true;

            // Try atomic update WITHOUT _root_ first
            var atomicAny = await TryAtomicTouch(ids, scanId);

            // Identify top-level/unknown (id==_root_ or _root_ missing)
            var pairs = await SelectIdRootPairsForFileAsync(projectId, fileId);
            var topLevelOrUnknown = pairs
                .Where(p => string.IsNullOrEmpty(p.Root) || string.Equals(p.Id, p.Root, StringComparison.Ordinal))
                .Select(p => p.Id)
                .Distinct(StringComparer.Ordinal)
                .ToList();

            var fullReplaceOk = topLevelOrUnknown.Count == 0 || await TryFullReplaceTouch(topLevelOrUnknown, scanId);

            // If any child docs exist we skip them (never send _root_)—return false so caller logs partial touch.
            var hasChildren = pairs.Any(p => !string.IsNullOrEmpty(p.Root) && !string.Equals(p.Id, p.Root, StringComparison.Ordinal));
            if (hasChildren) return false;

            return atomicAny || fullReplaceOk;
        }

        private async Task<bool> TryAtomicTouch(List<string> ids, string scanId)
        {
            const int BATCH = 500;
            bool anyOk = false;

            for (int i = 0; i < ids.Count; i += BATCH)
            {
                var docs = ids.Skip(i).Take(BATCH)
                    .Select(id => new Dictionary<string, object?>
                    {
                        ["id"] = id,
                        ["scanId_s"] = new Dictionary<string, object?> { ["set"] = scanId }
                    }).ToArray();

                var payload = JsonSerializer.Serialize(new Dictionary<string, object?> { ["add"] = docs });

                var (ok, _, body) = await PostSolrJsonAsyncDetailed($"{_solrURL}/update", payload, _logger);
                if (!ok)
                {
                    _logger.LogDebug("Atomic touch (no-root) batch skipped; body={body}", body);
                }
                else anyOk = true;
            }
            return anyOk;
        }

        private async Task<bool> TryFullReplaceTouch(List<string> ids, string scanId)
        {
            // RTG docs, set scanId_s, strip _version_ and _root_ (never send _root_)
            const int BATCH = 300;
            bool anyOk = false;

            for (int i = 0; i < ids.Count; i += BATCH)
            {
                var slice = ids.Skip(i).Take(BATCH).ToList();
                var docs = await RealTimeGetDocsAsync(slice);
                if (docs.Count == 0) continue;

                foreach (var d in docs)
                {
                    d["scanId_s"] = scanId;
                    d.Remove("_version_");
                    d.Remove("_root_");
                }

                var payload = JsonSerializer.Serialize(new Dictionary<string, object?> { ["add"] = docs });
                var (ok, _, body) = await PostSolrJsonAsyncDetailed($"{_solrURL}/update", payload, _logger);
                if (!ok)
                {
                    _logger.LogDebug("Full-replace touch batch skipped; body={body}", body);
                }
                else anyOk = true;
            }

            return anyOk;
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
                if (!doc.RootElement.TryGetProperty("response", out var response)) break;
                if (!response.TryGetProperty("docs", out var docs)) break;

                foreach (var d in docs.EnumerateArray())
                    if (d.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String)
                        results.Add(id.GetString()!);

                if (!doc.RootElement.TryGetProperty("nextCursorMark", out var next)) break;
                var nextCursor = next.GetString() ?? cursor;
                if (nextCursor == cursor) break;
                cursor = nextCursor;
            }
            return results;
        }

        private async Task<List<(string Id, string? Root)>> SelectIdRootPairsForFileAsync(int projectId, string fileId)
        {
            var results = new List<(string Id, string? Root)>();
            string cursor = "*";
            const int ROWS = 1000;

            while (true)
            {
                var url = $"{_solrURL}/select?q=projectId_i:{projectId}%20AND%20fileId_s:{fileId}" +
                          $"&fl=id,_root_&rows={ROWS}&sort=id%20asc&cursorMark={Uri.EscapeDataString(cursor)}&wt=json";
                var resp = await _http.GetAsync(url);
                if (!resp.IsSuccessStatusCode) break;

                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("response", out var response)) break;
                if (!response.TryGetProperty("docs", out var docs)) break;

                foreach (var d in docs.EnumerateArray())
                {
                    string? id = d.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String ? idEl.GetString() : null;
                    if (string.IsNullOrEmpty(id)) continue;
                    string? root = d.TryGetProperty("_root_", out var rootEl) && rootEl.ValueKind == JsonValueKind.String ? rootEl.GetString() : null;
                    results.Add((id!, root));
                }

                if (!doc.RootElement.TryGetProperty("nextCursorMark", out var next)) break;
                var nextCursor = next.GetString() ?? cursor;
                if (nextCursor == cursor) break;
                cursor = nextCursor;
            }
            return results;
        }

        private async Task<List<Dictionary<string, object?>>> RealTimeGetDocsAsync(List<string> ids)
        {
            var url = $"{_solrURL}/get?fl={Uri.EscapeDataString("*")}&wt=json";
            var payload = JsonSerializer.Serialize(new { ids = ids });
            var resp = await _http.PostAsync(url, new StringContent(payload, Encoding.UTF8, "application/json"));
            if (!resp.IsSuccessStatusCode) return new List<Dictionary<string, object?>>();

            var json = await resp.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("docs", out var arr) || arr.ValueKind != JsonValueKind.Array)
                return new List<Dictionary<string, object?>>();

            var list = new List<Dictionary<string, object?>>();
            foreach (var d in arr.EnumerateArray())
                list.Add(JsonToDictionary(d));
            return list;
        }

        private async Task<(bool ok, int status, string body)> PostSolrJsonAsyncDetailed(string url, string json, ILogger logger, int maxRetries = 2)
        {
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    using var content = new StringContent(json, Encoding.UTF8, "application/json");
                    var resp = await _http.PostAsync(url, content);
                    var body = await SafeReadAsync(resp);
                    if (resp.IsSuccessStatusCode) return (true, (int)resp.StatusCode, body);

                    logger.LogError("Solr call failed (attempt {attempt}/{max}) to {url}: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body: {body}",
                        attempt, maxRetries, url, (int)resp.StatusCode, resp.ReasonPhrase, body);
                }
                catch (TaskCanceledException tex)
                {
                    logger.LogError(tex, "Solr call timeout (attempt {attempt}/{max}) to {url}", attempt, maxRetries, url);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Solr call exception (attempt {attempt}/{max}) to {url}", attempt, maxRetries, url);
                }
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt * attempt));
            }
            return (false, 0, "");
        }

        private async Task<bool> PostSolrJsonAsync(string url, string json, ILogger logger, int maxRetries = 3)
        {
            var (ok, _, _) = await PostSolrJsonAsyncDetailed(url, json, logger, maxRetries);
            return ok;
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
                        try { text = pdf.GetPage(p)?.Text; }
                        catch (PdfDocumentFormatException ex) { _logger.LogWarning(ex, "Skipping bad PDF page {page} in {path}", p, path); }
                        catch (Exception ex) { _logger.LogWarning(ex, "Skipping page {page} in {path}", p, path); }

                        if (!string.IsNullOrWhiteSpace(text))
                            yield return (p, text);
                    }
                }
            }
            finally
            {
                if (!string.IsNullOrEmpty(tempSalvaged))
                {
                    try { File.Delete(tempSalvaged); } catch { /* ignore */ }
                }
            }
        }

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
                if (!TrySalvagePdfByHeader(path, out tempSalvaged)) return false;

                try { pdf = PdfDocument.Open(tempSalvaged); return true; }
                catch (Exception ex2) { _logger.LogError(ex2, "Salvaged PDF still failed to open: {temp}", tempSalvaged); return false; }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PDF open failed: {path}", path);
                return false;
            }
        }

        private bool TrySalvagePdfByHeader(string path, out string tempPath)
        {
            tempPath = null!;
            try
            {
                var bytes = File.ReadAllBytes(path);
                if (bytes == null || bytes.Length < 8) return false;

                var sig = Encoding.ASCII.GetBytes("%PDF-");
                var idx = IndexOf(bytes, sig, 0, Math.Min(bytes.Length, 1024 * 1024));
                if (idx < 0) return false;

                tempPath = Path.Combine(Path.GetTempPath(), "pcnw_pdfsalvage_" + Guid.NewGuid().ToString("N") + ".pdf");
                using (var fs = File.Create(tempPath)) fs.Write(bytes, idx, bytes.Length - idx);

                _logger.LogInformation("Salvaged PDF by trimming {trim} bytes: {temp}", idx, tempPath);
                return true;
            }
            catch (Exception ex) { _logger.LogWarning(ex, "TrySalvagePdfByHeader failed for {path}", path); tempPath = null!; return false; }
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
            return doc.ToString(SaveFormat.Text);
        }

        // ---------------------------
        // Cleanup & utilities
        // ---------------------------

        public async Task<bool> ClearSolrAllAsync()
        {
            try
            {
                var upd = $"{_solrURL}/update";
                _logger.LogWarning("Clearing ALL documents from Solr at: {url}", upd);

                var delOk = await PostSolrJsonAsync(upd, JsonSerializer.Serialize(new { delete = new { query = "*:*" } }), _logger);
                if (!delOk) return false;

                return await PostSolrJsonAsync($"{upd}?commit=true", "{}", _logger);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception while clearing Solr.");
                return false;
            }
        }

        public async Task<bool> RemoveDocsMissingRequiredFieldsAsync()
        {
            var q = "-projectId_i:[* TO *] OR -doc_type_s:[* TO *]";
            var payload = JsonSerializer.Serialize(new { delete = new { query = q } });
            var ok = await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
            if (!ok) return false;
            return await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
        }

        public async Task<bool> RemoveDocsWithLiteralSetInScanIdAsync()
        {
            var q = @"scanId_s:\{set\=*";
            var payload = JsonSerializer.Serialize(new { delete = new { query = q } });
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
            foreach (var rx in AsposeEvalNoise) text = rx.Replace(text, string.Empty);
            text = Regex.Replace(text, @"[ \t]+\r?\n", "\n");
            text = Regex.Replace(text, @"(\r?\n){3,}", "\n\n");
            return text.Trim();
        }

        private static string SanitizeForSolrText(string input, int maxToken = 800)
        {
            if (string.IsNullOrEmpty(input)) return string.Empty;

            var sb = new StringBuilder(input.Length);
            for (int i = 0; i < input.Length; i++)
            {
                char c = input[i];

                // Skip unpaired surrogates
                if (char.IsSurrogate(c))
                {
                    if (char.IsHighSurrogate(c))
                    {
                        if (i + 1 < input.Length && char.IsLowSurrogate(input[i + 1]))
                        {
                            sb.Append(c);
                            sb.Append(input[++i]);
                        }
                    }
                    continue;
                }

                if (c == '\0') continue;
                if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') continue;

                sb.Append(c);
            }

            var cleaned = sb.ToString();

            // Force-break very long non-whitespace runs to avoid Lucene "immense term"
            var outSb = new StringBuilder(cleaned.Length + 64);
            int runLen = 0;
            for (int i = 0; i < cleaned.Length; i++)
            {
                char c = cleaned[i];
                bool isWs = char.IsWhiteSpace(c);

                if (!isWs)
                {
                    if (runLen >= maxToken)
                    {
                        outSb.Append(' ');
                        runLen = 0;
                    }
                    runLen++;
                }
                else
                {
                    runLen = 0;
                }

                outSb.Append(c);
            }

            var finalText = Regex.Replace(outSb.ToString(), @"(\r?\n){3,}", "\n\n");
            return finalText.Trim();
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

        private static object? JsonToNet(JsonElement e) => e.ValueKind switch
        {
            JsonValueKind.String => e.GetString(),
            JsonValueKind.Number => e.TryGetInt64(out var l) ? l : (object?)e.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => e.EnumerateArray().Select(JsonToNet).ToList(),
            JsonValueKind.Object => e.EnumerateObject().ToDictionary(p => p.Name, p => JsonToNet(e.GetProperty(p.Name))),
            _ => null
        };

        private static Dictionary<string, object?> JsonToDictionary(JsonElement e)
        {
            var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var p in e.EnumerateObject()) dict[p.Name] = JsonToNet(p.Value);
            return dict;
        }

        private async Task<bool> DeleteByFileIdAsync(int projectId, string fileId)
        {
            var q = $"projectId_i:{projectId} AND fileId_s:{fileId}";
            return await PostSolrJsonAsync($"{_solrURL}/update",
                JsonSerializer.Serialize(new { delete = new { query = q } }), _logger);
        }
        private static string EscapeTerm(string term)
        {
            if (string.IsNullOrEmpty(term)) return "\"\"";
            bool needsQuote = term.Any(ch =>
                char.IsWhiteSpace(ch) || ch is ':' or '(' or ')' or '"' or '\\');
            return needsQuote
                ? "\"" + term.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\""
                : term;
        }

        private async Task DeleteStaleForFilesAsync(int projectId, string scanId, HashSet<string> processedFileIds)
        {
            if (processedFileIds.Count == 0) return;

            const int MAX_PER_BATCH = 100;
            var fileIds = processedFileIds.ToList();

            for (int i = 0; i < fileIds.Count; i += MAX_PER_BATCH)
            {
                var batch = fileIds.Skip(i).Take(MAX_PER_BATCH).ToList();

                // IMPORTANT: plain spaces in the Lucene query string (no %20)
                var or = string.Join(" OR ", batch.Select(fid => $"fileId_s:{EscapeTerm(fid)}"));

                var q = $"projectId_i:{projectId} AND ({or}) AND scanId_s:[* TO *] AND -scanId_s:{EscapeTerm(scanId)}";

                var payload = JsonSerializer.Serialize(new { delete = new { query = q } });

                _ = await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
            }
        }
    }
}
