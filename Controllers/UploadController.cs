using Aspose.Words;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using PCNWSolrUploadFiles.Data;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using UglyToad.PdfPig;
using UglyToad.PdfPig.Core;

namespace PCNWSolrUploadFiles.Controllers
{
    public class UploadController
    {
        private readonly PcnwprojectDbContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly string _fileUploadPath;
        private readonly string _solrURL; // e.g., http://localhost:8983/solr/pcnw_project
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

        // ---------- Logging EventIds ----------
        private static class Ev
        {
            public static readonly EventId RunStart = new(1000, nameof(RunStart));
            public static readonly EventId ProjectStart = new(1100, nameof(ProjectStart));
            public static readonly EventId FileStart = new(1200, nameof(FileStart));
            public static readonly EventId FileTouch = new(1201, nameof(FileTouch));
            public static readonly EventId FileUploadOk = new(1202, nameof(FileUploadOk));
            public static readonly EventId FileUploadFail = new(1203, nameof(FileUploadFail));
            public static readonly EventId PageAdd = new(1210, nameof(PageAdd));
            public static readonly EventId ProjectSummary = new(1300, nameof(ProjectSummary));
            public static readonly EventId RunComplete = new(1999, nameof(RunComplete));
        }

        [System.Diagnostics.Conditional("TRACE")]
        private void TracePageAdd(string file, int page, long ms)
        {
            _logger.LogTrace(Ev.PageAdd, "  └─ Page {page} added from {file} ({ms} ms)", page, file, ms);
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
        /// NEW: Purge invalid/empty docs that slipped into Solr (e.g., only id, or missing required fields, or empty content).
        /// </summary>
        public async Task<bool> PurgeEmptyOrInvalidDocsAsync() // NEW
        {
            try
            {
                // Build a conservative set of delete-by-queries. Each one targets a kind of "bad" doc.
                var queries = new List<string>
                {
                    // Missing required fields
                    "-projectId_i:[* TO *]",
                    "-fileId_s:[* TO *]",
                    "-doc_type_s:[* TO *]",
                    "-filename_s:[* TO *]",
                    "-relativePath_s:[* TO *]",
                    "-checksum_s:[* TO *]",
                    "-scanId_s:[* TO *]",

                    // For pages: missing page_i
                    "(doc_type_s:page AND -page_i:[* TO *])",

                    // Docs with content_txt explicitly empty string (stored field may hold "")
                    "content_txt:\"\"",

                    // Docs with no content_txt field at all (keep this **optional**; uncomment if desired)
                    // "-content_txt:[* TO *]"
                };

                foreach (var q in queries)
                {
                    var payload = JsonSerializer.Serialize(new { delete = new { query = q } });
                    var ok = await PostSolrJsonAsync($"{_solrURL}/update", payload, _logger);
                    _logger.LogInformation("Purge query executed: {q} -> {ok}", q, ok);
                }

                var commitOk = await PostSolrJsonAsync($"{_solrURL}/update?commit=true", "{}", _logger);
                _logger.LogInformation("Purge commit: {ok}", commitOk);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during PurgeEmptyOrInvalidDocsAsync.");
                return false;
            }
        }

        /// <summary>
        /// Main indexer: scans folders and sends files to Solr.
        /// - Only uploads when a project folder has supported files.
        /// - Unchanged files (checksum) => atomic "touch" to set scanId_s (no re-extraction), unless forceReindex=true.
        /// - New/changed => delete old docs of that fileId, re-add.
        /// - Project-level cleanup deletes only docs that have a scanId_s and don't match this run's scanId (safe).
        /// </summary>
        public async Task UploadAllFiles(bool forceReindex = false, bool purgeAfter = true) // NEW: forceReindex & purgeAfter
        {
            //await ClearSolrAllAsync();
            var overallStart = DateTime.UtcNow;
            var runId = Guid.NewGuid().ToString("N");

            using (_logger.BeginScope(new Dictionary<string, object> { ["RunId"] = runId }))
            {
                try
                {
                    _logger.LogInformation(Ev.RunStart, "UploadAllFiles RUN START at {local} ({utc} UTC)", DateTime.Now, DateTime.UtcNow);

                    if (string.IsNullOrWhiteSpace(_solrURL))
                        throw new InvalidOperationException("SolrURL not configured.");

                    // Ensure schema once up front (idempotent) — includes making content_txt stored=true
                    var schemaOk = await EnsureSolrSchemaAsync(); // NEW: will repair content_txt if needed
                    if (!schemaOk)
                        _logger.LogWarning("Could not verify/create schema. Proceeding, but field storage/indexing may be wrong.");

                    var baseDirectory = _fileUploadPath;
                    if (!Directory.Exists(baseDirectory))
                        throw new DirectoryNotFoundException($"Base directory not found: {baseDirectory}");

                    var yearFolders = Directory.GetDirectories(baseDirectory);
                    foreach (var yearFolder in yearFolders)
                    {
                        var monthFolders = Directory.GetDirectories(yearFolder);
                        foreach (var monthFolder in monthFolders)
                        {
                            var projectNumberFolders = Directory.GetDirectories(monthFolder);
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

                                var projectScope = new Dictionary<string, object>
                                {
                                    ["RunId"] = runId,
                                    ["ProjectId"] = projectId,
                                    ["ProjNumber"] = projectNumber
                                };

                                using (_logger.BeginScope(projectScope))
                                {
                                    _logger.LogInformation(Ev.ProjectStart,
                                        "PROJECT START {projNumber} (Id {pid}) with {count} candidate files",
                                        projectNumber, projectId, files.Count);

                                    if (files.Count == 0)
                                    {
                                        _logger.LogInformation("No supported files; skipping (no deletions).");
                                        continue;
                                    }

                                    var scanId = Guid.NewGuid().ToString("N");
                                    int index = 0, newOrUpdated = 0, touched = 0, failed = 0, addedDocs = 0;

                                    foreach (var file in files)
                                    {
                                        index++;
                                        var swFile = System.Diagnostics.Stopwatch.StartNew();
                                        try
                                        {
                                            var relativePath = GetRelativePath(projectFolder, file).Replace('\\', '/'); // project-relative
                                            var fileId = Sha1(relativePath); // stable per relative path
                                            var idBase = $"{projectId}|{fileId}";

                                            var checksum = ComputeSHA256(file);
                                            var existingChecksum = await GetAnyChecksumForFileAsync(projectId, fileId);

                                            _logger.LogInformation(Ev.FileStart, "[{idx}/{tot}] START file: {file} (fileId={fileId})",
                                                index, files.Count, relativePath, fileId);

                                            var treatAsChanged =
                                                forceReindex ||
                                                string.IsNullOrEmpty(existingChecksum) ||
                                                !string.Equals(existingChecksum, checksum, StringComparison.OrdinalIgnoreCase);

                                            if (!treatAsChanged)
                                            {
                                                // unchanged → just touch scanId_s
                                                var swTouch = System.Diagnostics.Stopwatch.StartNew();
                                                var touchedOk = await TouchAllDocsScanIdAsync(projectId, fileId, scanId);
                                                swTouch.Stop();

                                                if (!touchedOk)
                                                {
                                                    failed++;
                                                    _logger.LogError(Ev.FileUploadFail, "[{idx}/{tot}] TOUCH FAILED file: {file} (fileId={fileId}) in {ms} ms",
                                                        index, files.Count, relativePath, fileId, swTouch.ElapsedMilliseconds);
                                                }
                                                else
                                                {
                                                    touched++;
                                                    _logger.LogInformation(Ev.FileTouch, "[{idx}/{tot}] UNCHANGED → touched file: {file} (fileId={fileId}) in {ms} ms",
                                                        index, files.Count, relativePath, fileId, swTouch.ElapsedMilliseconds);
                                                }

                                                swFile.Stop();
                                                continue;
                                            }

                                            // Changed/new/forced → delete old docs for this file, then (re)add
                                            _ = await DeleteByFileIdAsync(projectId, fileId);

                                            var info = new FileInfo(file);
                                            var lastModUtc = info.LastWriteTimeUtc;

                                            var ext = Path.GetExtension(file).ToLowerInvariant();
                                            int pagesAdded = 0;

                                            if (ext == ".pdf")
                                            {
                                                foreach (var (page, textRaw) in ExtractPdfPages(file))
                                                {
                                                    var t0 = System.Diagnostics.Stopwatch.StartNew();
                                                    var text = CleanAsposeEvaluationNoise(textRaw);
                                                    if (string.IsNullOrWhiteSpace(text)) { t0.Stop(); continue; }

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
                                                        ["content_txt"] = text // STORED now after schema fix
                                                    };

                                                    if (!await AddDocsBatchAsync_One(doc))
                                                    {
                                                        failed++;
                                                        _logger.LogError(Ev.FileUploadFail, "[{idx}/{tot}] Failed adding page {page} for {file}",
                                                            index, files.Count, page, relativePath);
                                                        break;
                                                    }
                                                    t0.Stop();
                                                    pagesAdded++;
                                                    addedDocs++;
                                                    TracePageAdd(relativePath, page, t0.ElapsedMilliseconds);
                                                }
                                            }
                                            else if (ext == ".docx" || ext == ".doc")
                                            {
                                                var t0 = System.Diagnostics.Stopwatch.StartNew();
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

                                                    if (!await AddDocsBatchAsync_One(doc))
                                                    {
                                                        failed++;
                                                        _logger.LogError(Ev.FileUploadFail, "[{idx}/{tot}] Failed adding WORD doc {file}",
                                                            index, files.Count, relativePath);
                                                    }
                                                    else
                                                    {
                                                        pagesAdded = 1;
                                                        addedDocs++;
                                                        TracePageAdd(relativePath, 1, t0.ElapsedMilliseconds);
                                                    }
                                                }
                                            }
                                            else if (ext == ".txt")
                                            {
                                                var t0 = System.Diagnostics.Stopwatch.StartNew();
                                                var text = File.ReadAllText(file, Encoding.UTF8);
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

                                                    if (!await AddDocsBatchAsync_One(doc))
                                                    {
                                                        failed++;
                                                        _logger.LogError(Ev.FileUploadFail, "[{idx}/{tot}] Failed adding TXT doc {file}",
                                                            index, files.Count, relativePath);
                                                    }
                                                    else
                                                    {
                                                        pagesAdded = 1;
                                                        addedDocs++;
                                                        TracePageAdd(relativePath, 1, t0.ElapsedMilliseconds);
                                                    }
                                                }
                                            }

                                            swFile.Stop();

                                            if (pagesAdded > 0)
                                            {
                                                newOrUpdated++;
                                                _logger.LogInformation(Ev.FileUploadOk,
                                                    "[{idx}/{tot}] UPLOADED file: {file} (pages={pages}, docsAdded={docs}, {ms} ms)",
                                                    index, files.Count, relativePath, pagesAdded, addedDocs, swFile.ElapsedMilliseconds);
                                            }
                                            else
                                            {
                                                _logger.LogInformation(Ev.FileUploadOk,
                                                    "[{idx}/{tot}] SKIPPED (no extractable text) file: {file} ({ms} ms)",
                                                    index, files.Count, relativePath, swFile.ElapsedMilliseconds);
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            swFile.Stop();
                                            failed++;
                                            _logger.LogError(Ev.FileUploadFail, ex, "[{idx}/{tot}] ERROR file: {file} ({ms} ms)",
                                                index, files.Count, file, swFile.ElapsedMilliseconds);
                                        }
                                    }

                                    // Project-level cleanup: remove docs from earlier runs safely
                                    var deleteStaleQuery = $"projectId_i:{projectId} AND scanId_s:[* TO *] AND -scanId_s:{scanId}";
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
                                    _logger.LogInformation(Ev.ProjectSummary,
                                        "PROJECT SUMMARY {proj} → New/Updated={up}, UnchangedTouched={touch}, Failed={fail}, DocsAdded={docs}, Elapsed={ms}ms",
                                        projectNumber, newOrUpdated, touched, failed, addedDocs, swProject.ElapsedMilliseconds);
                                }
                            }
                        }
                    }

                    // Optional: one final purge across the core after indexing
                    if (purgeAfter)
                    {
                        await PurgeEmptyOrInvalidDocsAsync(); // NEW
                    }

                    var elapsed = DateTime.UtcNow - overallStart;
                    _logger.LogInformation(Ev.RunComplete, "RUN COMPLETE in {ms} ms (RunId={runId})", (long)elapsed.TotalMilliseconds, runId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred while uploading files (RunId={runId})", runId);
                    throw;
                }
            }
        }

        // ---------- Solr schema bootstrap & repairs ----------

        private sealed record FieldDef(string name, string type, bool stored, bool indexed);
        private sealed record SolrFieldInfo(string name, string type, bool stored, bool indexed); // NEW

        private async Task<SolrFieldInfo?> GetFieldAsync(string fieldName) // NEW
        {
            try
            {
                var url = $"{_solrURL}/schema/fields/{Uri.EscapeDataString(fieldName)}?wt=json";
                var resp = await _http.GetAsync(url);
                if (!resp.IsSuccessStatusCode) return null;

                using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
                if (doc.RootElement.TryGetProperty("field", out var f))
                {
                    return new SolrFieldInfo(
                        f.GetProperty("name").GetString()!,
                        f.GetProperty("type").GetString()!,
                        f.GetProperty("stored").GetBoolean(),
                        f.GetProperty("indexed").GetBoolean()
                    );
                }
                return null;
            }
            catch
            {
                return null;
            }
        }

        private async Task<bool> ReplaceField_ContentTxt_ToStoredAsync(bool useFastVector = false) // NEW
        {
            var field = new Dictionary<string, object?>
            {
                ["name"] = "content_txt",
                ["type"] = "text_general",
                ["stored"] = true,
                ["indexed"] = true,
                ["multiValued"] = false
            };

            // If you plan to use Fast Vector Highlighter later, enable term vectors and reindex
            if (useFastVector)
            {
                field["termVectors"] = true;
                field["termPositions"] = true;
                field["termOffsets"] = true;
            }

            var payload = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["replace-field"] = field
            });

            return await PostSolrJsonAsync($"{_solrURL}/schema", payload, _logger);
        }

        private async Task<bool> EnsureSolrSchemaAsync()
        {
            try
            {
                // 1) Get existing fields (plain list)
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

                // Ensure typed fields (adjust pint/plong vs int/long according to your core)
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
                    new("scanId_s","string", stored:true, indexed:true)
                    // content_txt handled separately below to guarantee stored=true + text_general
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
                    var addResp = await PostSolrJsonAsync($"{_solrURL}/schema", JsonSerializer.Serialize(addPayload), _logger);
                    if (!addResp) _logger.LogWarning("Schema add-field returned non-success for base fields.");
                }

                // 2) Guarantee content_txt is stored:true and analyzed
                var current = await GetFieldAsync("content_txt");
                if (current is null)
                {
                    var addContent = new Dictionary<string, object?>
                    {
                        ["add-field"] = new object[]
                        {
                            new Dictionary<string, object?>
                            {
                                ["name"] = "content_txt",
                                ["type"] = "text_general",
                                ["stored"] = true,
                                ["indexed"] = true,
                                ["multiValued"] = false
                            }
                        }
                    };
                    var ok = await PostSolrJsonAsync($"{_solrURL}/schema", JsonSerializer.Serialize(addContent), _logger);
                    if (!ok) _logger.LogWarning("Failed to add content_txt as stored=true, text_general.");
                }
                else
                {
                    if (!current.stored || !string.Equals(current.type, "text_general", StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogInformation("Fixing content_txt schema (stored={stored}, type={type}) → stored=true, text_general.",
                            current.stored, current.type);
                        var ok = await ReplaceField_ContentTxt_ToStoredAsync(useFastVector: false);
                        if (!ok) _logger.LogWarning("replace-field for content_txt failed.");
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

        // ---------- Solr helpers (add/touch/delete/select) ----------

        // Simple single-doc add (keeps payloads small & robust)
        private async Task<bool> AddDocsBatchAsync_One(Dictionary<string, object?> doc, int maxRetries = 3)
        {
            var payload = JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["add"] = new[] { new Dictionary<string, object?> { ["doc"] = doc } }
            });
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

        private async Task<bool> TouchAllDocsScanIdAsync(int projectId, string fileId, string scanId)
        {
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

        // ---------- Extraction helpers ----------

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
                            yield return (p, text);  // yield OUTSIDE the try/catch
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
            return doc.ToString(SaveFormat.Text);
        }

        // ---------- Utilities ----------

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
