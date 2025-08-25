using System;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

public static class SolrRootFieldFixer
{
    /// <summary>
    /// Makes sure the core/collection at {coreUrl} has _root_ with docValues=false, indexed=false, stored=false.
    /// Then reloads the core (or collection) to apply changes.
    ///
    /// Pass coreUrl like: "http://HOST:8983/solr/pcnw_project"
    /// </summary>
    public static async Task<bool> EnsureRootNoDocValuesAsync(
        string coreUrl,
        ILogger? logger = null,
        HttpClient? httpClient = null,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(coreUrl))
            throw new ArgumentException("coreUrl is required", nameof(coreUrl));

        var dispose = false;
        var http = httpClient;
        if (http is null)
        {
            http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            dispose = true;
        }

        try
        {
            // 1) Fetch schema fields
            var fieldsUrl = Combine(coreUrl, "schema/fields?wt=json");
            var fieldsResp = await http.GetAsync(fieldsUrl, ct);
            var fieldsJson = await SafeReadAsync(fieldsResp);
            if (!fieldsResp.IsSuccessStatusCode)
            {
                logger?.LogError("Schema fields call failed: {code} {reason}. Body: {body}",
                    (int)fieldsResp.StatusCode, fieldsResp.ReasonPhrase, fieldsJson);
                return false;
            }

            using var doc = JsonDocument.Parse(fieldsJson);
            var fieldsArr = doc.RootElement.GetProperty("fields");
            var rootField = fieldsArr.EnumerateArray()
                                     .FirstOrDefault(f => f.TryGetProperty("name", out var n) &&
                                                          n.ValueKind == JsonValueKind.String &&
                                                          string.Equals(n.GetString(), "_root_", StringComparison.Ordinal));

            bool needsChange = false;
            if (rootField.ValueKind == JsonValueKind.Undefined)
            {
                // _root_ missing — we'll add it with the minimal, safe settings.
                needsChange = true;
                var addOk = await PostSchemaAsync(http, coreUrl, new
                {
                    // use a dictionary because hyphens aren't allowed in anonymous type property names
                }, new Func<string>(() =>
                {
                    var payload = new System.Collections.Generic.Dictionary<string, object?>
                    {
                        ["add-field"] = new System.Collections.Generic.Dictionary<string, object?>
                        {
                            ["name"] = "_root_",
                            ["type"] = "string",
                            ["indexed"] = false,
                            ["stored"] = false,
                            ["docValues"] = false,
                            ["multiValued"] = false
                        }
                    };
                    return JsonSerializer.Serialize(payload);
                })(), logger, ct);

                if (!addOk) return false;
                logger?.LogInformation("Added _root_ as non-indexed/non-stored/docValues=false.");
            }
            else
            {
                bool docValues = rootField.TryGetProperty("docValues", out var dv) && dv.ValueKind == JsonValueKind.True;
                bool indexed = rootField.TryGetProperty("indexed", out var ix) && ix.ValueKind == JsonValueKind.True;
                bool stored = rootField.TryGetProperty("stored", out var st) && st.ValueKind == JsonValueKind.True;
                bool multiValued = rootField.TryGetProperty("multiValued", out var mv) && mv.ValueKind == JsonValueKind.True;

                if (docValues || indexed || stored || multiValued)
                {
                    needsChange = true;

                    // 2) Replace-field to minimal flags
                    var replacePayload = new System.Collections.Generic.Dictionary<string, object?>
                    {
                        ["replace-field"] = new System.Collections.Generic.Dictionary<string, object?>
                        {
                            ["name"] = "_root_",
                            ["type"] = "string",
                            ["indexed"] = false,
                            ["stored"] = false,
                            ["docValues"] = false,
                            ["multiValued"] = false
                        }
                    };
                    var replaceOk = await PostSchemaAsync(http, coreUrl, replacePayload, null, logger, ct);
                    if (!replaceOk) return false;
                    logger?.LogInformation("Replaced _root_ to non-indexed/non-stored/docValues=false.");
                }
            }

            // 3) Reload core/collection if we changed anything
            if (needsChange)
            {
                var coreName = GetCoreOrCollectionName(coreUrl);
                bool reloaded = await TryReloadCoreAsync(http, coreUrl, coreName, logger, ct)
                                 || await TryReloadCollectionAsync(http, coreUrl, coreName, logger, ct);
                if (!reloaded)
                {
                    logger?.LogWarning("Reload via /admin/cores and /admin/collections both failed. "
                                     + "If using SolrCloud, ensure collection name is correct and ZK is reachable.");
                    // not a hard failure; schema change might still be live on managed schema cores,
                    // but generally reload is recommended. Decide whether to treat as error:
                    // return false;
                }
                else
                {
                    logger?.LogInformation("Reloaded {core}.", coreName);
                }
            }
            else
            {
                logger?.LogInformation("_root_ already aligned; no schema change needed.");
            }

            // 4) Optional: sanity check with Luke (not required to succeed)
            try
            {
                var lukeUrl = Combine(coreUrl, "admin/luke?wt=json&numTerms=0");
                var lukeResp = await http.GetAsync(lukeUrl, ct);
                var lukeJson = await SafeReadAsync(lukeResp);
                if (lukeResp.IsSuccessStatusCode)
                {
                    using var l = JsonDocument.Parse(lukeJson);
                    if (l.RootElement.TryGetProperty("fields", out var fobj) &&
                        fobj.TryGetProperty("_root_", out var rootInfo) &&
                        rootInfo.TryGetProperty("schema", out var mask) &&
                        mask.ValueKind == JsonValueKind.String)
                    {
                        var flags = mask.GetString() ?? "";
                        // Expect no 'D' (DocValues) for _root_
                        if (flags.Contains('D'))
                            logger?.LogWarning("Luke shows DocValues still present for _root_. "
                                             + "If old segments exist with DV, consider a fresh reindex.");
                        else
                            logger?.LogInformation("Luke OK: _root_ has no DocValues.");
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogDebug(ex, "Luke check skipped/failed.");
            }

            return true;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "EnsureRootNoDocValuesAsync failed.");
            return false;
        }
        finally
        {
            if (dispose) http.Dispose();
        }
    }

    // ---------- helpers ----------

    private static async Task<bool> PostSchemaAsync(
        HttpClient http,
        string coreUrl,
        object? objPayload,
        string? rawJsonPayload,
        ILogger? logger,
        CancellationToken ct)
    {
        var schemaUrl = Combine(coreUrl, "schema");
        string json;

        if (rawJsonPayload is not null)
            json = rawJsonPayload;
        else
            json = JsonSerializer.Serialize(objPayload!, new JsonSerializerOptions { WriteIndented = false });

        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        var resp = await http.PostAsync(schemaUrl, content, ct);
        var body = await SafeReadAsync(resp);

        if (!resp.IsSuccessStatusCode)
        {
            logger?.LogError("Schema POST failed: {code} {reason}. Body: {body}",
                (int)resp.StatusCode, resp.ReasonPhrase, body);
            // Common failure when schema is not mutable (static schema.xml)
            // In that case, manual edit of schema + core reload is required.
            return false;
        }

        return true;
    }

    private static async Task<bool> TryReloadCoreAsync(
        HttpClient http, string coreUrl, string coreName, ILogger? logger, CancellationToken ct)
    {
        var adminCores = GetBaseSolrUrl(coreUrl) + "/admin/cores?action=RELOAD&core=" + Uri.EscapeDataString(coreName);
        var resp = await http.GetAsync(adminCores, ct);
        var body = await SafeReadAsync(resp);
        if (!resp.IsSuccessStatusCode)
        {
            logger?.LogDebug("Core reload failed: {code} {reason}. Body: {body}",
                (int)resp.StatusCode, resp.ReasonPhrase, body);
            return false;
        }
        return true;
    }

    private static async Task<bool> TryReloadCollectionAsync(
        HttpClient http, string coreUrl, string collectionName, ILogger? logger, CancellationToken ct)
    {
        var adminCollections = GetBaseSolrUrl(coreUrl) + "/admin/collections?action=RELOAD&name=" + Uri.EscapeDataString(collectionName);
        var resp = await http.GetAsync(adminCollections, ct);
        var body = await SafeReadAsync(resp);
        if (!resp.IsSuccessStatusCode)
        {
            logger?.LogDebug("Collection reload failed: {code} {reason}. Body: {body}",
                (int)resp.StatusCode, resp.ReasonPhrase, body);
            return false;
        }
        return true;
    }

    private static string GetCoreOrCollectionName(string coreUrl)
    {
        var u = new Uri(coreUrl, UriKind.Absolute);
        // Expect: /solr/<coreOrCollection>[/...]
        var segments = u.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var solrIdx = Array.FindIndex(segments, s => string.Equals(s, "solr", StringComparison.OrdinalIgnoreCase));
        if (solrIdx >= 0 && solrIdx + 1 < segments.Length)
            return segments[solrIdx + 1];
        // fallback: last segment
        return segments.LastOrDefault() ?? "collection1";
    }

    private static string GetBaseSolrUrl(string coreUrl)
    {
        var u = new Uri(coreUrl, UriKind.Absolute);
        // strip everything after /solr
        var basePath = u.AbsolutePath;
        var idx = basePath.IndexOf("/solr", StringComparison.OrdinalIgnoreCase);
        var prefix = idx >= 0 ? basePath.Substring(0, idx + 5) : "/solr";
        return $"{u.Scheme}://{u.Host}{(u.IsDefaultPort ? "" : ":" + u.Port)}{prefix}";
    }

    private static string Combine(string baseUrl, string tail)
    {
        if (baseUrl.EndsWith("/")) return baseUrl + tail;
        return baseUrl + "/" + tail;
    }

    private static async Task<string> SafeReadAsync(HttpResponseMessage resp)
    {
        try { return await resp.Content.ReadAsStringAsync(); }
        catch { return "<unreadable>"; }
    }
}
