package rangefind.bench;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

public final class LuceneFrwikiQuality {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final class QuerySpec {
    public String set;
    public String label;
    public String q;
    public String expectedTitle;
  }

  private record Profile(String name, boolean andOperator, boolean exactTitleBoost) {}

  private static String text(Map<String, Object> doc, String key) {
    Object value = doc.get(key);
    return value == null ? "" : String.valueOf(value);
  }

  private static String norm(String value) {
    return Normalizer.normalize(String.valueOf(value == null ? "" : value), Normalizer.Form.NFKD)
      .replaceAll("\\p{M}+", "")
      .replace('œ', 'o')
      .replace('æ', 'a')
      .toLowerCase(Locale.ROOT)
      .trim();
  }

  private static void buildIndex(Path docsPath, Path indexPath, boolean force) throws Exception {
    Path marker = indexPath.resolve("segments.gen");
    if (!force && Files.exists(marker)) return;
    Files.createDirectories(indexPath);
    try (Analyzer analyzer = new FrenchAnalyzer();
         IndexWriter writer = new IndexWriter(FSDirectory.open(indexPath), new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
      JsonFactory factory = new JsonFactory();
      long indexed = 0;
      try (BufferedReader reader = Files.newBufferedReader(docsPath, StandardCharsets.UTF_8)) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isBlank()) continue;
          Map<String, Object> item;
          try (JsonParser parser = factory.createParser(line)) {
            item = MAPPER.readValue(parser, new TypeReference<>() {});
          }
          String id = text(item, "id");
          String title = text(item, "title");
          if (id.isBlank() || title.isBlank()) continue;
          Document doc = new Document();
          doc.add(new StringField("id", id, Field.Store.YES));
          doc.add(new StoredField("title", title));
          doc.add(new StoredField("url", text(item, "url")));
          doc.add(new StringField("titleExact", norm(title), Field.Store.NO));
          doc.add(new TextField("title", title, Field.Store.NO));
          doc.add(new TextField("categories", text(item, "categories"), Field.Store.NO));
          doc.add(new TextField("body", text(item, "body"), Field.Store.NO));
          writer.addDocument(doc);
          indexed++;
          if (indexed % 100000 == 0) System.err.printf("lucene indexed %,d docs%n", indexed);
        }
      }
      writer.commit();
      System.err.printf("lucene indexed %,d docs total%n", indexed);
    }
  }

  private static Query makeQuery(QuerySpec spec, Profile profile, Analyzer analyzer) throws Exception {
    Map<String, Float> boosts = new HashMap<>();
    boosts.put("title", 5.5f);
    boosts.put("categories", 2.0f);
    boosts.put("body", 1.0f);
    MultiFieldQueryParser parser = new MultiFieldQueryParser(new String[] { "title", "categories", "body" }, analyzer, boosts);
    parser.setDefaultOperator(profile.andOperator ? QueryParser.Operator.AND : QueryParser.Operator.OR);
    Query parsed = parser.parse(QueryParser.escape(spec.q));
    if (!profile.exactTitleBoost) return parsed;
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(parsed, BooleanClause.Occur.SHOULD);
    builder.add(new BoostQuery(new TermQuery(new Term("titleExact", norm(spec.q))), 50.0f), BooleanClause.Occur.SHOULD);
    return builder.build();
  }

  private static Map<String, Object> search(IndexSearcher searcher, Analyzer analyzer, QuerySpec spec, Profile profile, int size) throws Exception {
    long start = System.nanoTime();
    TopDocs hits = searcher.search(makeQuery(spec, profile, analyzer), size);
    double ms = (System.nanoTime() - start) / 1_000_000.0;
    List<Map<String, Object>> results = new ArrayList<>();
    int rank = 0;
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      ScoreDoc scoreDoc = hits.scoreDocs[i];
      Document doc = searcher.storedFields().document(scoreDoc.doc);
      String title = doc.get("title");
      if (rank == 0 && title.equals(spec.expectedTitle)) rank = i + 1;
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("rank", i + 1);
      row.put("id", doc.get("id"));
      row.put("title", title);
      row.put("score", scoreDoc.score);
      results.add(row);
    }
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("set", spec.set);
    row.put("label", spec.label);
    row.put("q", spec.q);
    row.put("expectedTitle", spec.expectedTitle);
    row.put("rank", rank);
    row.put("totalHits", hits.totalHits.value);
    row.put("totalHitsRelation", hits.totalHits.relation.toString());
    row.put("ms", ms);
    row.put("top", results.isEmpty() ? "" : results.get(0).get("title"));
    row.put("results", results);
    return row;
  }

  private static Map<String, Object> metrics(List<Map<String, Object>> rows) {
    int n = rows.size();
    double hit1 = 0;
    double hit3 = 0;
    double hit10 = 0;
    double mrr10 = 0;
    for (Map<String, Object> row : rows) {
      int rank = ((Number) row.get("rank")).intValue();
      if (rank == 1) hit1++;
      if (rank > 0 && rank <= 3) hit3++;
      if (rank > 0 && rank <= 10) hit10++;
      if (rank > 0 && rank <= 10) mrr10 += 1.0 / rank;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("n", n);
    out.put("hit1", n == 0 ? 0 : hit1 / n);
    out.put("hit3", n == 0 ? 0 : hit3 / n);
    out.put("hit10", n == 0 ? 0 : hit10 / n);
    out.put("mrr10", n == 0 ? 0 : mrr10 / n);
    return out;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      throw new IllegalArgumentException("Usage: <docs.jsonl> <indexDir> <queries.json> <out.json> <size> [--force]");
    }
    Path docsPath = Path.of(args[0]);
    Path indexPath = Path.of(args[1]);
    Path queriesPath = Path.of(args[2]);
    Path outPath = Path.of(args[3]);
    int size = Integer.parseInt(args[4]);
    boolean force = args.length > 5 && "--force".equals(args[5]);

    long buildStart = System.nanoTime();
    buildIndex(docsPath, indexPath, force);
    double buildMs = (System.nanoTime() - buildStart) / 1_000_000.0;

    List<QuerySpec> queries = MAPPER.readValue(queriesPath.toFile(), new TypeReference<>() {});
    List<Profile> profiles = List.of(
      new Profile("lucene_bm25_or", false, false),
      new Profile("lucene_bm25_and", true, false),
      new Profile("lucene_title_boost", false, true)
    );

    Map<String, Object> report = new LinkedHashMap<>();
    report.put("engine", "lucene");
    report.put("docs", docsPath.toString());
    report.put("index", indexPath.toString());
    report.put("size", size);
    report.put("buildOrOpenMs", buildMs);
    Map<String, Object> profileReports = new LinkedHashMap<>();
    try (Analyzer analyzer = new FrenchAnalyzer();
         DirectoryReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
      IndexSearcher searcher = new IndexSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());
      report.put("documents", reader.numDocs());
      for (Profile profile : profiles) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (QuerySpec spec : queries) rows.add(search(searcher, analyzer, spec, profile, size));
        Map<String, Object> bySet = new LinkedHashMap<>();
        for (String set : List.of("known", "typo")) {
          List<Map<String, Object>> setRows = rows.stream().filter(row -> set.equals(row.get("set"))).toList();
          Map<String, Object> setReport = new LinkedHashMap<>();
          setReport.put("metrics", metrics(setRows));
          setReport.put("rows", setRows);
          bySet.put(set, setReport);
        }
        profileReports.put(profile.name, bySet);
      }
    }
    report.put("profiles", profileReports);
    Files.createDirectories(outPath.getParent());
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(outPath.toFile(), report);
  }
}
