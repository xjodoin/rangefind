use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tantivy::collector::{Count, TopDocs};
use tantivy::doc;
use tantivy::query::{
    BooleanQuery, BoostQuery, FuzzyTermQuery, Occur, Query, QueryParser, TermQuery,
};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TantivyDocument, TextFieldIndexing, TextOptions, Value, FAST,
    INDEXED, STORED, STRING, TEXT,
};
use tantivy::tokenizer::{
    Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer,
};
use tantivy::{Index, IndexWriter, ReloadPolicy, Term};
use unicode_normalization::char::is_combining_mark;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuerySpec {
    set: String,
    label: String,
    q: String,
    expected_title: String,
}

#[derive(Clone, Copy)]
struct Fields {
    id: Field,
    title: Field,
    title_exact: Field,
    title_folded: Field,
    categories: Field,
    body: Field,
    docnum: Field,
}

#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    and_operator: bool,
    exact_title_boost: bool,
    fuzzy_title: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HitRow {
    rank: usize,
    id: String,
    title: String,
    score: f32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryRow {
    set: String,
    label: String,
    q: String,
    expected_title: String,
    rank: usize,
    total_hits: usize,
    total_hits_relation: String,
    ms: f64,
    top: String,
    results: Vec<HitRow>,
}

fn text(value: &JsonValue, key: &str) -> String {
    match value.get(key) {
        Some(JsonValue::String(s)) => s.clone(),
        Some(JsonValue::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        Some(other) if !other.is_null() => other.to_string(),
        _ => String::new(),
    }
}

fn norm(value: &str) -> String {
    value
        .replace('œ', "oe")
        .replace('Œ', "oe")
        .replace('æ', "ae")
        .replace('Æ', "ae")
        .nfkd()
        .filter(|c| !is_combining_mark(*c))
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn word_tokens(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in norm(value).chars() {
        if ch.is_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            if current.len() >= 2 {
                out.push(std::mem::take(&mut current));
            } else {
                current.clear();
            }
        }
    }
    if current.len() >= 2 {
        out.push(current);
    }
    out
}

fn stored_text(doc: &TantivyDocument, field: Field) -> String {
    doc.get_first(field)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}

fn schema() -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let id = builder.add_text_field("id", STRING | STORED);
    let title = builder.add_text_field("title", TEXT | STORED);
    let title_exact = builder.add_text_field("titleExact", STRING);
    let title_folded = builder.add_text_field(
        "titleFolded",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("folded")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );
    let categories = builder.add_text_field("categories", TEXT);
    let body = builder.add_text_field("body", TEXT);
    let docnum = builder.add_u64_field("docnum", INDEXED | FAST);
    let schema = builder.build();
    (
        schema,
        Fields {
            id,
            title,
            title_exact,
            title_folded,
            categories,
            body,
            docnum,
        },
    )
}

fn register_tokenizers(index: &Index) {
    let fr = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(Stemmer::new(Language::French))
        .build();
    let folded = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build();
    index.tokenizers().register("default", fr);
    index.tokenizers().register("folded", folded);
}

fn add_doc(
    writer: &mut IndexWriter,
    fields: Fields,
    value: &JsonValue,
    docnum: u64,
) -> Result<bool> {
    let id = text(value, "id");
    let title = text(value, "title");
    if id.trim().is_empty() || title.trim().is_empty() {
        return Ok(false);
    }
    let categories = text(value, "categories");
    let body = text(value, "body");
    writer.add_document(doc!(
        fields.id => id,
        fields.title => title.clone(),
        fields.title_exact => norm(&title),
        fields.title_folded => norm(&title),
        fields.categories => categories,
        fields.body => body,
        fields.docnum => docnum
    ))?;
    Ok(true)
}

fn build_index(docs_path: &Path, index_path: &Path, force: bool) -> Result<(Index, Fields, u64)> {
    if force && index_path.exists() {
        fs::remove_dir_all(index_path)
            .with_context(|| format!("failed to remove {}", index_path.display()))?;
    }
    let (schema, fields) = schema();
    let index = if index_path.join("meta.json").exists() {
        Index::open_in_dir(index_path)
            .with_context(|| format!("failed to open {}", index_path.display()))?
    } else {
        fs::create_dir_all(index_path)
            .with_context(|| format!("failed to create {}", index_path.display()))?;
        Index::create_in_dir(index_path, schema.clone())
            .with_context(|| format!("failed to create {}", index_path.display()))?
    };
    register_tokenizers(&index);
    if index_path.join("meta.json").exists() && !force {
        let reader = index.reader()?;
        let docs = reader.searcher().num_docs();
        if docs > 0 {
            return Ok((index, fields, docs));
        }
        drop(reader);
        drop(index);
        fs::remove_dir_all(index_path)
            .with_context(|| format!("failed to remove empty {}", index_path.display()))?;
        fs::create_dir_all(index_path)
            .with_context(|| format!("failed to recreate {}", index_path.display()))?;
        let index = Index::create_in_dir(index_path, schema.clone())
            .with_context(|| format!("failed to create {}", index_path.display()))?;
        register_tokenizers(&index);
        return build_fresh_index(index, fields, docs_path);
    }

    build_fresh_index(index, fields, docs_path)
}

fn build_fresh_index(
    index: Index,
    fields: Fields,
    docs_path: &Path,
) -> Result<(Index, Fields, u64)> {
    let input =
        File::open(docs_path).with_context(|| format!("failed to open {}", docs_path.display()))?;
    let mut writer = index.writer(256_000_000)?;
    let mut indexed = 0_u64;
    for line in BufReader::new(input).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(&line)?;
        if add_doc(&mut writer, fields, &value, indexed)? {
            indexed += 1;
        }
        if indexed % 100_000 == 0 {
            eprintln!("tantivy indexed {indexed} docs");
        }
    }
    writer.commit()?;
    writer.wait_merging_threads()?;
    eprintln!("tantivy indexed {indexed} docs total");
    Ok((index, fields, indexed))
}

fn make_parser(index: &Index, fields: Fields, profile: Profile) -> QueryParser {
    let mut parser =
        QueryParser::for_index(index, vec![fields.title, fields.categories, fields.body]);
    parser.set_field_boost(fields.title, 5.5);
    parser.set_field_boost(fields.categories, 2.0);
    parser.set_field_boost(fields.body, 1.0);
    if profile.and_operator {
        parser.set_conjunction_by_default();
    }
    parser
}

fn make_query(
    index: &Index,
    fields: Fields,
    spec: &QuerySpec,
    profile: Profile,
) -> Result<Box<dyn Query>> {
    let parser = make_parser(index, fields, profile);
    let parsed = parser.parse_query_lenient(&spec.q).0;
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Should, parsed)];
    if profile.exact_title_boost {
        let query = TermQuery::new(
            Term::from_field_text(fields.title_exact, &norm(&spec.q)),
            IndexRecordOption::Basic,
        );
        clauses.push((
            Occur::Should,
            Box::new(BoostQuery::new(Box::new(query), 50.0)),
        ));
    }
    if profile.fuzzy_title {
        for token in word_tokens(&spec.q).into_iter().take(8) {
            let query = FuzzyTermQuery::new(
                Term::from_field_text(fields.title_folded, &token),
                if token.len() <= 4 { 1 } else { 2 },
                true,
            );
            clauses.push((
                Occur::Should,
                Box::new(BoostQuery::new(Box::new(query), 8.0)),
            ));
        }
    }
    if clauses.len() == 1 {
        Ok(clauses.pop().unwrap().1)
    } else {
        Ok(Box::new(BooleanQuery::new(clauses)))
    }
}

fn search(
    searcher: &tantivy::Searcher,
    index: &Index,
    fields: Fields,
    spec: &QuerySpec,
    profile: Profile,
    size: usize,
) -> Result<QueryRow> {
    let query = make_query(index, fields, spec, profile)?;
    let start = Instant::now();
    let (top_docs, total_hits) = searcher.search(&*query, &(TopDocs::with_limit(size), Count))?;
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut rank = 0_usize;
    let mut results = Vec::with_capacity(top_docs.len());
    for (idx, (score, addr)) in top_docs.into_iter().enumerate() {
        let doc: TantivyDocument = searcher.doc(addr)?;
        let title = stored_text(&doc, fields.title);
        if rank == 0 && title == spec.expected_title {
            rank = idx + 1;
        }
        results.push(HitRow {
            rank: idx + 1,
            id: stored_text(&doc, fields.id),
            title,
            score,
        });
    }
    Ok(QueryRow {
        set: spec.set.clone(),
        label: spec.label.clone(),
        q: spec.q.clone(),
        expected_title: spec.expected_title.clone(),
        rank,
        total_hits,
        total_hits_relation: "EQUAL_TO".to_string(),
        ms,
        top: results
            .first()
            .map(|row| row.title.clone())
            .unwrap_or_default(),
        results,
    })
}

fn metrics(rows: &[QueryRow]) -> serde_json::Value {
    let n = rows.len() as f64;
    let denom = if n == 0.0 { 1.0 } else { n };
    let hit1 = rows.iter().filter(|row| row.rank == 1).count() as f64 / denom;
    let hit3 = rows
        .iter()
        .filter(|row| row.rank > 0 && row.rank <= 3)
        .count() as f64
        / denom;
    let hit10 = rows
        .iter()
        .filter(|row| row.rank > 0 && row.rank <= 10)
        .count() as f64
        / denom;
    let mrr10 = rows
        .iter()
        .map(|row| {
            if row.rank > 0 && row.rank <= 10 {
                1.0 / row.rank as f64
            } else {
                0.0
            }
        })
        .sum::<f64>()
        / denom;
    json!({
        "n": rows.len(),
        "hit1": hit1,
        "hit3": hit3,
        "hit10": hit10,
        "mrr10": mrr10
    })
}

fn report_for_profile(rows: Vec<QueryRow>) -> serde_json::Value {
    let known: Vec<QueryRow> = rows
        .iter()
        .filter(|row| row.set == "known")
        .cloned()
        .collect();
    let typo: Vec<QueryRow> = rows
        .iter()
        .filter(|row| row.set == "typo")
        .cloned()
        .collect();
    json!({
        "known": { "metrics": metrics(&known), "rows": known },
        "typo": { "metrics": metrics(&typo), "rows": typo }
    })
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        anyhow::bail!("Usage: <docs.jsonl> <indexDir> <queries.json> <out.json> <size> [--force]");
    }
    let docs_path = PathBuf::from(&args[1]);
    let index_path = PathBuf::from(&args[2]);
    let queries_path = PathBuf::from(&args[3]);
    let out_path = PathBuf::from(&args[4]);
    let size = args[5].parse::<usize>()?;
    let force = args.iter().skip(6).any(|arg| arg == "--force");

    let build_start = Instant::now();
    let (index, fields, indexed_docs) = build_index(&docs_path, &index_path, force)?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let queries: Vec<QuerySpec> = serde_json::from_reader(File::open(&queries_path)?)?;
    let profiles = [
        Profile {
            name: "tantivy_bm25_or",
            and_operator: false,
            exact_title_boost: false,
            fuzzy_title: false,
        },
        Profile {
            name: "tantivy_bm25_and",
            and_operator: true,
            exact_title_boost: false,
            fuzzy_title: false,
        },
        Profile {
            name: "tantivy_title_boost",
            and_operator: false,
            exact_title_boost: true,
            fuzzy_title: false,
        },
        Profile {
            name: "tantivy_title_fuzzy",
            and_operator: false,
            exact_title_boost: true,
            fuzzy_title: true,
        },
    ];

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    reader.reload()?;
    let searcher = reader.searcher();
    let mut profile_reports = BTreeMap::new();
    for profile in profiles {
        let mut rows = Vec::new();
        for spec in &queries {
            rows.push(search(&searcher, &index, fields, spec, profile, size)?);
        }
        profile_reports.insert(profile.name, report_for_profile(rows));
    }

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let report = json!({
        "engine": "tantivy",
        "docs": docs_path,
        "index": index_path,
        "size": size,
        "buildOrOpenMs": build_ms,
        "documents": indexed_docs,
        "profiles": profile_reports
    });
    serde_json::to_writer_pretty(File::create(out_path)?, &report)?;
    Ok(())
}
