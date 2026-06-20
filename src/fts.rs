use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, STORED, STRING,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer};
use tantivy::{doc, Index, IndexReader, IndexWriter, TantivyDocument, Term};

const TOKENIZER: &str = "dufs_ngram";
const NGRAM_SIZE: usize = 3;
const WRITER_MEMORY_BYTES: usize = 50_000_000;

pub struct FtsIndex {
    writer: IndexWriter,
    reader: IndexReader,
    fields: FtsFields,
}

pub struct FtsSearchResult {
    pub paths: Vec<String>,
    pub total_hits: usize,
}

#[derive(Clone, Copy)]
struct FtsFields {
    path_id: Field,
    name: Field,
    path: Field,
}

impl FtsIndex {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let schema = build_schema();
        let index = match Index::open_in_dir(path) {
            Ok(index) => index,
            Err(_) => Index::create_in_dir(path, schema)?,
        };
        register_tokenizer(&index)?;
        let fields = fields(&index.schema())?;
        let writer = index.writer(WRITER_MEMORY_BYTES)?;
        let reader = index.reader()?;
        Ok(Self {
            writer,
            reader,
            fields,
        })
    }

    pub fn rebuild(&mut self, entries: &[(String, String)]) -> Result<u64> {
        self.writer.delete_all_documents()?;
        for (path, name) in entries {
            self.add_document(path, name)?;
        }
        self.commit()?;
        Ok(entries.len() as u64)
    }

    pub fn upsert(&mut self, path: &str, name: &str) -> Result<()> {
        self.delete(path);
        self.add_document(path, name)
    }

    pub fn delete(&mut self, path: &str) {
        self.writer
            .delete_term(Term::from_field_text(self.fields.path_id, path));
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<FtsSearchResult> {
        let grams = ngrams(query);
        if grams.is_empty() {
            return Ok(FtsSearchResult {
                paths: vec![],
                total_hits: 0,
            });
        }
        let mut must_queries: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(grams.len());
        for gram in grams {
            let name_query: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(self.fields.name, &gram),
                IndexRecordOption::Basic,
            ));
            let path_query: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(self.fields.path, &gram),
                IndexRecordOption::Basic,
            ));
            let field_query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (Occur::Should, name_query),
                (Occur::Should, path_query),
            ]));
            must_queries.push((Occur::Must, field_query));
        }

        let query = BooleanQuery::new(must_queries);
        let searcher = self.reader.searcher();
        let (top_docs, total_hits) =
            searcher.search(&query, &(TopDocs::with_limit(limit), Count))?;
        let mut paths = Vec::with_capacity(top_docs.len());
        for (_, address) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(address)?;
            if let Some(path) = doc
                .get_first(self.fields.path_id)
                .and_then(|value| value.as_str())
            {
                paths.push(path.to_string());
            }
        }
        Ok(FtsSearchResult { paths, total_hits })
    }

    fn add_document(&mut self, path: &str, name: &str) -> Result<()> {
        self.writer.add_document(doc!(
            self.fields.path_id => path,
            self.fields.name => name,
            self.fields.path => path,
        ))?;
        Ok(())
    }
}

pub fn can_accelerate(query: &str) -> bool {
    query.chars().count() >= NGRAM_SIZE && !crate::utils::has_search_wildcard(query)
}

fn build_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("path_id", STRING | STORED);
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER)
        .set_index_option(IndexRecordOption::Basic);
    let options = TextOptions::default().set_indexing_options(indexing);
    schema_builder.add_text_field("name", options.clone());
    schema_builder.add_text_field("path", options);
    schema_builder.build()
}

fn register_tokenizer(index: &Index) -> Result<()> {
    let tokenizer = TextAnalyzer::builder(NgramTokenizer::all_ngrams(NGRAM_SIZE, NGRAM_SIZE)?)
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TOKENIZER, tokenizer);
    Ok(())
}

fn fields(schema: &Schema) -> Result<FtsFields> {
    Ok(FtsFields {
        path_id: schema.get_field("path_id")?,
        name: schema.get_field("name")?,
        path: schema.get_field("path")?,
    })
}

fn ngrams(value: &str) -> Vec<String> {
    let chars: Vec<char> = value.to_lowercase().chars().collect();
    if chars.len() < NGRAM_SIZE {
        return vec![];
    }
    let mut seen = HashSet::new();
    let mut grams = vec![];
    for window in chars.windows(NGRAM_SIZE) {
        let gram: String = window.iter().collect();
        if seen.insert(gram.clone()) {
            grams.push(gram);
        }
    }
    grams
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_accelerate() {
        assert!(can_accelerate("report"));
        assert!(!can_accelerate("ab"));
        assert!(!can_accelerate("*.html"));
    }

    #[test]
    fn test_ngrams() {
        assert_eq!(ngrams("abcd"), ["abc", "bcd"]);
        assert_eq!(ngrams("AbC"), ["abc"]);
        assert!(ngrams("ab").is_empty());
    }

    #[test]
    fn test_search_reports_total_hits() -> Result<()> {
        let tmpdir = assert_fs::TempDir::new()?;
        let mut index = FtsIndex::open(tmpdir.path())?;
        index.rebuild(&[
            ("alpha-one.txt".to_string(), "alpha-one.txt".to_string()),
            ("alpha-two.txt".to_string(), "alpha-two.txt".to_string()),
        ])?;

        let result = index.search("alpha", 1)?;
        assert_eq!(result.paths.len(), 1);
        assert_eq!(result.total_hits, 2);
        Ok(())
    }
}
