use anyhow::Result;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

use crate::analyzer::TextAnalyzer;
use crate::data_models::InvertedIndexDoc;
use crate::db::Database;
use crate::db::collections;

pub struct QueryEngine {
    db: Database,
    analyzer: TextAnalyzer,
}

impl QueryEngine {
    pub fn new(db: Database, analyzer: TextAnalyzer) -> Self {
        Self { db, analyzer }
    }

    pub async fn query(&self, query: &str) -> Result<Vec<ObjectId>> {
        // Analyze the query text using the same pipeline as documents
        let terms = self.analyzer.analyze(query.to_string())?;

        // TODO: Implement actual query logic using the inverted index
        let i_index = self.db.collection::<InvertedIndexDoc>(collections::INDEX);
        let filter = doc! { "term": {
            "$in": terms
        }};
        let _index_docs = i_index.find(filter).await?;

        unimplemented!()
    }
}
