use anyhow::{Context, Result};
use mongodb::{
    Client, Collection, Database as MongoDatabase,
    bson::{Document, doc, oid::ObjectId},
    options::ClientOptions,
};
use once_cell::sync::OnceCell;
use serde::{Serialize, de::DeserializeOwned};

use crate::config::CONFIG;
use crate::data_models::CrawlResult;

/// Global database instance
static DB: OnceCell<Database> = OnceCell::new();

/// Collection names as constants for consistency
pub mod collections {
    pub const CRAWL_RESULTS: &str = "crawl_results";
    // Add more collection names here as your project grows
    // pub const USERS: &str = "users";
}

/// Main database wrapper providing connection management and collection access
#[derive(Debug, Clone)]
pub struct Database {
    client: Client,
    db: MongoDatabase,
}

impl Database {
    /// Create a new Database instance with custom URI and database name.
    /// Useful for testing with a different database.
    pub async fn new(uri: &str, db_name: &str) -> Result<Self> {
        let client_options = ClientOptions::parse(uri)
            .await
            .context("Failed to parse MongoDB connection string")?;

        let client =
            Client::with_options(client_options).context("Failed to create MongoDB client")?;

        // Ping the database to verify connection
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .context("Failed to connect to MongoDB")?;

        log::info!("Connected to MongoDB database: {}", db_name);

        let db = client.database(db_name);

        Ok(Self { client, db })
    }

    /// Create a Database instance using environment configuration
    pub async fn from_config() -> Result<Self> {
        Self::new(&CONFIG.mongo_uri, &CONFIG.mongo_db_name).await
    }

    /// Initialize the global database instance.
    /// Call this once at application startup.
    pub async fn init_global() -> Result<&'static Database> {
        let db = Self::from_config().await?;
        DB.set(db)
            .map_err(|_| anyhow::anyhow!("Database already initialized"))?;
        Ok(DB.get().unwrap())
    }

    /// Initialize global database with custom settings (useful for tests)
    pub async fn init_global_with(uri: &str, db_name: &str) -> Result<&'static Database> {
        let db = Self::new(uri, db_name).await?;
        DB.set(db)
            .map_err(|_| anyhow::anyhow!("Database already initialized"))?;
        Ok(DB.get().unwrap())
    }

    /// Get the global database instance.
    /// Panics if database hasn't been initialized.
    pub fn get() -> &'static Database {
        DB.get()
            .expect("Database not initialized. Call Database::init_global() first.")
    }

    /// Get a typed collection by name
    pub fn collection<T>(&self, name: &str) -> Collection<T>
    where
        T: Send + Sync,
    {
        self.db.collection(name)
    }

    /// Get the underlying MongoDB client (for advanced operations)
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the underlying MongoDB database (for advanced operations)
    pub fn database(&self) -> &MongoDatabase {
        &self.db
    }

    // =========================================================================
    // Collection accessors - add typed accessors for each collection
    // =========================================================================

    /// Get the crawl_results collection
    pub fn crawl_results(&self) -> Collection<CrawlResult> {
        self.collection(collections::CRAWL_RESULTS)
    }

    // Add more collection accessors as needed:
    // pub fn users(&self) -> Collection<User> {
    //     self.collection(collections::USERS)
    // }
}

// =============================================================================
// Generic CRUD operations
// =============================================================================

/// Generic repository trait for common CRUD operations.
/// Implement this for specific collections or use the generic functions below.
pub struct Repository<T>
where
    T: Send + Sync,
{
    collection: Collection<T>,
}

impl<T> Repository<T>
where
    T: Serialize + DeserializeOwned + Unpin + Send + Sync,
{
    pub fn new(collection: Collection<T>) -> Self {
        Self { collection }
    }

    /// Insert a single document
    pub async fn insert(&self, doc: &T) -> Result<ObjectId> {
        let result = self
            .collection
            .insert_one(doc)
            .await
            .context("Failed to insert document")?;

        result
            .inserted_id
            .as_object_id()
            .ok_or_else(|| anyhow::anyhow!("Failed to get inserted ObjectId"))
    }

    /// Insert multiple documents
    pub async fn insert_many(&self, docs: &[T]) -> Result<Vec<ObjectId>> {
        let result = self
            .collection
            .insert_many(docs)
            .await
            .context("Failed to insert documents")?;

        Ok(result
            .inserted_ids
            .values()
            .filter_map(|id| id.as_object_id())
            .collect())
    }

    /// Find a document by ObjectId
    pub async fn find_by_id(&self, id: ObjectId) -> Result<Option<T>> {
        let filter = doc! { "_id": id };
        self.collection
            .find_one(filter)
            .await
            .context("Failed to find document by id")
    }

    /// Find a single document matching a filter
    pub async fn find_one(&self, filter: Document) -> Result<Option<T>> {
        self.collection
            .find_one(filter)
            .await
            .context("Failed to find document")
    }

    /// Find all documents matching a filter
    pub async fn find(&self, filter: Document) -> Result<Vec<T>> {
        use futures::TryStreamExt;

        let cursor = self
            .collection
            .find(filter)
            .await
            .context("Failed to execute find query")?;

        cursor
            .try_collect()
            .await
            .context("Failed to collect results")
    }

    /// Find all documents in the collection
    pub async fn find_all(&self) -> Result<Vec<T>> {
        self.find(doc! {}).await
    }

    /// Update a document by ObjectId
    pub async fn update_by_id(&self, id: ObjectId, update: Document) -> Result<bool> {
        let filter = doc! { "_id": id };
        let result = self
            .collection
            .update_one(filter, doc! { "$set": update })
            .await
            .context("Failed to update document")?;

        Ok(result.modified_count > 0)
    }

    /// Update multiple documents matching a filter
    pub async fn update_many(&self, filter: Document, update: Document) -> Result<u64> {
        let result = self
            .collection
            .update_many(filter, doc! { "$set": update })
            .await
            .context("Failed to update documents")?;

        Ok(result.modified_count)
    }

    /// Delete a document by ObjectId
    pub async fn delete_by_id(&self, id: ObjectId) -> Result<bool> {
        let filter = doc! { "_id": id };
        let result = self
            .collection
            .delete_one(filter)
            .await
            .context("Failed to delete document")?;

        Ok(result.deleted_count > 0)
    }

    /// Delete multiple documents matching a filter
    pub async fn delete_many(&self, filter: Document) -> Result<u64> {
        let result = self
            .collection
            .delete_many(filter)
            .await
            .context("Failed to delete documents")?;

        Ok(result.deleted_count)
    }

    /// Count documents matching a filter
    pub async fn count(&self, filter: Document) -> Result<u64> {
        self.collection
            .count_documents(filter)
            .await
            .context("Failed to count documents")
    }

    /// Check if a document exists
    pub async fn exists(&self, filter: Document) -> Result<bool> {
        Ok(self.count(filter).await? > 0)
    }
}

// =============================================================================
// Convenience functions for CrawlResult collection
// =============================================================================

impl Database {
    /// Get a repository for CrawlResult documents
    pub fn crawl_results_repo(&self) -> Repository<CrawlResult> {
        Repository::new(self.crawl_results())
    }
}

// =============================================================================
// CrawlResult-specific operations
// =============================================================================

/// Extended operations specific to CrawlResult collection
pub struct CrawlResultRepo {
    repo: Repository<CrawlResult>,
}

impl CrawlResultRepo {
    pub fn new(db: &Database) -> Self {
        Self {
            repo: db.crawl_results_repo(),
        }
    }

    /// Insert a new crawl result
    pub async fn insert(&self, result: &CrawlResult) -> Result<ObjectId> {
        self.repo.insert(result).await
    }

    pub async fn insert_many(&self, results: &[CrawlResult]) -> Result<Vec<ObjectId>> {
        self.repo.insert_many(results).await
    }

    /// Find by URL
    pub async fn find_by_url(&self, url: &str) -> Result<Option<CrawlResult>> {
        self.repo.find_one(doc! { "url": url }).await
    }

    /// Check if URL has been crawled
    pub async fn url_exists(&self, url: &str) -> Result<bool> {
        self.repo.exists(doc! { "url": url }).await
    }

    /// Find all seed URLs
    pub async fn find_seeds(&self) -> Result<Vec<CrawlResult>> {
        self.repo.find(doc! { "is_seed": true }).await
    }

    /// Find by depth
    pub async fn find_by_depth(&self, depth: u32) -> Result<Vec<CrawlResult>> {
        self.repo.find(doc! { "depth": depth }).await
    }

    /// Delete by URL
    pub async fn delete_by_url(&self, url: &str) -> Result<bool> {
        let result = self
            .repo
            .collection
            .delete_one(doc! { "url": url })
            .await
            .context("Failed to delete by URL")?;
        Ok(result.deleted_count > 0)
    }

    /// List all crawl results
    pub async fn list_all(&self) -> Result<Vec<CrawlResult>> {
        self.repo.find_all().await
    }

    /// Find by ID
    pub async fn find_by_id(&self, id: ObjectId) -> Result<Option<CrawlResult>> {
        self.repo.find_by_id(id).await
    }

    /// Delete by ID
    pub async fn delete_by_id(&self, id: ObjectId) -> Result<bool> {
        self.repo.delete_by_id(id).await
    }

    /// Update crawl result
    pub async fn update(&self, id: ObjectId, update: Document) -> Result<bool> {
        self.repo.update_by_id(id, update).await
    }
}

// =============================================================================
// Test utilities
// =============================================================================

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Create a unique test database name
    pub fn unique_test_db_name() -> String {
        let count = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("harvest_test_{}_{}", timestamp, count)
    }

    /// Create a test database instance.
    /// Uses MONGO_URI from environment but creates a unique test database.
    pub async fn create_test_db() -> Result<(Database, String)> {
        dotenvy::dotenv().ok();
        let uri =
            std::env::var("MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
        let db_name = unique_test_db_name();
        let db = Database::new(&uri, &db_name).await?;
        Ok((db, db_name))
    }

    /// Clean up a test database by dropping it
    pub async fn cleanup_test_db(db: &Database, db_name: &str) -> Result<()> {
        db.client()
            .database(db_name)
            .drop()
            .await
            .context("Failed to drop test database")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::*;

    #[tokio::test]
    async fn test_crawl_result_crud() -> Result<()> {
        let (db, db_name) = create_test_db().await?;
        let repo = CrawlResultRepo::new(&db);

        // Create
        let result = CrawlResult::new(
            "https://example.com".to_string(),
            "Example".to_string(),
            "<html></html>".to_string(),
            vec!["https://example.com/page1".to_string()],
            0,
            true,
        );

        let id = repo.insert(&result).await?;

        // Read
        let found = repo.find_by_id(id).await?;
        assert!(found.is_some());
        assert_eq!(found.unwrap().url, "https://example.com");

        // Find by URL
        let by_url = repo.find_by_url("https://example.com").await?;
        assert!(by_url.is_some());

        // URL exists
        assert!(repo.url_exists("https://example.com").await?);
        assert!(!repo.url_exists("https://notexists.com").await?);

        // Update
        let updated = repo.update(id, doc! { "title": "Updated Title" }).await?;
        assert!(updated);

        let found = repo.find_by_id(id).await?;
        assert_eq!(found.unwrap().title, "Updated Title");

        // Delete
        let deleted = repo.delete_by_id(id).await?;
        assert!(deleted);

        let found = repo.find_by_id(id).await?;
        assert!(found.is_none());

        // Cleanup
        cleanup_test_db(&db, &db_name).await?;

        Ok(())
    }
}
