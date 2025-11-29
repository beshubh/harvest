use dotenvy::dotenv;
use once_cell::sync::Lazy;
use std::env;

pub static CONFIG: Lazy<Config> = Lazy::new(|| {
    dotenv().ok(); // Load .env file if present
    Config {
        mongo_uri: get_env("MONGO_URI"),
        mongo_db_name: get_env_or_default("MONGO_DB_NAME", "harvest"),
    }
});

pub struct Config {
    pub mongo_uri: String,
    pub mongo_db_name: String,
}

fn get_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Missing required environment variable: {key}"))
}

fn get_env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
