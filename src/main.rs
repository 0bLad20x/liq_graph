mod core;
mod orchestration;
mod workers;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    orchestration::run().await
}
