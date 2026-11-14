#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = ticket_rust::connect().await?;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("listening on http://localhost:3000");
    axum::serve(listener, ticket_rust::app(state)).await?;
    Ok(())
}
