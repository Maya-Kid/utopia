//! Reproduction only: passing asserts the current in-flight version gap.
//! This is not a regression contract and is not intended to merge as-is.
use super::*;
use axum::{extract::State, response::IntoResponse, routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Model {
    replies: Arc<Vec<Value>>,
    requests: Arc<Mutex<Vec<Value>>>,
    edit: bool,
    pool: sqlx::PgPool,
    class: Uuid,
}
async fn reply(State(m): State<Model>, Json(body): Json<Value>) -> impl IntoResponse {
    let n = {
        let mut seen = m.requests.lock().unwrap();
        seen.push(body);
        seen.len() - 1
    };
    // This write commits after the worker has sent its old prompt, before any response.
    if m.edit && n == 0 {
        sqlx::query("UPDATE entity_types SET description='NEW definition', updated_at=clock_timestamp() WHERE id=$1")
            .bind(m.class).execute(&m.pool).await.unwrap();
    }
    let text = m
        .replies
        .get(n)
        .expect("unexpected model request")
        .to_string();
    let frame = json!({"choices":[{"delta":{"content":text}}]});
    (
        [("content-type", "text/event-stream")],
        format!("data: {frame}\n\ndata: [DONE]\n\n"),
    )
}
struct Fx {
    pool: sqlx::PgPool,
    state: AppState,
    org: Uuid,
    kb: Uuid,
    class: Uuid,
    model: Model,
    server: tokio::task::JoinHandle<()>,
    dir: tempfile::TempDir,
}
impl Fx {
    async fn new(replies: Vec<Value>, edit: bool) -> anyhow::Result<Option<Self>> {
        let Some(url) = utopia_store::test_db::url() else {
            return Ok(None);
        };
        let pool = sqlx::PgPool::connect(&url).await?;
        utopia_store::db::migrate(&pool).await?;
        let (org, ws, kb, class, entity) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
        );
        sqlx::query("INSERT INTO organizations(id,name) VALUES($1,'alignment-audit')")
            .bind(org)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO workspaces(id,org_id,name) VALUES($1,$2,'alignment-audit')")
            .bind(ws)
            .bind(org)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO knowledge_bases(id,workspace_id,name) VALUES($1,$2,'alignment-audit')",
        )
        .bind(kb)
        .bind(ws)
        .execute(&pool)
        .await?;
        sqlx::query("INSERT INTO entity_types(id,kb_id,key,label,description) VALUES($1,$2,'organization','Organization','OLD definition')").bind(class).bind(kb).execute(&pool).await?;
        sqlx::query("INSERT INTO entities(id,kb_id,canonical_name,specific_type) VALUES($1,$2,'Acme','company')").bind(entity).bind(kb).execute(&pool).await?;
        let model = Model {
            replies: Arc::new(replies),
            requests: Arc::new(Mutex::new(Vec::new())),
            edit,
            pool: pool.clone(),
            class,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let router = Router::new()
            .route("/chat/completions", post(reply))
            .with_state(model.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        utopia_store::settings::upsert(
            &pool,
            ws,
            Some(&endpoint),
            None,
            Some("scripted"),
            None,
            None,
            None,
            None,
        )
        .await?;
        let dir = tempfile::tempdir()?;
        let cfg = utopia_core::config::AppConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..Default::default()
        };
        let search = Arc::new(utopia_search::SearchIndex::open(
            &dir.path().join("search"),
        )?);
        let state = AppState::new(pool.clone(), &cfg, search, "test-only".into());
        Ok(Some(Self {
            pool,
            state,
            org,
            kb,
            class,
            model,
            server,
            dir,
        }))
    }
    async fn run(&self) -> anyhow::Result<()> {
        align_types(&self.state, self.kb).await
    }
    fn requests(&self) -> Vec<Value> {
        self.model.requests.lock().unwrap().clone()
    }
    async fn cleanup(self) -> anyhow::Result<()> {
        self.server.abort();
        sqlx::query("DELETE FROM jobs WHERE payload->>'kb_id'=$1")
            .bind(self.kb.to_string())
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM organizations WHERE id=$1")
            .bind(self.org)
            .execute(&self.pool)
            .await?;
        drop(self.state);
        self.dir.close()?;
        Ok(())
    }
}
fn vote(class: Option<&str>) -> Value {
    json!({"b":[[0,class]]})
}

#[tokio::test]
async fn definition_edited_during_model_call_is_treated_as_fresh() -> anyhow::Result<()> {
    let Some(f) = Fx::new(vec![vote(None), vote(None)], true).await? else {
        return Ok(());
    };
    f.run().await?;
    let requests = f.requests();
    assert_eq!(requests.len(), 2);
    for request in requests {
        let text = request.to_string();
        assert!(text.contains("OLD definition"));
        assert!(!text.contains("NEW definition"));
    }
    let timestamps:bool=sqlx::query_scalar("SELECT t.updated_at < b.decided_at FROM entity_types t JOIN type_bindings b ON b.kb_id=t.kb_id WHERE t.id=$1").bind(f.class).fetch_one(&f.pool).await?;
    assert!(
        timestamps,
        "definition edit completed before the old response was committed"
    );
    assert!(
        type_bindings::stale(&f.pool, f.kb).await?.is_empty(),
        "observed limitation: old input is treated as current"
    );
    f.run().await?;
    assert_eq!(f.requests().len(), 2);
    f.cleanup().await
}
