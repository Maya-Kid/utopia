//! 推陈述的来源（0054）：请求体就是开放抽取契约，门口拒绝契约之外的键，通过的载荷
//! 整份成一块，抽取按契约解析而**不问模型**——夹具故意不配对话模型，证明这条路不需要它。
//! 连库的部分没有 `UTOPIA_DATABASE_URL` 就跳过（同 documents_routes_tests）。

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;
use utopia_core::models::Proposer;
use utopia_store::documents;
use uuid::Uuid;

/// 门口的校验不连库，任何环境都跑
#[test]
fn the_door_refuses_what_the_contract_has_no_slot_for() {
    let ok = json!({
        "external_id": "obs-1",
        "e": [["cup-7", "cup", true], ["kitchen table", "table", true]],
        "s": [[null, "cup-7", "is on", "kitchen table", null, {}, "08:14:03", null]],
        "n": []
    });
    let (_, content) = super::validate_statements_payload(ok.to_string().as_bytes())
        .expect("a well-formed contract passes");
    let content = content.expect("a non-tombstone yields the document text");
    // 存的是我们重新序列化的那份：没有信封，能被抽取用的同一个解析器读回
    assert!(!content.contains("external_id"));
    let parsed = utopia_extract::open::parse_open_response(&content).unwrap();
    assert_eq!(parsed.statements.len(), 1);
    assert_eq!(parsed.statements[0].phrase, "is on");

    let refuse = |body: Value, needle: &str| {
        let err = super::validate_statements_payload(body.to_string().as_bytes())
            .err()
            .unwrap_or_else(|| panic!("{body} must be refused"));
        assert!(err.contains(needle), "{err:?} should mention {needle:?}");
    };
    // 契约里没有属性的格子：一个 `predicate` 键在门口就拦下，而不是静默忽略
    let mut typed = ok.clone();
    typed["predicate"] = json!("located_in");
    refuse(typed, "unknown key");
    // 引文格必须为空：条目自己就是证据
    let mut quoted = ok.clone();
    quoted["s"][0][0] = json!("cup-7 is on the kitchen table");
    refuse(quoted, "quote");
    // 八格少一格不是截断，是形状错
    let mut short = ok.clone();
    short["s"][0] = json!([null, "cup-7", "is on", "kitchen table"]);
    refuse(short, "eight");
    // 没有身份就没有更新语义
    let mut anon = ok.clone();
    anon["external_id"] = json!("  ");
    refuse(anon, "external_id");
    // 空陈述数组：什么都推不进图，直说
    let mut empty = ok.clone();
    empty["s"] = json!([]);
    refuse(empty, "at least one");
}

struct Fixture {
    pool: sqlx::PgPool,
    state: crate::state::AppState,
    app: axum::Router,
    org: Uuid,
    kb: Uuid,
    source: Uuid,
    api_source: Uuid,
    token: String,
    api_token: String,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> anyhow::Result<Option<Self>> {
        let Some(url) = utopia_store::test_db::url() else {
            return Ok(None);
        };
        let pool = sqlx::PgPool::connect(&url).await?;
        utopia_store::db::migrate(&pool).await?;
        let (org, ws, kb, user, source, api_source) = (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
        );
        // Only locally generated UUIDs are interpolated into fixture SQL.
        sqlx::raw_sql(&format!(
            "INSERT INTO organizations(id,name) VALUES ('{org}','statements-push-test');
             INSERT INTO workspaces(id,org_id,name) VALUES ('{ws}','{org}','statements-push-test');
             INSERT INTO users(id,org_id,email,display_name,password_hash)
                 VALUES ('{user}','{org}','{user}@statements.test','statements-test','unused');
             INSERT INTO knowledge_bases(id,workspace_id,name) VALUES ('{kb}','{ws}','observations');
             INSERT INTO kb_members(kb_id,user_id,role) VALUES ('{kb}','{user}','editor');
             INSERT INTO sources(id,kb_id,kind,name) VALUES
                 ('{source}','{kb}','statements','robot-1'),
                 ('{api_source}','{kb}','api','api-source');"
        ))
        .execute(&pool)
        .await?;
        // 故意**不**配对话模型：这条路不需要它
        let token = super::new_ingest_token();
        utopia_store::sources::set_ingest_token(&pool, source, &token).await?;
        let api_token = super::new_ingest_token();
        utopia_store::sources::set_ingest_token(&pool, api_source, &api_token).await?;
        let dir = tempfile::tempdir()?;
        let cfg = utopia_core::config::AppConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            ..Default::default()
        };
        let search = Arc::new(utopia_search::SearchIndex::open(
            &dir.path().join("search"),
        )?);
        let state = crate::state::AppState::new(pool.clone(), &cfg, search, "test-only".into());
        let app = super::super::router(state.clone(), &cfg);
        Ok(Some(Self {
            pool,
            state,
            app,
            org,
            kb,
            source,
            api_source,
            token,
            api_token,
            _dir: dir,
        }))
    }

    async fn push(
        &self,
        source: Uuid,
        token: &str,
        body: &Value,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/sources/{source}/statements"))
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Content-Type", "application/json")
                    .body(Body::from(body.to_string()))?,
            )
            .await?;
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1 << 20).await?;
        let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Ok((status, value))
    }

    async fn cleanup(self) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM organizations WHERE id = $1")
            .bind(self.org)
            .execute(&self.pool)
            .await?;
        self.pool.close().await;
        Ok(())
    }
}

fn observation(when: &str, place: &str) -> Value {
    json!({
        "external_id": "obs-000412",
        "doc_time": "2026-09-23T08:14:03Z",
        "e": [["cup-7", "cup", true], [place, "table", true]],
        "s": [[null, "cup-7", "is on", place, null, {}, when, null]],
        "n": []
    })
}

/// 推一条陈述，走完处理与抽取，它就是一条开放陈述：有短语、有主宾实体、有证据行
/// （块 = 载荷，偏移为空），而工作区没有任何对话模型
#[tokio::test]
async fn a_pushed_statement_reaches_the_open_graph_without_a_model() -> anyhow::Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let (status, body) = f
        .push(
            f.source,
            &f.token,
            &observation("08:14:03", "kitchen table"),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["action"], "created");

    let doc = documents::find_by_external_key(&f.pool, f.source, "statements:obs-000412")
        .await?
        .expect("the push created a document under its identity");
    assert_eq!(doc.mime, "application/json");

    crate::pipeline::process_document(&f.state, doc.id).await?;
    let chunks: Vec<(String,)> =
        sqlx::query_as("SELECT text FROM chunks WHERE document_id = $1 AND superseded_at IS NULL")
            .bind(doc.id)
            .fetch_all(&f.pool)
            .await?;
    assert_eq!(
        chunks.len(),
        1,
        "the payload is one chunk, not a budgeted split"
    );
    utopia_extract::open::parse_open_response(&chunks[0].0)
        .expect("the chunk is the contract verbatim");

    crate::extraction::extract_document(
        &f.state,
        doc.id,
        Proposer {
            user_id: None,
            token_id: None,
        },
    )
    .await?;
    let (status,): (String,) = sqlx::query_as("SELECT graph_status FROM documents WHERE id = $1")
        .bind(doc.id)
        .fetch_one(&f.pool)
        .await?;
    assert_ne!(status, "failed", "extraction must not need a chat model");

    let facts: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, phrase FROM facts
          WHERE kb_id = $1 AND layer = 'open' AND invalidated_at IS NULL",
    )
    .bind(f.kb)
    .fetch_all(&f.pool)
    .await?;
    assert_eq!(facts.len(), 1, "{facts:?}");
    assert_eq!(facts[0].1, "is on");
    let (chunk_matches, quote_null, offsets_null): (bool, bool, bool) = sqlx::query_as(
        "SELECT chunk_id = (SELECT id FROM chunks WHERE document_id = $2 AND superseded_at IS NULL),
                quote IS NULL, quote_start IS NULL AND quote_end IS NULL
           FROM fact_evidence WHERE fact_id = $1",
    )
    .bind(facts[0].0)
    .bind(doc.id)
    .fetch_one(&f.pool)
    .await?;
    assert!(chunk_matches, "the evidence is the payload's own chunk");
    assert!(
        quote_null && offsets_null,
        "the item is its own evidence: no quote, no offsets"
    );
    let (entities,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM entities
          WHERE kb_id = $1 AND canonical_name IN ('cup-7', 'kitchen table')",
    )
    .bind(f.kb)
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(
        entities, 2,
        "both things are entities with the pushed names"
    );
    f.cleanup().await
}

/// 同一身份再推一份新内容是更新：原地替换并记版本，和 `api` 推送一个语义
#[tokio::test]
async fn a_second_push_under_the_same_identity_updates_in_place() -> anyhow::Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let (status, body) = f
        .push(
            f.source,
            &f.token,
            &observation("08:14:03", "kitchen table"),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = f
        .push(
            f.source,
            &f.token,
            &observation("08:14:03", "kitchen table"),
        )
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["action"], "unchanged", "same content is a no-op");
    let (status, body) = f
        .push(f.source, &f.token, &observation("08:20:00", "counter"))
        .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["action"], "updated");
    let doc = documents::find_by_external_key(&f.pool, f.source, "statements:obs-000412")
        .await?
        .expect("still one document under the identity");
    let (versions,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM document_versions WHERE document_id = $1")
            .bind(doc.id)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(versions, 2, "the update recorded a version");
    f.cleanup().await
}

/// 门口的拒绝走到 HTTP 是 422（`AppError::Validation`，与 `api` 推送被拒时同一个码），
/// 并且这次推送留在 run 历史里；`api` 来源不认这条路由（404），钥匙不对是 401
#[tokio::test]
async fn the_route_answers_422_404_and_401_at_the_door() -> anyhow::Result<()> {
    let Some(f) = Fixture::new().await? else {
        return Ok(());
    };
    let mut typed = observation("08:14:03", "kitchen table");
    typed["class"] = json!("Cup");
    let (status, body) = f.push(f.source, &f.token, &typed).await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (status, _) = f
        .push(
            f.api_source,
            &f.api_token,
            &observation("08:14:03", "kitchen table"),
        )
        .await?;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an api source has no statements route"
    );
    let (status, _) = f
        .push(
            f.source,
            "utp_not-the-key",
            &observation("08:14:03", "kitchen table"),
        )
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    f.cleanup().await
}
