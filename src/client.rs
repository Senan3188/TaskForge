use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use axum::body::Bytes;
use axum::http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{Client as HttpClient, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::protocol::{Assignment, JobStatus, Poll, SubmitJob};

/// An HTTP client for a TaskForge master.
#[derive(Clone)]
pub struct Client {
    master: String,
    http: HttpClient<HttpConnector, Full<Bytes>>,
}

impl Client {
    /// Takes a master URL such as `http://127.0.0.1:7878`.
    pub fn new(master: &str) -> Result<Self> {
        let uri: Uri = master.parse().context("invalid master URL")?;
        ensure!(
            uri.scheme_str() == Some("http")
                && uri.authority().is_some()
                && uri.path() == "/"
                && uri.query().is_none(),
            "master URL must be an HTTP origin, e.g. http://127.0.0.1:7878"
        );
        Ok(Self {
            master: master.trim_end_matches('/').to_owned(),
            http: HttpClient::builder(TokioExecutor::new()).build_http(),
        })
    }

    pub async fn submit(&self, job: &SubmitJob) -> Result<JobStatus> {
        self.request(Method::POST, "/v1/jobs", Some(job)).await
    }

    pub async fn status(&self, id: Uuid) -> Result<JobStatus> {
        self.request(Method::GET, &format!("/v1/jobs/{id}"), None::<&()>)
            .await
    }

    pub(crate) async fn poll(&self, worker_id: Uuid, poll: &Poll) -> Result<Vec<Assignment>> {
        self.request(
            Method::POST,
            &format!("/v1/workers/{worker_id}/poll"),
            Some(poll),
        )
        .await
    }

    async fn request<T: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&T>,
    ) -> Result<R> {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.master));
        let bytes = match body {
            Some(body) => {
                request = request.header("content-type", "application/json");
                Bytes::from(serde_json::to_vec(body)?)
            }
            None => Bytes::new(),
        };
        let request = request.body(Full::new(bytes))?;

        tokio::time::timeout(Duration::from_secs(5), async {
            let response = self
                .http
                .request(request)
                .await
                .context("request to master failed")?;
            let status = response.status();
            let bytes = response.into_body().collect().await?.to_bytes();
            if !status.is_success() {
                return Err(anyhow!(
                    "master returned {status}: {}",
                    String::from_utf8_lossy(&bytes)
                ));
            }
            serde_json::from_slice(&bytes).context("invalid response from master")
        })
        .await
        .context("request to master timed out")?
    }
}
