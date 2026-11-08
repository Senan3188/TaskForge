use axum::http::header::CONTENT_TYPE;
use axum::response::Html;

pub async fn page() -> Html<&'static str> {
    Html(include_str!("../dashboard/index.html"))
}

pub async fn styles() -> ([(axum::http::HeaderName, &'static str); 1], &'static str) {
    (
        [(CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../dashboard/dashboard.css"),
    )
}

pub async fn script() -> ([(axum::http::HeaderName, &'static str); 1], &'static str) {
    (
        [(CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../dashboard/dashboard.js"),
    )
}
