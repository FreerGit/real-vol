use std::f64;

use axum::{response::Html, routing::get, Router};
use chrono::{serde::ts_milliseconds, DateTime, Utc};
use maud::{html, Markup, PreEscaped};
use reqwest::Error;
use serde::{de, Deserialize, Deserializer};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;

#[derive(Debug, Deserialize)]
struct BybitResponse {
    retCode: i32,
    retMsg: String,
    result: KlinesForTicker,
}

#[derive(Debug, Deserialize)]
struct KlinesForTicker {
    symbol: String,
    category: String,
    list: Vec<CandleData>,
}

fn parse_f64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(s.parse::<f64>().map_err(de::Error::custom)?)
}
fn parse_datetime<'de, D: Deserializer<'de>>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let timestamp_num = s.parse::<i64>().map_err(de::Error::custom)?;
    Ok(chrono::DateTime::from_timestamp_millis(timestamp_num).unwrap())
}

#[derive(Debug, Deserialize, Clone)]
struct CandleData {
    #[serde(deserialize_with = "parse_datetime")]
    start: DateTime<Utc>,
    #[serde(deserialize_with = "parse_f64")]
    open: f64,
    #[serde(deserialize_with = "parse_f64")]
    high: f64,
    #[serde(deserialize_with = "parse_f64")]
    low: f64,
    #[serde(deserialize_with = "parse_f64")]
    close: f64,
    volume: String,
    turnover: String,
}

const BYBIT_URL: &str = "https://api.bybit.com/v5/market/kline";

async fn fetch_ohlc(symbol: &str, interval: &str, limit: usize) -> Result<Vec<CandleData>, Error> {
    let url = format!(
        "{}?symbol={}&interval={}&limit={}",
        BYBIT_URL, symbol, interval, limit
    );

    let body = reqwest::get(&url).await?.text().await?;
    let response = serde_json::from_str::<BybitResponse>(&body.as_str()).unwrap();

    assert!(response.retCode == 0);
    assert!(response.retMsg == "OK".to_string());

    Ok(response.result.list)
}

fn calculate_parkinson(klines: Vec<CandleData>) -> f64 {
    let sum: f64 = klines
        .iter()
        .map(|k| (k.high.ln() - k.low.ln()).powi(2))
        .sum();

    let coefficient = 1.0 / (4.0 * klines.len() as f64 * f64::consts::LN_2);
    let time_series_vol = (coefficient * sum).sqrt();
    time_series_vol * 365.25_f64.sqrt()
}

async fn fetch_rolling_volatility() -> Html<String> {
    let data = fetch_ohlc("BTCUSDT", "D", 365).await.unwrap();
    let rolling_vol: Vec<(i64, f64)> = data
        .windows(7)
        .map(|window| {
            let vol = calculate_parkinson(window.to_vec());
            (window.last().unwrap().start.timestamp_millis(), vol)
        })
        .collect();
    Html(serde_json::to_string(&rolling_vol).unwrap())
}

async fn serve_html() -> Html<String> {
    let script = PreEscaped(
        r#"
        function plotData() {
            fetch('/rolling_volatility')
                .then(response => response.json())
                .then(data => {
                    let timestamps = data.map(d => d[0] / 1000);
                    let volatilities = data.map(d => d[1] * 100);
                    let opts = {
                        title: '7-Day Rolling Volatility',
                        width: 800, height: 400,
                        scales: { x: { time: true } },
                        series: [{}, { label: 'Volatility (%)', stroke: 'red', width: 2 }]
                    };
                    new uPlot(opts, [timestamps, volatilities], document.getElementById('chart'));
                });
        }
        plotData();
    "#,
    );

    let page: Markup = html! {
        (maud::DOCTYPE)
        html {
            head {
                title { "Rolling Parkinson's Volatility" }
                script src="https://unpkg.com/htmx.org@1.9.6" {}
                script src="https://unpkg.com/uplot/dist/uPlot.iife.min.js" {}
                link rel="stylesheet" href="https://unpkg.com/uplot/dist/uPlot.min.css";
                style { "body { font-family: Arial, sans-serif; text-align: center; }" }
            }
            body {
                h1 { "7-Day Rolling Parkinson's Volatility" }
                div id="chart" {}
                script { (script) }
            }
        }
    };
    Html(page.into_string())
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(serve_html))
        .route("/rolling_volatility", get(fetch_rolling_volatility))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
