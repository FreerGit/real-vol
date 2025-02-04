use std::f64;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use reqwest::Error;
use serde::{de, Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
struct BybitResponse {
    retCode: i32,
    retMsg: String,
    result: KlinesForTicker, // hardcoded, fight me
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
    // .map_err(de::Error::custom)?
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
    time_series_vol * 365.25_f64.sqrt() // annualize
}

#[tokio::main]
async fn main() {
    let abc = fetch_ohlc("BTCUSDT", "60", 100).await.unwrap(); // Fetch last 100 hourly candles
                                                               // let skip_today: Vec<CandleData> = abc.iter().skip(1).cloned().collect();
    for candle in abc.iter() {
        println!(
            "Time: {}, High: {}, Low: {}",
            candle.start, candle.high, candle.low
        );
    }

    let p = calculate_parkinson(abc);
    println!("Parkinson: {}", p);
}
