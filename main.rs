use rand::{distributions::{Alphanumeric, Distribution, Uniform}, Rng};
use reqwest::{Client, header};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{BufReader, BufRead};
use base64::{encode};
use tokio;

async fn generate_random_value(value_type: &str) -> Value {
    let mut rng = rand::thread_rng();
    match value_type {
        "int" => Value::from(rng.gen_range(1..101)),
        "float" => Value::from(rng.gen_range(1.0..100.0)),
        "string" => Value::from(format!("random_string_{}", rng.gen_range(1..101))),
        "bool" => Value::from(rng.gen_bool(0.5)),
        "list" => Value::from((0..3).map(|_| rng.gen_range(1..11)).collect::<Vec<i32>>()),
        "dict" => json!({ "key": rng.gen_range(1..11) }),
        _ => Value::Null,
    }
}

async fn add_specific_attributes(data_dict: &mut Value) {
    let data_types = vec!["int", "float", "string", "bool", "list", "dict"];
    let mut rng = rand::thread_rng();
    for i in 1..=10 {
        let value_type = data_types.choose(&mut rng).unwrap();
        data_dict["ts_event_user_extra_authentication_kubernetes_io_attr".to_string() + &i.to_string()] = generate_random_value(value_type).await;
    }
}

async fn add_random_attributes(data_dict: &mut Value) {
    let data_types = vec!["int", "float", "string", "bool", "list", "dict"];
    let mut rng = rand::thread_rng();
    for _ in 0..rng.gen_range(1..101) {
        let key = format!("random_attr_{}", rng.gen_range(1..6001));
        let value_type = data_types.choose(&mut rng).unwrap();
        data_dict[&key] = generate_random_value(value_type).await;
    }
}

async fn insert_ndjson_data() {
    let client = Client::new();
    let file = File::open("data1.ndjson").expect("file not found");
    let reader = BufReader::new(file);

    let mut modified_data = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let mut row: Value = serde_json::from_str(&line.unwrap()).expect("error parsing json");
        
        if index % 2 == 0 {
            add_specific_attributes(&mut row).await;
        }
        if rand::random::<f64>() < 0.5 {
            add_random_attributes(&mut row).await;
        }

        modified_data.push(serde_json::to_string(&row).unwrap());
    }

    let data = modified_data.join("\n");
    let user = "a@a.com";
    let password = "a";
    let creds = encode(format!("{}:{}", user, password));

    let headers = header::HeaderMap::new();
    headers.insert(header::AUTHORIZATION, header::HeaderValue::from_str(&format!("Basic {}", creds)).unwrap());
    headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/x-ndjson"));

    let res = client.post("http://localhost:8000/_bulk")
        .headers(headers)
        .body(data)
        .send()
        .await
        .expect("request failed");

    println!("Status: {}", res.status());
}

#[tokio::main]
async fn main() {
    insert_ndjson_data().await;
}
