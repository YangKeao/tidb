// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Verify the HTTP decoders selected by the workspace dependency graph.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::time::Duration;

use flate2::{
    Compression,
    write::{GzEncoder, ZlibEncoder},
};

#[tokio::test]
async fn http_client_decodes_gzip_and_deflate() {
    let sample = b"compressed object metadata";
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(sample).unwrap();
    let mut deflate = ZlibEncoder::new(Vec::new(), Compression::default());
    deflate.write_all(sample).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for (encoding, payload) in [
            ("gzip", gzip.finish().unwrap()),
            ("deflate", deflate.finish().unwrap()),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&mut stream);
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Encoding: {encoding}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len(),
            ).unwrap();
            stream.write_all(&payload).unwrap();
        }
    });

    let client = reqwest12::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for method in ["gzip", "deflate"] {
        let body = client
            .get(format!("http://{address}/{method}"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body.as_ref(), sample, "{method}");
    }
    server.join().unwrap();
}
