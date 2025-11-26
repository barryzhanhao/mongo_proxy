use anyhow::Context;
use anyhow::Result;

use byteorder::{LittleEndian, ReadBytesExt};
use mongodb::{bson::{Bson, Document}, Client};
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Cursor, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};

const MSG_HEADER_LEN: usize = 16;
const OP_REPLY: i32 = 1;
const OP_QUERY: i32 = 2004;
const OP_MSG: i32 = 2013;
const OP_COMPRESSED: i32 = 2012;

const MAX_MSG_LEN: usize = 48 * 1024 * 1024;
const IO_TIMEOUT_MS: u64 = 10_000;
const IDLE_TIMEOUT_MS: u64 = 60_000;
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let listen_addr = "0.0.0.0:27017";
    let upstream_addr = std::env::var("MONGO_URI")
        .unwrap_or_else(|_| "mongodb://mongoadmin:secret@127.0.0.1:27018".to_string());

    let mongo_client = Arc::new(Client::with_uri_str(&upstream_addr).await
        .with_context(|| format!("failed to connect to {}", upstream_addr))?);

    let listener = TcpListener::bind(listen_addr).await?;
    println!("Proxy listening on {}, upstream is {}", listen_addr, upstream_addr);

    let limiter = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        let mongo_client = mongo_client.clone();
        let permit = limiter.clone().acquire_owned().await?;
        tokio::spawn(async move {
            let _permit = permit; // release when dropped
            if let Err(e) = handle_client(client_stream, client_addr, mongo_client).await {
                eprintln!("Connection {} error: {:?}", client_addr, e);
            }
        });
    }
}

async fn handle_client(mut client: TcpStream, client_addr: SocketAddr, mongo_client: Arc<Client>) -> Result<()> {
    println!("{} connected", client_addr);

    loop {
        // 读取完整 message（包含 4 字节长度 header）
        let mut buf = vec![0u8; 4];
        match timeout(Duration::from_millis(IDLE_TIMEOUT_MS), client.read_exact(&mut buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                println!("Client {} disconnected", client_addr);
                return Ok(());
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("read header error: {:?}", e)),
            Err(_) => {
                println!("Client {} idle timeout waiting for header", client_addr);
                return Ok(());
            }
        }

        // total length (包含 header 本身)
        let total_len = i32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        if total_len < MSG_HEADER_LEN || total_len > MAX_MSG_LEN {
            println!("Client {} invalid/oversized message length: {}", client_addr, total_len);
            return Err(anyhow::anyhow!("client message length invalid"));
        }

        // resize buffer to whole message and read remaining bytes
        buf.resize(total_len, 0);
        match timeout(Duration::from_millis(IO_TIMEOUT_MS), client.read_exact(&mut buf[4..])).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                println!("Client {} disconnected while reading body", client_addr);
                return Ok(());
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("client read body error: {:?}", e)),
            Err(_) => return Err(anyhow::anyhow!("client read body timeout")),
        }

        // header is first 16 bytes
        if buf.len() < MSG_HEADER_LEN {
            return Err(anyhow::anyhow!("message too short for header"));
        }
        let mut header = [0u8; MSG_HEADER_LEN];
        header.copy_from_slice(&buf[..MSG_HEADER_LEN]);

        let op_code = i32::from_le_bytes(header[12..16].try_into().unwrap());

        // body is bytes after header
        let body = &buf[MSG_HEADER_LEN..];

        if op_code == OP_MSG {
            match parse_op_msg(body) {
                Ok(cmd_doc) => {
                    if is_admin_command(&cmd_doc, false) {
                        let resp_doc = build_admin_response(&cmd_doc);
                        send_op_msg_reply(&mut client, &header, &resp_doc).await?;
                    } else {
                        let db_name = extract_db_from_doc(&cmd_doc);
                        let command = sanitize_command_doc(&cmd_doc);
                        match execute_command(&*mongo_client, &db_name, command).await {
                            Ok(result_doc) => {
                                send_op_msg_reply(&mut client, &header, &result_doc).await?;
                            }
                            Err(err) => {
                                let err_doc = build_error_response(err);
                                send_op_msg_reply(&mut client, &header, &err_doc).await?;
                            }
                        }
                    }
                }
                Err(err) => {
                    eprintln!("Failed to parse OP_MSG: {:?}", err);
                    let err_doc = build_error_response(err);
                    send_op_msg_reply(&mut client, &header, &err_doc).await?;
                }
            }
        } else if op_code == OP_QUERY {
            // parse OP_QUERY body as per MongoDB spec
            let mut cur = Cursor::new(body);
            let flags = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
            // read cstring fullCollectionName
            let mut name_bytes = Vec::new();
            loop {
                let b = ReadBytesExt::read_u8(&mut cur)?;
                if b == 0 { break; }
                name_bytes.push(b);
            }
            let full_collection_name = String::from_utf8(name_bytes)?;

            let number_to_skip = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;
            let number_to_return = ReadBytesExt::read_i32::<LittleEndian>(&mut cur)?;

            let query_doc = Document::from_reader(&mut cur)
                .context("failed to parse OP_QUERY query document")?;

            println!("OP_QUERY [{}]: flags={}, skip={}, return={}, query={:?}",
                     full_collection_name, flags, number_to_skip, number_to_return, query_doc);

            if is_admin_command(&query_doc, full_collection_name.ends_with(".$cmd")) {
                let resp_doc = build_admin_response(&query_doc);
                send_op_reply(&mut client, &header, &resp_doc).await?;
                continue;
            }

            let db_name = extract_db_from_namespace(&full_collection_name);
            let command = sanitize_command_doc(&query_doc);
            match execute_command(&*mongo_client, &db_name, command).await {
                Ok(result_doc) => {
                    send_op_reply(&mut client, &header, &result_doc).await?;
                }
                Err(err) => {
                    let err_doc = build_error_response(err);
                    send_op_reply(&mut client, &header, &err_doc).await?;
                }
            }
        } else if op_code == OP_COMPRESSED {
            // not implemented in demo
            println!("Received OP_COMPRESSED - not handled in demo.");
            let err_doc = build_error_response(anyhow::anyhow!("OP_COMPRESSED not supported in demo"));
            send_op_reply(&mut client, &header, &err_doc).await?;
        } else {
            let err_doc = build_error_response(anyhow::anyhow!("unsupported opcode {}", op_code));
            send_op_reply(&mut client, &header, &err_doc).await?;
        }
    }
}

fn is_admin_command(doc: &Document, admin_hint: bool) -> bool {
    let special = doc.keys().any(|key| {
        matches!(
            key.to_lowercase().as_str(),
            "ismaster" | "hello" | "ping" | "getlasterror"
        )
    });
    if special { return true; }
    if admin_hint { return true; }
    match doc.get("$db") {
        Some(Bson::String(db)) if db == "admin" => true,
        _ => false,
    }
}

fn build_admin_response(cmd: &Document) -> Document {
    let mut resp = Document::new();
    let has_ismaster = cmd.keys().any(|k| k.eq_ignore_ascii_case("ismaster"));
    let has_hello = cmd.keys().any(|k| k.eq_ignore_ascii_case("hello"));
    if has_ismaster || has_hello {
        resp.insert("ismaster", Bson::Boolean(true));
        resp.insert("helloOk", Bson::Boolean(true));
        resp.insert("maxWireVersion", Bson::Int32(17));
        resp.insert("minWireVersion", Bson::Int32(0));
        resp.insert("maxBsonObjectSize", Bson::Int32(16 * 1024 * 1024));
        resp.insert("maxMessageSizeBytes", Bson::Int32(48 * 1024 * 1024));
        resp.insert("maxWriteBatchSize", Bson::Int32(1000));
        resp.insert("logicalSessionTimeoutMinutes", Bson::Int32(30));
        resp.insert("readOnly", Bson::Boolean(false));
    } else if cmd.contains_key("getlasterror") {
        resp.insert("err", Bson::Null);
        resp.insert("n", Bson::Int32(0));
    } else if cmd.contains_key("ping") {
        resp.insert("reply", Bson::String("pong".to_string()));
    }
    resp.insert("$db", Bson::String("admin".to_string()));
    resp.insert("ok", Bson::Double(1.0));
    resp
}

async fn send_op_reply(client: &mut TcpStream, request_header: &[u8; MSG_HEADER_LEN], doc: &Document) -> Result<()> {
    let request_id = i32::from_le_bytes(request_header[4..8].try_into().unwrap());
    let mut doc_bytes = Vec::new();
    doc.to_writer(&mut doc_bytes)?;

    // OP_REPLY body layout (simplified): flags (int32), cursorId (int64), startingFrom (int32), numberReturned (int32), documents...
    let mut body = Vec::with_capacity(20 + doc_bytes.len());
    body.extend_from_slice(&0i32.to_le_bytes()); // flags
    body.extend_from_slice(&0i64.to_le_bytes()); // cursorId
    body.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    body.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    body.extend_from_slice(&doc_bytes);

    send_wire_message(client, request_id, OP_REPLY, &body).await
}

async fn send_op_msg_reply(client: &mut TcpStream, request_header: &[u8; MSG_HEADER_LEN], doc: &Document) -> Result<()> {
    let request_id = i32::from_le_bytes(request_header[4..8].try_into().unwrap());
    let mut doc_bytes = Vec::new();
    doc.to_writer(&mut doc_bytes)?;

    // OP_MSG body: flags (uint32) then sections. We'll use a single type=0 section (body)
    let mut body = Vec::with_capacity(4 + 1 + doc_bytes.len());
    body.extend_from_slice(&0u32.to_le_bytes()); // flags
    body.push(0u8); // section kind = 0
    body.extend_from_slice(&doc_bytes);

    send_wire_message(client, request_id, OP_MSG, &body).await
}

async fn send_wire_message(client: &mut TcpStream, request_id: i32, op_code: i32, body: &[u8]) -> Result<()> {
    let response_request_id = request_id.wrapping_add(1);
    let total_len = (MSG_HEADER_LEN + body.len()) as i32;

    let mut header = [0u8; MSG_HEADER_LEN];
    header[0..4].copy_from_slice(&total_len.to_le_bytes());
    header[4..8].copy_from_slice(&response_request_id.to_le_bytes());
    header[8..12].copy_from_slice(&request_id.to_le_bytes());
    header[12..16].copy_from_slice(&op_code.to_le_bytes());

    client.write_all(&header).await?;
    client.write_all(body).await?;
    client.flush().await?;
    Ok(())
}

fn sanitize_command_doc(doc: &Document) -> Document {
    let mut cmd = doc.clone();
    cmd.remove("$db");
    cmd.remove("$readPreference");
    cmd.remove("lsid");
    cmd.remove("$clusterTime");
    cmd.remove("txnNumber");
    cmd
}

fn extract_db_from_doc(doc: &Document) -> String {
    doc.get_str("$db").map(|s| s.to_string()).unwrap_or_else(|_| "admin".to_string())
}

fn extract_db_from_namespace(ns: &str) -> String {
    ns.split('.').next().unwrap_or("admin").to_string()
}

async fn execute_command(client: &Client, db: &str, command: Document) -> Result<Document> {
    let database = client.database(db);
    let result = database.run_command(command, None).await?;
    Ok(result)
}

fn build_error_response(err: anyhow::Error) -> Document {
    let mut doc = Document::new();
    doc.insert("ok", Bson::Double(0.0));
    doc.insert("errmsg", Bson::String(err.to_string()));
    doc
}

/// Parse OP_MSG body (sections). Returns merged command document.
/// Supports:
///  - section type 0 (single body document)
///  - section type 1 (document sequence) -> appended as array under its identifier
fn parse_op_msg(body: &[u8]) -> Result<Document> {
    if body.len() < 4 {
        anyhow::bail!("op_msg body too short");
    }
    let mut idx = 0usize;

    // flags (uint32)
    if body.len() < idx + 4 {
        anyhow::bail!("op_msg body missing flags");
    }
    let flags = u32::from_le_bytes(body[idx..idx + 4].try_into().unwrap());
    idx += 4;

    let checksum_present = (flags & 1) != 0;
    let limit = if checksum_present {
        if body.len() < 4 {
            anyhow::bail!("op_msg body too short for checksum");
        }
        body.len() - 4
    } else {
        body.len()
    };
    let body = &body[..limit];

    let mut command: Option<Document> = None;
    let mut sequences: HashMap<String, Vec<Document>> = HashMap::new();

    while idx < body.len() {
        let payload_type = body[idx];
        idx += 1;

        match payload_type {
            0 => {
                // type 0: a single BSON document follows starting at idx
                if idx + 4 > body.len() {
                    anyhow::bail!("type 0 section truncated");
                }
                let mut cursor = Cursor::new(&body[idx..]);
                let doc = Document::from_reader(&mut cursor)?;
                let read = cursor.position() as usize;
                idx += read;
                if command.is_none() {
                    command = Some(doc);
                }
            }
            1 => {
                // type 1: document sequence
                // next comes int32 size (section size) - BUT per spec for type=1 the layout is:
                // sequence = int32 size + cstring identifier + document... (size includes the int32 and identifier and documents)
                // Note: in OP_MSG the "size" follows the kind byte, not including the kind byte itself.
                if idx + 4 > body.len() {
                    anyhow::bail!("type 1 section missing size");
                }
                let section_size = ReadBytesExt::read_u32::<LittleEndian>(&mut (&body[idx..idx + 4]))? as usize;
                // section_size includes the int32, identifier, docs; we are currently at idx pointing to that int32 start
                let section_end = idx + section_size;
                if section_end > body.len() {
                    anyhow::bail!("document sequence exceeds body length");
                }
                // identifier starts after the int32 (which we just read)
                let mut content_idx = idx + 4;
                // read cstring identifier
                let mut name_bytes = Vec::new();
                loop {
                    if content_idx >= section_end {
                        anyhow::bail!("identifier not terminated");
                    }
                    let b = body[content_idx];
                    content_idx += 1;
                    if b == 0 { break; }
                    name_bytes.push(b);
                }
                let identifier = String::from_utf8(name_bytes)?;
                let mut docs = Vec::new();
                // read documents until section_end
                while content_idx < section_end {
                    if content_idx + 4 > section_end {
                        anyhow::bail!("document length incomplete in sequence");
                    }
                    let mut doc_cursor = Cursor::new(&body[content_idx..section_end]);
                    let doc = Document::from_reader(&mut doc_cursor)?;
                    let bytes_read = doc_cursor.position() as usize;
                    if bytes_read == 0 {
                        anyhow::bail!("zero-length document in sequence");
                    }
                    docs.push(doc);
                    content_idx += bytes_read;
                }
                sequences.entry(identifier).or_insert_with(Vec::new).extend(docs);
                idx = section_end;
            }
            other => {
                anyhow::bail!("unknown OP_MSG section type {}", other);
            }
        }
    }

    let mut cmd = command.ok_or_else(|| anyhow::anyhow!("missing command document"))?;
    for (k, v) in sequences {
        let arr = v.into_iter().map(Bson::Document).collect();
        cmd.insert(k, Bson::Array(arr));
    }
    Ok(cmd)
}
