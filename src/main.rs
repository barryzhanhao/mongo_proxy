use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};
use tokio::sync::Semaphore;
use std::sync::Arc;
use bytes::{BytesMut, Buf};
use std::net::SocketAddr;
use bson::Document;
use std::io::{Cursor, ErrorKind};

/// MongoDB wire message header length
const MSG_HEADER_LEN: usize = 16;

/// Opcodes (partial)
const OP_MSG: i32 = 2013;

const OP_QUERY: i32 = 2004;

const OP_COMPRESSED: i32 = 2012; // if you plan to support it later

/// Safety limits and timeouts
const MAX_MSG_LEN: usize = 48 * 1024 * 1024; // 48MB upper bound to protect memory
const IO_TIMEOUT_MS: u64 = 10_000; // per IO op timeout
const IDLE_TIMEOUT_MS: u64 = 60_000; // idle header wait timeout
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 监听本地端口（真实情况可能是 27017）
    let listen_addr = "0.0.0.0:27017";
    let upstream_addr = "127.0.0.1:27018"; // 真实 MongoDB 地址（示例）
    let listener = TcpListener::bind(listen_addr).await?;
    println!("Proxy listening on {}, upstream is {}", listen_addr, upstream_addr);

    let limiter = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        let upstream_addr = upstream_addr.to_string();
        let permit = limiter.clone().acquire_owned().await?;
        tokio::spawn(async move {
            let _permit = permit; // drop on scope end to release slot
            if let Err(e) = handle_client(client_stream, client_addr, &upstream_addr).await {
                eprintln!("Connection {} error: {:?}", client_addr, e);
            }
        });
    }
}

async fn handle_client(mut client: TcpStream, client_addr: SocketAddr, upstream_addr: &str) -> anyhow::Result<()> {
    // 建立到上游 Mongo 的 TCP 连接（简单起见）
    let mut upstream = match timeout(Duration::from_millis(IO_TIMEOUT_MS), TcpStream::connect(upstream_addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(anyhow::anyhow!("connect upstream error: {:?}", e)),
        Err(_) => return Err(anyhow::anyhow!("connect upstream timeout")),
    };
    println!("{} -> connected to upstream {}", client_addr, upstream_addr);

    // 复用缓冲区，降低分配抖动
    let mut body_buf = BytesMut::with_capacity(8 * 1024);
    let mut up_body_buf = BytesMut::with_capacity(8 * 1024);

    loop {
        // 先读取 header 16 字节（blocking until available）
        let mut header = [0u8; MSG_HEADER_LEN];
        let total_len = match timeout(Duration::from_millis(IDLE_TIMEOUT_MS), client.read_exact(&mut header)).await {
            Ok(Ok(_)) => {
                let len = i32::from_le_bytes(header[0..4].try_into()?);
                if len <= 0 {
                    // 0 或负数长度视为对端异常/关闭，优雅退出
                    println!("Client {} disconnected or sent invalid length: {}", client_addr, len);
                    return Ok(());
                }
                let len_usize = len as usize;
                if len_usize < MSG_HEADER_LEN || len_usize > MAX_MSG_LEN {
                    println!("Client {} invalid/oversized message length: {}", client_addr, len_usize);
                    return Err(anyhow::anyhow!("client message length invalid"));
                }
                len_usize
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // 正常断开连接
                println!("Client {} disconnected", client_addr);
                return Ok(());
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("client read header error: {:?}", e)),
            Err(_) => {
                println!("Client {} idle timeout waiting for header", client_addr);
                return Ok(());
            }
        };

        // 解析消息长度和 opcode
        let op_code = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        // 长度已在上面检查

        // 读取 body（剩余部分）
        let body_len = total_len - MSG_HEADER_LEN;
        body_buf.resize(body_len, 0u8);
        match timeout(Duration::from_millis(IO_TIMEOUT_MS), client.read_exact(&mut body_buf[..])).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                println!("Client {} disconnected while reading body", client_addr);
                return Ok(());
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("client read body error: {:?}", e)),
            Err(_) => return Err(anyhow::anyhow!("client read body timeout")),
        }

        // 我们现在拥有完整的一条 wire message（header + body）
        // 若需要解析：对 OP_MSG 做基本解析（示例仅处理 payloadType=0）
        // 并根据部分命令（如 awaitable hello 的 maxAwaitTimeMS）自适应上游响应超时
        let mut response_timeout_ms: u64 = IO_TIMEOUT_MS;
        if op_code == OP_MSG {
            // OP_MSG 格式： header(16) | flagBits(4) | sections...
            if body_len >= 4 {
                let mut idx = 4usize;
                // 解析第一个 section（若存在）
                if idx < body_len {
                    let payload_type = body_buf[idx];
                    idx += 1;
                    if payload_type == 0 {
                        // payload type 0: a single BSON document starts at idx
                        if idx + 4 <= body_len {
                            // BSON 文档以 int32 大小开头
                            let bson_size = i32::from_le_bytes([body_buf[idx], body_buf[idx+1], body_buf[idx+2], body_buf[idx+3]]) as usize;
                            if idx + bson_size <= body_len {
                                let bson_slice = &body_buf[idx..idx + bson_size];
                                // 解析 BSON 文档
                                match Document::from_reader(&mut Cursor::new(bson_slice)) {
                                    Ok(doc) => {
                                        // 这是客户端发过来的第一个 BSON 文档（通常是 command）
                                        println!("Parsed OP_MSG first document: {:?}", doc);
                                        // 如果包含 maxAwaitTimeMS（例如 awaitable hello），放宽上游响应超时
                                        if let Some(ms) = doc.get_i64("maxAwaitTimeMS").ok() {
                                            let with_slack = ms.saturating_add(5_000) as u64;
                                            response_timeout_ms = with_slack.clamp(5_000, 120_000);
                                        }
                                        // 你可以基于 doc 的内容做路由决策，例如：
                                        // if let Some(cmd) = doc.get_str("find").ok() { route_to_shard(cmd); }
                                    }
                                    Err(e) => {
                                        eprintln!("BSON parse error: {:?}", e);
                                    }
                                }
                            }
                        }
                    } else if payload_type == 1 {
                        // payload type 1: sequence of BSON documents with a cstring identifier.
                        // 解析略（可以按 spec 实现）
                        println!("OP_MSG payload type 1 (document sequence) - not parsed in demo");
                    }
                }
            }
        } else if op_code == OP_QUERY {
            let mut cur = Cursor::new(&body_buf);

            // 1. flags
            let flags = cur.get_i32_le();

            // 2. fullCollectionName (cstring)
            let mut name_bytes = Vec::new();
            loop {
                let b = cur.get_u8();
                if b == 0 {
                    break;
                }
                name_bytes.push(b);
            }
            let full_collection_name = String::from_utf8(name_bytes)?;

            // 3. numberToSkip + numberToReturn
            let number_to_skip = cur.get_i32_le();
            let number_to_return = cur.get_i32_le();

            // 4. query document
            let query_doc = bson::Document::from_reader(&mut cur)?;
            println!("OP_QUERY [{}]: flags={}, skip={}, return={}, query={:?}",
                     full_collection_name, flags, number_to_skip, number_to_return, query_doc);

            // 5. (Optional) returnFieldsSelector
            if (cur.position() as usize) < body_buf.len() {
                match bson::Document::from_reader(&mut cur) {
                    Ok(selector_doc) => {
                        println!("returnFieldsSelector={:?}", selector_doc);
                    }
                    Err(_) => {
                        // ignore parse failure, likely absent
                    }
                }
            }

            // 这里可以根据 full_collection_name 或 query_doc 做路由
        } else if op_code == OP_COMPRESSED {
            // OP_COMPRESSED: 需要解压并再解析被包装的 inner opcode
            println!("Received OP_COMPRESSED - not handled in demo. Must decompress per spec.");
        } else {
            // legacy opcodes or others — 可以选择解析或直接转发
            println!("Received opcode {} (len={}) - forwarding", op_code, total_len);
        }

        // 把原始消息（header + body）原封不动地转发给上游
        // 先写 header
        timeout(Duration::from_millis(IO_TIMEOUT_MS), upstream.write_all(&header)).await??;
        timeout(Duration::from_millis(IO_TIMEOUT_MS), upstream.write_all(&body_buf)).await??;
        timeout(Duration::from_millis(IO_TIMEOUT_MS), upstream.flush()).await??;

        // 然后从上游读取响应（一条或多条消息）并回写到 client
        // 注意：一个请求可能会有多条响应或大响应，这里简化实现只读一条响应并回写
        // 更健壮的做法是循环读取上游直到上游关闭或根据实际协议语义处理
        // 读取上游 header
        let mut up_header = [0u8; MSG_HEADER_LEN];
        match timeout(Duration::from_millis(response_timeout_ms), upstream.read_exact(&mut up_header)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                println!("Upstream closed while reading header");
                return Ok(());
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("upstream read header error: {:?}", e)),
            Err(_) => return Err(anyhow::anyhow!("upstream read header timeout")),
        }
        let up_total_len = i32::from_le_bytes([up_header[0], up_header[1], up_header[2], up_header[3]]) as usize;
        if up_total_len < MSG_HEADER_LEN || up_total_len > MAX_MSG_LEN {
            return Err(anyhow::anyhow!("invalid upstream total_len: {}", up_total_len));
        }
        let up_body_len = up_total_len - MSG_HEADER_LEN;
        up_body_buf.resize(up_body_len, 0u8);
        match timeout(Duration::from_millis(response_timeout_ms), upstream.read_exact(&mut up_body_buf[..])).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                println!("Upstream closed while reading body");
                return Ok(());
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("upstream read body error: {:?}", e)),
            Err(_) => return Err(anyhow::anyhow!("upstream read body timeout")),
        }

        // 把上游消息回写给客户端
        timeout(Duration::from_millis(IO_TIMEOUT_MS), client.write_all(&up_header)).await??;
        timeout(Duration::from_millis(IO_TIMEOUT_MS), client.write_all(&up_body_buf)).await??;
        timeout(Duration::from_millis(IO_TIMEOUT_MS), client.flush()).await??;
    }
}
