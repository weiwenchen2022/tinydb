//! A "tiny database" and accompanying protocol
//!
//! This example shows the usage of shared state amongst all connected clients,
//! namely a database of key/value pairs. Each connected client can send a
//! series of GET/SET commands to query the current value of a key or set the
//! value of a key.
//!
//! This example has a simple protocol you can use to interact with the server.
//! To run, first run this in one terminal window:
//!
//!     cargo run
//!
//! and next in another windows run:
//!
//!     nc 127.0.0.1 6142
//!
//! In the `connect` window you can type in commands where when you hit enter
//! you'll get a response from the server for that command. An example session
//! is:
//!
//!
//!     $ nc 127.0.0.1 6142
//!     GET foo
//!     foo = bar
//!     GET FOOBAR
//!     error: no key FOOBAR
//!     SET FOOBAR my awesome string
//!     set FOOBAR = `my awesome string`, previous: None
//!     SET foo tokio
//!     set foo = `tokio`, previous: Some("bar")
//!     GET foo
//!     foo = tokio
//!
//! Namely you can issue two forms of commands:
//!
//! * `GET $key` - this will fetch the value of `$key` from the database and
//!   return it. The server's database is initially populated with the key `foo`
//!   set to the value `bar`
//! * `SET $key $value` - this will set the value of `$key` to `$value`,
//!   returning the previous value, if any.

#![warn(rust_2018_idioms)]

use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use bytes::Bytes;

use futures::SinkExt;

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};

/// Possible requests our clients can send us
enum Request {
    Get { key: String },
    Set { key: String, value: Bytes },
}

/// Responses to the `Request` commands above
enum Response {
    Error {
        msg: String,
    },
    Get {
        key: String,
        value: Bytes,
    },
    Set {
        key: String,
        value: Bytes,
        previous: Option<Bytes>,
    },
}

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
struct Database {
    map: Mutex<Db>,
}

use tokio::fs::File;

use tokio::sync::{broadcast, mpsc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6142".to_string());
    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let initial_db = match File::open("tinydb.json").await {
        Ok(f) => load_as_json(f).await?,
        Err(err) if ErrorKind::NotFound == err.kind() => {
            let mut initial_db = Db::new();
            initial_db.insert("foo".to_string(), "bar".into());
            initial_db
        }
        Err(err) => Err(err)?,
    };
    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    // When the provided `shutdown` future completes, we must send a shutdown
    // message to all active connections. We use a broadcast channel for this
    // purpose. The call below ignores the receiver of the broadcast pair, and when
    // a receiver is needed, the subscribe() method on the sender is used to create
    // one.
    let (notify_shutdown, _) = broadcast::channel::<()>(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

    loop {
        let socket = tokio::select! {
            res = listener.accept() => match res {
                Ok((socket, _)) => socket,
                Err(e) => {
                    eprintln!("error accepting socket; error = {:?}", e);
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };

        // After getting a new connection first we see a clone of the database
        // being created, which is creating a new reference for this connected
        // client to use.
        let db = Arc::clone(&db);
        let shutdown = notify_shutdown.subscribe();
        let _shutdown_complete = shutdown_complete_tx.clone();

        // Like with other small servers, we'll `spawn` this client to ensure it
        // runs concurrently with all other clients. The `move` keyword is used
        // here to move ownership of our db handle into the async closure.
        tokio::spawn(async move {
            // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
            // to convert our stream of bytes, `socket`, into a `Stream` of lines
            // as well as convert our line based responses into a stream of bytes.
            let lines = Framed::new(socket, LinesCodec::new());
            tokio::pin!(lines, shutdown);

            // Here for every line we get back from the `Framed` decoder,
            // we parse the request, and if it's valid we generate a response
            // based on the values in the database.
            loop {
                tokio::select! {
                    result = lines.next() => {
                        if let Some(result) = result {
                        match result {
                            Ok(line) => {
                                let response = handle_request(&line, &db).await;
                                let response = response.serialize();

                                if let Err(e) = lines.send(response.as_str()).await {
                                    eprintln!("error on sending response; error = {:?}", e);
                                }
                            }
                            Err(e) => eprintln!("error on decoding from socket; error = {:?}", e),
                        }
                    } else {
                        // The connection will be closed at this point as `lines.next()` has returned `None`.
                        break;
                    }
                }
                    _ = shutdown.recv() => break,
                }
            }

            drop(db);
            drop(_shutdown_complete);
        });
    }

    // let _ = notify_shutdown.send(());
    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;

    let db = Arc::into_inner(db).unwrap().map.into_inner().unwrap();
    let f = File::create("tinydb.json").await?;
    save_as_json(f, &db).await?;

    Ok(())
}

async fn handle_request(line: &str, db: &Arc<Database>) -> Response {
    let request = match Request::parse(line) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };

    let mut db = db.map.lock().unwrap();
    match request {
        Request::Get { key } => match db.get(&key) {
            Some(value) => Response::Get {
                key: key.clone(),
                value: value.clone(),
            },
            None => Response::Error {
                msg: format!("no key {:?}", key),
            },
        },
        Request::Set { key, value } => {
            let previous = db.insert(key.clone(), value.clone());
            Response::Set {
                key,
                value,
                previous,
            }
        }
    }
}

impl Request {
    pub fn parse(input: &str) -> Result<Self, String> {
        let mut parts = input.splitn(3, ' ');

        let cmd = parts.next().ok_or("empty input")?;
        match cmd.to_uppercase().as_str() {
            "GET" => {
                let key = parts.next().ok_or("GET must be followed by a key")?;
                if parts.next().is_some() {
                    return Err("GET's key must not be followed by anything".into());
                }

                Ok(Request::Get {
                    key: key.to_string(),
                })
            }
            "SET" => {
                let key = parts.next().ok_or("SET must be followed by a key")?;
                let value = parts.next().ok_or("SET needs a value")?;
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string().into(),
                })
            }
            _ => Err(format!("unknown command: {}", cmd)),
        }
    }
}

impl Response {
    pub fn serialize(&self) -> String {
        match self {
            Response::Error { msg } => format!("error: {}", msg),
            Response::Get { key, value } => format!("{} = {:?}", key, value),
            Response::Set {
                key,
                value,
                previous,
            } => format!("set {} = `{:?}`, previous: {:?}", key, value, previous),
        }
    }
}

use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Serialize, Deserialize, Clone)]
struct Db(HashMap<String, Bytes>);

impl Db {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl Deref for Db {
    type Target = HashMap<String, Bytes>;
    fn deref(&self) -> &HashMap<String, Bytes> {
        &self.0
    }
}
impl DerefMut for Db {
    fn deref_mut(&mut self) -> &mut HashMap<String, Bytes> {
        &mut self.0
    }
}

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn save_as_json<W>(mut writer: W, db: &Db) -> Result<(), Box<dyn std::error::Error>>
where
    W: AsyncWriteExt + Unpin,
{
    let json = serde_json::to_string(db)?;
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

async fn load_as_json<R>(mut reader: R) -> Result<Db, Box<dyn std::error::Error>>
where
    R: AsyncReadExt + Unpin,
{
    let mut json = String::new();
    reader.read_to_string(&mut json).await?;
    let db = serde_json::from_str(&json)?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use crate::{load_as_json, save_as_json, Db};
    use std::io::Cursor;

    #[tokio::test]
    async fn serialized() -> Result<(), Box<dyn std::error::Error>> {
        let mut db = Db::new();
        db.insert("foo".into(), "bar".into());

        let buf = Vec::new();
        let mut buf = Cursor::new(buf);
        save_as_json(&mut buf, &db).await.unwrap();

        buf.set_position(0);
        let db = load_as_json(&mut buf).await.unwrap();
        assert_eq!(b"bar", &db.get("foo").unwrap()[..]);

        Ok(())
    }
}
