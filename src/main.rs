#![feature(fnbox)]
#![feature(plugin)]
#![plugin(rocket_codegen)]

#[macro_use]
extern crate error_chain;

extern crate futures;
extern crate futures_cpupool;
extern crate hyper;

extern crate log4rs;
#[macro_use]
extern crate log;
extern crate regex;
extern crate rocket;
extern crate rocket_contrib;
extern crate tokio_timer;

#[macro_use]
extern crate serde_derive;

extern crate serde_json;
extern crate structopt;

#[macro_use]
extern crate structopt_derive;

extern crate url;

use futures::Future;
use futures_cpupool::CpuPool;
use hyper::client::Client;
use hyper::header::ContentType;
use regex::Regex;
use rocket::config::{Config, Environment};
use rocket::State;
use rocket_contrib::Json;
use std::boxed::FnBox;
use std::collections::HashMap;
use std::error::Error;
use std::io::{self, Read, Write};
use std::iter;
use std::process::{self, Command};
use std::net::SocketAddr;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;
use structopt::StructOpt;
use tokio_timer::Timer;
use url::Url;

// while obviously it is better to use Scheme as an enum
// it needs to be in integer

// #[derive(Serialize, Deserialize, Clone, Copy, Debug)]
// enum Scheme {
//     Http,
//     Https,
// }

type Scheme = i32;

const HTTP: i32 = 0;
// const HTTPS: i32 = 1;
// const INVALID: i32 = 2;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
struct ExecReq {
    id: String,
    cmd_id_re: String,
    cmd: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PingReq {
    id: String,
    scheme: Scheme,
    port: u16,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PingRsp {
    server: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CommStatus {
    exit_code: i32,

    #[serde(skip_serializing_if = "Option::is_none")]
    stdout: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    stderr: Option<String>,

    hostname: String,
    port: u16,
}

#[derive(Debug)]
struct ClientInfo {
    scheme: Scheme,
    url: Url,
}

type CommOverallStatus = HashMap<String, CommStatus>;
type ClientMap = RwLock<HashMap<String, ClientInfo>>;

mod errors {
    error_chain! {
        errors {
            ClientMapRead {
                description("error in reading client map")
                display("error in reading client map")
            }

            ClientMapWrite {
                description("error in writing to client map")
                display("error in writing to client map")
            }

            Timeout {
                description("execution timeout")
                display("execution timeout")
            }
        }
    }
}

use errors::*;

#[post("/ping", data = "<ping_req>")]
fn ping(socket_addr: SocketAddr,
        config: State<MainConfig>,
        client_map: State<ClientMap>,
        ping_req: Json<PingReq>)
        -> Result<Json<PingRsp>> {
    info!("Received ping from: {:?} with client name '{}'",
          socket_addr,
          ping_req.id);

    match client_map.write() {
        Ok(mut client_map) => {
            client_map.insert(ping_req.id.clone(),
                              ClientInfo {
                                  scheme: ping_req.scheme,
                                  url: {
                                      Url::parse(&format!("http://{}:{}", socket_addr.ip(), ping_req.port))
                                          .chain_err(|| "Unable to parse the base URL for client map!")?
                                  },
                              });

            Ok(Json(PingRsp { server: config.name.to_owned() }))
        }

        Err(_) => {
            error!("Unable to write into client map!");
            bail!(ErrorKind::ClientMapWrite)
        }
    }
}

macro_rules! create_fut {
    ($pool:expr, $timeout:expr, $action:expr) => {{
        let timer = Timer::default();
        let action_fut = $pool.spawn_fn($action);

        // must force into boxed form to ensure type sameness for different branch
        // furthermore must explicit type out the Send so that the inner type remains as Send
        // throughout passing around into other stuff

        let bail_fn: Box<FnBox(_) -> _ + Send> = Box::new(|_| bail!(ErrorKind::Timeout));

        let timeout_fut = timer.sleep($timeout)
            .then(bail_fn);

        let win_fn: Box<FnBox(_) -> _ + Send> = Box::new(|(win, _)| win);

        timeout_fut
            .select(action_fut)
            .map(win_fn)
    }};
}

fn execute_impl(is_blocking: bool,
                config: State<MainConfig>,
                client_map: State<ClientMap>,
                exec_req: Json<ExecReq>)
                -> Result<Option<CommOverallStatus>> {
    let timeout = Duration::from_millis(config.timeout as u64);
    let pool = CpuPool::new(config.thread_count as usize);

    if let Ok(client_map) = client_map.read() {
        // broadcast and execute for each client
        info!("Broadcasting commands to each client...");

        let client_comm_overall_status_futs: Vec<_> = client_map
            .iter()
            .map(|(client_name, client_info)| {
                let client_execute_url = {
                    let mut url = client_info.url.clone();
                    let path = if is_blocking { "execute" } else { "executenb" };
                    url.set_path(path);
                    url
                };

                let exec_req = exec_req.clone();
                let client_name = client_name.to_owned();

                info!("Broadcasting '{:?}' for client '{}' to '{}'...",
                      exec_req,
                      client_name,
                      client_execute_url);

                create_fut!(pool, timeout, move || {
                    let client = Client::new();

                    let mut res = client
                        .post(client_execute_url)
                        .body(&serde_json::to_string(&exec_req)
                                   .chain_err(|| "Unable to convert execution request Json into string")?)
                        .header(ContentType::json())
                        .send()
                        .chain_err(|| "Unable to perform client post")?;

                    let mut rsp_body = String::new();
                    let _ = res.read_to_string(&mut rsp_body);

                    info!("Client '{}' response body: {}", client_name, rsp_body);

                    let partial_comm_overall_status: CommOverallStatus =
                        serde_json::from_str(&rsp_body)
                            .chain_err(|| "Unable to parse client response body into comm overall status")?;

                    Ok(Some(partial_comm_overall_status))
                })
            })
            .collect();

        // execute for self if matching
        let client_key = Regex::new(&exec_req.cmd_id_re)
            .chain_err(|| format!("Unable to parse '{}' as regex", exec_req.cmd_id_re))?;

        info!("Checking for self name '{}' match against regex '{}'...",
              config.name,
              client_key);

        let self_comm_overall_status_fut = if client_key.is_match(&config.name) {
            let cmd = exec_req.cmd.to_owned();
            let address = config.address.to_owned();
            let port = config.port;
            let name = config.name.to_owned();

            info!("Name '{}' matches regex, executing '{}'...", name, cmd);

            create_fut!(pool,
                        timeout,
                        move || -> Result<Option<CommOverallStatus>> {
                let child = if cfg!(target_os = "windows") {
                    Command::new("cmd").args(&["/C", &cmd]).output()
                } else {
                    Command::new("sh").args(&["-c", &cmd]).output()
                };

                const ERROR_EXIT_CODE: i32 = 127;

                let self_comm_status = match child {
                    Ok(child) => {
                        let exit_code = match child.status.code() {
                            Some(code) => code,
                            None => ERROR_EXIT_CODE,
                        };

                        CommStatus {
                            exit_code: exit_code,
                            stdout: Some(String::from_utf8_lossy(&child.stdout).to_string()),
                            stderr: Some(String::from_utf8_lossy(&child.stderr).to_string()),
                            hostname: address,
                            port: port,
                        }
                    }

                    Err(e) => {
                        CommStatus {
                            exit_code: ERROR_EXIT_CODE,
                            stdout: None,
                            stderr: Some(e.description().to_owned()),
                            hostname: address,
                            port: port,
                        }
                    }
                };

                let mut partial_comm_overall_status = CommOverallStatus::new();
                partial_comm_overall_status.insert(name, self_comm_status);
                Ok(Some(partial_comm_overall_status))
            })
        } else {
            info!("Name '{}' does not match regex, returning None...",
                  config.name);

            create_fut!(pool,
                        timeout,
                        || -> Result<Option<CommOverallStatus>> { Ok(None) })
        };

        // merge all the results
        info!("Merging results from broadcast and self-execution...");

        let comm_overall_status_futs = client_comm_overall_status_futs
            .into_iter()
            .chain(iter::once(self_comm_overall_status_fut));

        // handle future results based on the type of execution
        if is_blocking {
            info!("Blocking mode, waiting for all created futures with possible timeout...");

            // converts from vec of status into hash map
            let comm_overall_status: CommOverallStatus = comm_overall_status_futs
                .map(|comm_overall_status_fut| comm_overall_status_fut.wait())
                .filter_map(|partial_comm_overall_status| {
                    match partial_comm_overall_status {
                        // expected case for valid use
                        Ok(Some(partial_comm_overall_status)) => Some(partial_comm_overall_status),

                        // invalid command given or e
                        Err((e, _)) => {
                            error!("Comm execution error: {}", e);

                            const OTHER_ERROR_EXIT_CODE: i32 = 126;
                            let mut partial_comm_overall_status = CommOverallStatus::new();

                            partial_comm_overall_status.insert(config.name.to_owned(),
                                                               CommStatus {
                                                                   exit_code: OTHER_ERROR_EXIT_CODE,
                                                                   stdout: None,
                                                                   stderr: Some(format!("{}", e)),
                                                                   hostname: config.address.to_owned(),
                                                                   port: config.port,
                                                               });

                            Some(partial_comm_overall_status)
                        }

                        // empty comm overall status result
                        _ => None,
                    }
                })
                .flat_map(|partial_comm_overall_status| partial_comm_overall_status.into_iter())
                .collect();

            info!("Done waiting, overall status: {:?}", comm_overall_status);

            Ok(Some(comm_overall_status))
        } else {
            info!("Non-blocking mode, forgeting all created futures...");

            for comm_overall_status_fut in comm_overall_status_futs {
                pool.spawn(comm_overall_status_fut).forget();
            }

            Ok(None)
        }
    } else {
        error!("Unable to read from client map!");
        bail!(ErrorKind::ClientMapRead);
    }
}

#[post("/execute", data = "<exec_req>")]
fn execute(config: State<MainConfig>,
           client_map: State<ClientMap>,
           exec_req: Json<ExecReq>)
           -> Result<Option<Json<CommOverallStatus>>> {
    info!("Received /execute: {:?}", exec_req);
    execute_impl(true, config, client_map, exec_req).map(|comm_overall_status| match comm_overall_status {
                                                             Some(comm_overall_status) => {
                                                                 Some(Json(comm_overall_status))
                                                             }
                                                             None => None,
                                                         })
}

#[post("/executenb", data = "<exec_req>")]
fn executenb(config: State<MainConfig>, client_map: State<ClientMap>, exec_req: Json<ExecReq>) -> Result<()> {
    info!("Received /executenb: {:?}", exec_req);

    execute_impl(false, config, client_map, exec_req).map(|_| ())
}

#[post("/shutdown", data = "<exec_req>")]
fn shutdown(config: State<MainConfig>, client_map: State<ClientMap>, exec_req: Json<ExecReq>) -> Result<()> {
    info!("Received /shutdown: {:?}", exec_req);
    executenb(config, client_map, exec_req)
}

#[derive(StructOpt, Debug)]
#[structopt(name = "Comm Service", about = "Program to ping and execute.")]
struct MainConfig {
    #[structopt(short = "n", long = "name", help = "Name of this communication server")]
    name: String,

    #[structopt(short = "t", long = "timeout", help = "# of milliseconds for execution timeout",
                default_value = "30000")]
    timeout: u32,

    #[structopt(short = "a", long = "address", help = "Interface address to host",
                default_value = "0.0.0.0")]
    address: String,

    #[structopt(short = "p", long = "port", help = "Port to host")]
    port: u16,

    #[structopt(short = "d", long = "ping-to", help = "Server to ping to (optional)")]
    ping_to: Option<Url>,

    #[structopt(short = "i", long = "interval", help = "Ping-to-server interval",
                default_value = "3000")]
    ping_to_interval: u32,

    #[structopt(short = "c", long = "thread-count", help = "# of threads in cpu pool",
                default_value = "64")]
    thread_count: u32,

    #[structopt(short = "l", long = "log-config-path", help = "Log config file path")]
    log_config_path: String,
}

fn run() -> Result<()> {
    let config = MainConfig::from_args();

    log4rs::init_file(&config.log_config_path, Default::default())
        .chain_err(|| {
                       format!("Unable to initialize log4rs logger with the given config file at '{}'",
                               config.log_config_path)
                   })?;

    info!("Config: {:?}", config);

    let rocket_config = Config::build(Environment::Production)
        .address(config.address.to_owned())
        .port(config.port)
        .finalize()
        .chain_err(|| "Unable to create the custom rocket configuration!")?;

    // set up pinger (client)
    if let Some(ref ping_to) = config.ping_to {
        // for moving into looping thread
        let ping_to = ping_to
            .clone()
            .join("/ping")
            .chain_err(|| "Unable to join /ping to url!")?;

        let ping_to_interval = Duration::from_millis(config.ping_to_interval as u64);
        let name = config.name.to_owned();
        let port = config.port;

        thread::spawn(move || {
            let client = Client::new();

            let ping_req = PingReq {
                id: name,
                scheme: HTTP,
                port: port,
            };

            let ping_req_str = serde_json::to_string(&ping_req);

            match ping_req_str {
                Ok(ping_req_str) => {
                    loop {
                        info!("Ping-to: {}", ping_to);
                        let ping_to = ping_to.clone();

                        let res = client
                            .post(ping_to)
                            .body(&ping_req_str)
                            .header(ContentType::json())
                            .send();

                        match res {
                            Ok(_) => (),
                            Err(e) => error!("Pinger error: {}", e),
                        }

                        info!("Sleeping for {:?}...", ping_to_interval);
                        thread::sleep(ping_to_interval);
                    }
                }

                Err(e) => {
                    // handle the error before looping gracefully
                    error!("Client pinger error: {}", e);
                }
            }
        });
    };

    // set up the server and do not reinitialize the logging system
    rocket::custom(rocket_config, false)
        .manage(config)
        .manage(ClientMap::new(HashMap::new()))
        .mount("/", routes![ping, execute, executenb, shutdown])
        .launch();

    Ok(())
}

fn main() {
    match run() {
        Ok(_) => {
            println!("Program completed!");
            process::exit(0)
        }

        Err(ref e) => {
            let stderr = &mut io::stderr();

            writeln!(stderr, "Error: {}", e).expect("Unable to write error into stderr!");

            for e in e.iter().skip(1) {
                writeln!(stderr, "- Caused by: {}", e).expect("Unable to write error causes into stderr!");
            }

            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use HTTP;
    use PingReq;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use url::Url;

    #[test]
    fn url_parse() {
        let socket_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080));

        let ping_req = PingReq {
            id: "server".to_owned(),
            scheme: HTTP,
            port: 17385,
        };

        let url = Url::parse(&format!("http://{}:{}", socket_addr.ip(), ping_req.port)).unwrap();
        assert!(url.host_str().unwrap() == "127.0.0.1");
        assert!(url.port().unwrap() == 17385);
    }
}
