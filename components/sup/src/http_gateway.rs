// Copyright (c) 2016 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs, SocketAddr, SocketAddrV4};
use std::ops::{Deref, DerefMut};
use std::option;
use std::result;
use std::str::FromStr;
use std::thread::{self, JoinHandle};

use hcore::service::{ApplicationEnvironment, ServiceGroup};
use iron::prelude::*;
use iron::{headers, status};
use iron::modifiers::Header;
use router::Router;
use serde::Serialize;
use serde_json;

use error::{Result, Error, SupError};
use manager::Manager;
use manager::service::HealthCheck;

static LOGKEY: &'static str = "HG";
const APIDOCS: &'static str = include_str!(concat!(env!("OUT_DIR"), "/api.html"));

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ListenAddr(SocketAddr);

impl ListenAddr {
    pub fn new(ip: IpAddr, port: u16) -> ListenAddr {
        ListenAddr(SocketAddr::new(ip, port))
    }
}

impl Default for ListenAddr {
    fn default() -> ListenAddr {
        ListenAddr(SocketAddr::V4(
            SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 9631),
        ))
    }
}

impl Deref for ListenAddr {
    type Target = SocketAddr;

    fn deref(&self) -> &SocketAddr {
        &self.0
    }
}

impl DerefMut for ListenAddr {
    fn deref_mut(&mut self) -> &mut SocketAddr {
        &mut self.0
    }
}

impl FromStr for ListenAddr {
    type Err = SupError;

    fn from_str(val: &str) -> Result<Self> {
        match SocketAddr::from_str(val) {
            Ok(addr) => Ok(ListenAddr(addr)),
            Err(_) => {
                match IpAddr::from_str(val) {
                    Ok(ip) => {
                        let mut addr = ListenAddr::default();
                        addr.set_ip(ip);
                        Ok(addr)
                    }
                    Err(_) => Err(sup_error!(Error::IPFailed)),
                }
            }
        }
    }
}

impl ToSocketAddrs for ListenAddr {
    type Iter = option::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> io::Result<Self::Iter> {
        self.0.to_socket_addrs()
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

pub struct Server(Iron<Router>, ListenAddr);

impl Server {
    pub fn new(listen_addr: ListenAddr) -> Self {
        let router =
            router!(
            doc: get "/" => doc,
            butterfly: get "/butterfly" => butterfly,
            census: get "/census" => census,
            services: get "/services" => services,
            services_mut: post "/services" => services_mut,
            service: get "/services/:svc/:group" => service,
            service_mut: post "/services/:svc/:group" => service_mut,
            service_org: get "/services/:svc/:group/:org" => service,
            service_org_mut: post "/services/:svc/:group/:org" => service_mut,
            service_config: get "/services/:svc/:group/config" => config,
            service_health: get "/services/:svc/:group/health" => health,
            service_config_org: get "/services/:svc/:group/:org/config" => config,
            service_health_org: get "/services/:svc/:group/:org/health" => health,
        );
        Server(Iron::new(router), listen_addr)
    }

    pub fn start(self) -> Result<JoinHandle<()>> {
        let handle = thread::Builder::new()
            .name("http-gateway".to_string())
            .spawn(move || {
                self.0.http(*self.1).expect(
                    "unable to start http-gateway thread",
                );
            })?;
        Ok(handle)
    }
}

#[derive(Default, Serialize)]
struct HealthCheckBody {
    stdout: String,
    stderr: String,
}

fn butterfly(req: &mut Request) -> IronResult<Response> {
    let mgr = Manager::connect().unwrap();
    render(mgr.butterfly())
}

fn census(req: &mut Request) -> IronResult<Response> {
    let mgr = Manager::connect().unwrap();
    render(mgr.census())
}

fn config(req: &mut Request) -> IronResult<Response> {
    let mgr = Manager::connect().unwrap();
    match build_service_group(req) {
        Ok(sg) => render(mgr.config(&sg)),
        Err(_) => Ok(Response::with(status::BadRequest)),
    }
}

fn doc(_req: &mut Request) -> IronResult<Response> {
    Ok(Response::with(
        (status::Ok, Header(headers::ContentType::html()), APIDOCS),
    ))
}

fn health(req: &mut Request) -> IronResult<Response> {
    let mgr = Manager::connect().unwrap();
    match build_service_group(req) {
        Ok(sg) => render(mgr.health(&sg)),
        Err(_) => Ok(Response::with(status::BadRequest)),
    }
}

fn render<T>(result: Result<T>) -> IronResult<Response>
where
    T: Serialize,
{
    match result {
        Ok(data) => Ok(Response::with((
            status::Ok,
            Header(headers::ContentType::json()),
            serde_json::to_string(&data).unwrap(),
        ))),
        Err(err) => {
            // JW TODO: map these errors - check if this error is ENTITY_NOT_FOUND
            error!("{}", err);
            Ok(Response::with(status::ServiceUnavailable))
        }
    }
}

fn service(req: &mut Request) -> IronResult<Response> {
    let mgr = Manager::connect().unwrap();
    match build_service_group(req) {
        Ok(sg) => render(mgr.service(&sg)),
        Err(_) => Ok(Response::with(status::BadRequest)),
    }
}

fn service_mut(req: &mut Request) -> IronResult<Response> {
    // parse params to figure out what this user is doing
    // authenticate request
    // match on those params to form a message
    let mgr = Manager::connect().unwrap();
    Ok(Response::with(status::Ok))
}

fn services(req: &mut Request) -> IronResult<Response> {
    let mgr = Manager::connect().unwrap();
    render(mgr.services())
}

fn services_mut(req: &mut Request) -> IronResult<Response> {
    // parse params to figure out what this user is doing
    // authenticate request
    // match on those params to form a message
    let mgr = Manager::connect().unwrap();
    Ok(Response::with(status::Ok))
}

fn build_service_group(req: &mut Request) -> Result<ServiceGroup> {
    let app_env = match req.extensions.get::<Router>().unwrap().find(
        "application_environment",
    ) {
        Some(s) => {
            match ApplicationEnvironment::from_str(s) {
                Ok(app_env) => Some(app_env),
                Err(_) => None,
            }
        }
        None => None,
    };
    let sg = ServiceGroup::new(
        app_env.as_ref(),
        req.extensions
            .get::<Router>()
            .unwrap()
            .find("svc")
            .unwrap_or(""),
        req.extensions
            .get::<Router>()
            .unwrap()
            .find("group")
            .unwrap_or(""),
        req.extensions.get::<Router>().unwrap().find("org"),
    )?;
    Ok(sg)
}

impl Into<Response> for HealthCheck {
    fn into(self) -> Response {
        let status: status::Status = self.into();
        Response::with(status)
    }
}

impl Into<status::Status> for HealthCheck {
    fn into(self) -> status::Status {
        match self {
            HealthCheck::Ok | HealthCheck::Warning => status::Ok,
            HealthCheck::Critical => status::ServiceUnavailable,
            HealthCheck::Unknown => status::InternalServerError,
        }
    }
}
