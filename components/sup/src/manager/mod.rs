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

pub mod service;
#[macro_use]
mod debug;
mod events;
mod periodic;
mod self_updater;
mod file_watcher;
mod peer_watcher;
mod sys;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use butterfly;
use butterfly::member::Member;
use butterfly::trace::Trace;
use butterfly::server::timing::Timing;
use butterfly::server::Suitability;
use hcore::crypto::{default_cache_key_path, SymKey};
use hcore::env;
use hcore::fs::FS_ROOT_PATH;
use hcore::service::ServiceGroup;
use hcore::os::process::{self, Pid, Signal};
use hcore::package::{Identifiable, PackageIdent, PackageInstall};
use launcher_client::{LAUNCHER_LOCK_CLEAN_ENV, LAUNCHER_PID_ENV, LauncherMgr};
use serde;
use time::{self, Timespec, Duration as TimeDuration};
use zmq;

pub use self::service::{CompositeSpec, Service, ServiceBind, ServiceSpec, UpdateStrategy, Topology};
pub use self::sys::Sys;
use self::self_updater::{SUP_PKG_IDENT, SelfUpdater};
use self::service::{Cfg, HealthCheck, Pkg, ProcessState, ServiceCli, StartStyle};
use self::peer_watcher::PeerWatcher;
use {SOCKET_CONTEXT, VERSION};
use error::{Error, Result, SupError};
use config::GossipListenAddr;
use census::CensusRing;
use http_gateway;

const MEMBER_ID_FILE: &'static str = "MEMBER_ID";
const PROC_LOCK_FILE: &'static str = "LOCK";

static LOGKEY: &'static str = "MR";

lazy_static! {
    /// The root path containing all runtime service directories and files
    pub static ref STATE_PATH_PREFIX: PathBuf = {
        Path::new(&*FS_ROOT_PATH).join("hab/sup")
    };
}

/// FileSystem paths that the Manager uses to persist data to disk.
///
/// This is shared with the `http_gateway` and `service` modules for reading and writing
/// persistence data.
#[derive(Debug, Serialize)]
pub struct FsCfg {
    data_path: PathBuf,
    composites_path: PathBuf,
    member_id_file: PathBuf,
    proc_lock_file: PathBuf,
}

impl FsCfg {
    fn new<T>(sup_svc_root: T) -> Self
    where
        T: Into<PathBuf>,
    {
        let sup_svc_root = sup_svc_root.into();
        let data_path = sup_svc_root.join("data");
        FsCfg {
            composites_path: sup_svc_root.join("composites"),
            data_path: data_path,
            member_id_file: sup_svc_root.join(MEMBER_ID_FILE),
            proc_lock_file: sup_svc_root.join(PROC_LOCK_FILE),
        }
    }
}

pub struct ManagerCli(zmq::Socket);

impl ManagerCli {
    pub fn butterfly(&self) -> Result<butterfly::Server> {
        unimplemented!();
    }

    pub fn census(&self) -> Result<CensusRing> {
        unimplemented!();
    }

    pub fn config(&self, service_group: &ServiceGroup) -> Result<Cfg> {
        unimplemented!();
    }

    pub fn health(&self, service_group: &ServiceGroup) -> Result<HealthCheck> {
        unimplemented!();
    }

    pub fn service(&self, service_group: &ServiceGroup) -> Result<Service> {
        unimplemented!();
    }

    pub fn services(&self) -> Result<Vec<Service>> {
        unimplemented!();
    }
}

#[derive(Clone, Default)]
pub struct ManagerConfig {
    pub auto_update: bool,
    pub eventsrv_group: Option<ServiceGroup>,
    pub update_url: String,
    pub update_channel: String,
    pub gossip_listen: GossipListenAddr,
    pub http_listen: http_gateway::ListenAddr,
    pub gossip_peers: Vec<SocketAddr>,
    pub gossip_permanent: bool,
    pub ring: Option<String>,
    pub name: Option<String>,
    pub organization: Option<String>,
    pub watch_peer_file: Option<String>,

    custom_state_path: Option<PathBuf>,
}

pub struct Manager {
    butterfly: butterfly::Server,
    census_ring: Arc<RwLock<CensusRing>>,
    cli_sock: zmq::Socket,
    events_group: Option<ServiceGroup>,
    fs_cfg: Arc<FsCfg>,
    services: HashSet<ServiceCli>,
    organization: Option<String>,
    peer_watcher: Option<PeerWatcher>,
    self_updater: Option<SelfUpdater>,
    service_states: HashMap<PackageIdent, Timespec>,
    sys: Arc<Sys>,
}

impl Manager {
    pub fn connect() -> Result<ManagerCli> {
        unimplemented!();
    }

    /// Determines if there is already a Habitat Supervisor running on the host system.
    pub fn is_running(cfg: &ManagerConfig) -> Result<bool> {
        let state_path = Self::state_path_from(&cfg);
        let fs_cfg = FsCfg::new(state_path);

        match read_process_lock(&fs_cfg.proc_lock_file) {
            Ok(pid) => Ok(process::is_alive(pid)),
            Err(SupError { err: Error::ProcessLockCorrupt, .. }) => Ok(false),
            Err(SupError { err: Error::ProcessLockIO(_, _), .. }) => {
                // JW TODO: We need to check the raw OS error and translate it to a "file not found"
                // case. This is an acceptable reason to assume that another manager is not running
                // but other IO errors are an actual problem. For now, let's just assume an IO
                // error here is a file not found.
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    /// Load a Manager with the given configuration.
    ///
    /// The returned Manager will be pre-populated with any cached data from disk from a previous
    /// run if available.
    pub fn load(cfg: ManagerConfig) -> Result<Manager> {
        let state_path = Self::state_path_from(&cfg);
        Self::create_state_path_dirs(&state_path)?;
        Self::clean_dirty_state(&state_path)?;
        let fs_cfg = FsCfg::new(state_path);
        if env::var(LAUNCHER_LOCK_CLEAN_ENV).is_ok() {
            release_process_lock(&fs_cfg);
        }
        obtain_process_lock(&fs_cfg)?;
        Self::new(cfg, fs_cfg)
    }

    pub fn term(cfg: &ManagerConfig) -> Result<()> {
        let state_path = Self::state_path_from(&cfg);
        let fs_cfg = FsCfg::new(state_path);
        match read_process_lock(&fs_cfg.proc_lock_file) {
            Ok(pid) => {
                process::signal(pid, Signal::TERM).map_err(|_| {
                    sup_error!(Error::SignalFailed)
                })?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Read all spec files and rewrite them to disk migrating their format from a previous
    /// Supervisor's to the one currently running.
    fn migrate_specs(fs_cfg: &FsCfg) {
        // JW: In the future we should write spec files to the Supervisor's DAT file in a more
        // appropriate machine readable format. We'll need to wait until we modify how we load and
        // unload services, though. Right now we watch files on disk and communicate with the
        // Supervisor asynchronously. We need to move to communicating directly with the
        // Supervisor's main loop through IPC.
        unimplemented!()
    }

    fn new(cfg: ManagerConfig, fs_cfg: FsCfg) -> Result<Manager> {
        // JW TODO: handle socket errors
        let cli_sock = (**SOCKET_CONTEXT).socket(zmq::ROUTER).unwrap();
        cli_sock.set_router_mandatory(true).unwrap();
        let current = PackageIdent::from_str(&format!("{}/{}", SUP_PKG_IDENT, VERSION)).unwrap();
        let self_updater = if cfg.auto_update {
            if current.fully_qualified() {
                Some(SelfUpdater::new(
                    current,
                    cfg.update_url,
                    cfg.update_channel,
                ))
            } else {
                warn!("Supervisor version not fully qualified, unable to start self-updater");
                None
            }
        } else {
            None
        };
        let mut sys = Sys::new(cfg.gossip_permanent, cfg.gossip_listen, cfg.http_listen);
        let member = Self::load_member(&mut sys, &fs_cfg)?;
        let ring_key = match cfg.ring {
            Some(ref ring_with_revision) => {
                outputln!("Joining ring {}", ring_with_revision);
                Some(SymKey::get_pair_for(
                    &ring_with_revision,
                    &default_cache_key_path(None),
                )?)
            }
            None => None,
        };
        let services = HashSet::new();
        let server = butterfly::Server::new(
            sys.gossip_listen(),
            sys.gossip_listen(),
            member,
            Trace::default(),
            ring_key,
            None,
            Some(&fs_cfg.data_path),
            Box::new(SuitabilityLookup),
        )?;
        outputln!("Supervisor Member-ID {}", sys.member_id);
        for peer_addr in &cfg.gossip_peers {
            let mut peer = Member::default();
            peer.set_address(format!("{}", peer_addr.ip()));
            peer.set_swim_port(peer_addr.port() as i32);
            peer.set_gossip_port(peer_addr.port() as i32);
            server.member_list.add_initial_member(peer);
        }
        Self::migrate_specs(&fs_cfg);
        let peer_watcher = if let Some(path) = cfg.watch_peer_file {
            Some(PeerWatcher::run(path)?)
        } else {
            None
        };
        let census = Arc::new(RwLock::new(CensusRing::new(sys.member_id.clone())));
        Ok(Manager {
            cli_sock: cli_sock,
            self_updater: self_updater,
            census_ring: census,
            butterfly: server,
            events_group: cfg.eventsrv_group,
            services: services,
            fs_cfg: Arc::new(fs_cfg),
            organization: cfg.organization,
            service_states: HashMap::new(),
            sys: Arc::new(sys),
            peer_watcher: peer_watcher,
        })
    }

    /// Load the initial Butterly Member which is used in initializing the Butterfly server. This
    /// will load the member-id for the initial Member from disk if a previous manager has been
    /// run.
    ///
    /// The mutable ref to `Sys` will be configured with Butterfly Member details and will also
    /// populate the initial Member.
    fn load_member(sys: &mut Sys, fs_cfg: &FsCfg) -> Result<Member> {
        let mut member = Member::default();
        match File::open(&fs_cfg.member_id_file) {
            Ok(mut file) => {
                let mut member_id = String::new();
                file.read_to_string(&mut member_id).map_err(|e| {
                    sup_error!(Error::BadDataFile(fs_cfg.member_id_file.clone(), e))
                })?;
                member.set_id(member_id);
            }
            Err(_) => {
                match File::create(&fs_cfg.member_id_file) {
                    Ok(mut file) => {
                        file.write(member.get_id().as_bytes()).map_err(|e| {
                            sup_error!(Error::BadDataFile(fs_cfg.member_id_file.clone(), e))
                        })?;
                    }
                    Err(err) => {
                        return Err(sup_error!(
                            Error::BadDataFile(fs_cfg.member_id_file.clone(), err)
                        ))
                    }
                }
            }
        }
        sys.member_id = member.get_id().to_string();
        member.set_persistent(sys.permanent);
        Ok(member)
    }

    pub fn composite_path_for(cfg: &ManagerConfig, spec: &CompositeSpec) -> PathBuf {
        Self::composites_path(&Self::state_path_from(cfg)).join(spec.file_name())
    }

    // TODO (CM): BAAAAARF
    pub fn composite_path_by_ident(cfg: &ManagerConfig, ident: &PackageIdent) -> PathBuf {
        let mut p = Self::composites_path(&Self::state_path_from(cfg)).join(&ident.name);
        p.set_extension("spec");
        p
    }

    pub fn save_composite_spec_for(cfg: &ManagerConfig, spec: &CompositeSpec) -> Result<()> {
        spec.to_file(Self::composite_path_for(cfg, spec))
    }

    fn clean_dirty_state<T>(state_path: T) -> Result<()>
    where
        T: AsRef<Path>,
    {
        let data_path = Self::data_path(&state_path);
        debug!("Cleaning cached health checks");
        match fs::read_dir(&data_path) {
            Ok(entries) => {
                for entry in entries {
                    if let Ok(entry) = entry {
                        match entry.path().extension().and_then(|p| p.to_str()) {
                            Some("tmp") | Some("health") => {
                                fs::remove_file(&entry.path()).map_err(|err| {
                                    sup_error!(Error::BadDataPath(data_path.clone(), err))
                                })?;
                            }
                            _ => continue,
                        }
                    }
                }
                Ok(())
            }
            Err(err) => Err(sup_error!(Error::BadDataPath(data_path, err))),
        }
    }

    fn create_state_path_dirs<T>(state_path: T) -> Result<()>
    where
        T: AsRef<Path>,
    {
        let data_path = Self::data_path(&state_path);
        debug!("Creating data directory: {}", data_path.display());
        if let Some(err) = fs::create_dir_all(&data_path).err() {
            return Err(sup_error!(Error::BadDataPath(data_path, err)));
        }

        let composites_path = Self::composites_path(&state_path);
        debug!(
            "Creating composites directory: {}",
            composites_path.display()
        );
        if let Some(err) = fs::create_dir_all(&composites_path).err() {
            return Err(sup_error!(Error::BadCompositesPath(composites_path, err)));
        }

        Ok(())
    }

    #[inline]
    fn data_path<T>(state_path: T) -> PathBuf
    where
        T: AsRef<Path>,
    {
        state_path.as_ref().join("data")
    }

    #[inline]
    fn composites_path<T>(state_path: T) -> PathBuf
    where
        T: AsRef<Path>,
    {
        state_path.as_ref().join("composites")
    }

    fn state_path_from(cfg: &ManagerConfig) -> PathBuf {
        match cfg.custom_state_path {
            Some(ref custom) => custom.clone(),
            None => {
                match cfg.name {
                    Some(ref name) => STATE_PATH_PREFIX.join(name),
                    None => STATE_PATH_PREFIX.join("default"),
                }
            }
        }
    }

    fn add_service(&mut self, spec: ServiceSpec) {
        outputln!("Starting {}", &spec.ident);
        // JW TODO: This clone sucks, but our data structures are a bit messy here. What we really
        // want is the service to hold the spec and, on failure, return an error with the spec
        // back to us. Since we consume and deconstruct the spec in `Service::new()` which
        // `Service::load()` eventually delegates to we just can't have that. We should clean
        // this up in the future.
        let service = match Service::load(
            self.sys.clone(),
            self.census_ring.clone(),
            spec.clone(),
            self.organization.as_ref().map(|org| &**org),
        ) {
            Ok(service) => service,
            Err(err) => {
                outputln!("Unable to start {}, {}", &spec.ident, err);
                return;
            }
        };
        self.services.insert(service);
    }

    pub fn run(&mut self) -> Result<()> {
        self.start_initial_services()?;
        outputln!(
            "Starting gossip-listener on {}",
            self.butterfly.gossip_addr()
        );
        self.butterfly.start(Timing::default())?;
        debug!("gossip-listener started");
        let http_listen_addr = self.sys.http_listen();
        outputln!("Starting http-gateway on {}", &http_listen_addr);
        http_gateway::Server::new(http_listen_addr).start()?;
        debug!("http-gateway started");
        let events = match self.events_group {
            Some(ref evg) => Some(events::EventsMgr::start(evg.clone())),
            None => None,
        };
        loop {
            let next_check = time::get_time() + TimeDuration::milliseconds(1000);
            if LauncherMgr::is_stopping() {
                self.shutdown();
                return Ok(());
            }
            if self.check_for_departure() {
                self.shutdown();
                return Err(sup_error!(Error::Departed));
            }
            if let Some(package) = self.check_for_updated_supervisor() {
                outputln!(
                    "Supervisor shutting down for automatic update to {}",
                    package
                );
                self.shutdown();
                return Ok(());
            }
            self.update_peers_from_watch_file()?;
            self.restart_elections();
            {
                self.census_ring.write().unwrap().update_from_rumors(
                    &self.butterfly.service_store,
                    &self.butterfly.election_store,
                    &self.butterfly.update_store,
                    &self.butterfly.member_list,
                    &self.butterfly.service_config_store,
                    &self.butterfly.service_file_store,
                );
            }

            let census_changed = {
                let census_ring = self.census_ring.read().unwrap();
                if census_ring.changed() {
                    events.as_ref().map(
                        |events| events.try_connect(&census_ring),
                    );
                }
                census_ring.changed()
            };
            if census_changed {
                for service in self.services.iter() {
                    service.tick();
                }
            }

            let time_to_wait = (next_check - time::get_time()).num_milliseconds();
            match self.cli_sock.poll(zmq::POLLIN, time_to_wait) {
                // did we get messages?
            }
        }
    }

    fn check_for_departure(&self) -> bool {
        self.butterfly.is_departed()
    }

    fn check_for_updated_supervisor(&mut self) -> Option<PackageInstall> {
        if let Some(ref mut updater) = self.self_updater {
            return updater.updated();
        }
        None
    }

    fn gossip_latest_service_rumor(&self, service: &Service) {
        let mut incarnation = 1;
        {
            let list = self.butterfly.service_store.list.read().expect(
                "Rumor store lock poisoned",
            );
            if let Some(rumor) = list.get(&*service.service_group).and_then(|r| {
                r.get(&self.sys.member_id)
            })
            {
                incarnation = rumor.clone().get_incarnation() + 1;
            }
        }
        self.butterfly.insert_service(service.to_rumor(incarnation));
    }

    /// Remove the given service from the manager.
    ///
    /// Passing `true` for the term argument will also request the Launcher to terminate the running
    /// service. Passing a value of `false` will let the Launcher keep the service running. This
    /// useful if you want the Supervisor to shutdown temporarily and then come back and re-attach
    /// to all running processes.
    fn remove_service(&self, service: &mut ServiceCli, term: bool) {
        // JW TODO: Update service rumor to remove service from cluster
        if term {
            service.stop();
        }
        if service.start_style() == StartStyle::Transient {
            // JW TODO: drop this from our state
        }
    }

    /// Check if any elections need restarting.
    fn restart_elections(&mut self) {
        self.butterfly.restart_elections();
    }

    fn shutdown(&mut self) {
        outputln!("Gracefully departing from gossip network.");
        self.butterfly.set_departed();
        for mut service in self.services.drain() {
            self.remove_service(&mut service, false);
        }
        release_process_lock(&self.fs_cfg);
    }

    fn start_initial_services(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn update_peers_from_watch_file(&mut self) -> Result<()> {
        if !self.butterfly.need_peer_seeding() {
            return Ok(());
        }
        match self.peer_watcher {
            None => Ok(()),
            Some(ref watcher) => {
                if watcher.has_fs_events() {
                    let members = watcher.get_members()?;
                    self.butterfly.member_list.set_initial_members(members);
                }
                Ok(())
            }
        }
    }
}

#[derive(Deserialize)]
pub struct ProcessStatus {
    #[serde(deserialize_with = "deserialize_time", rename = "state_entered")]
    pub elapsed: TimeDuration,
    pub pid: Option<u32>,
    pub state: ProcessState,
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.pid {
            Some(pid) => {
                write!(
                    f,
                    "state:{}, time:{}, pid:{}",
                    self.state,
                    self.elapsed,
                    pid
                )
            }
            None => write!(f, "state:{}, time:{}", self.state, self.elapsed),
        }

    }
}

#[derive(Deserialize)]
pub struct ServiceStatus {
    pub pkg: Pkg,
    pub process: ProcessStatus,
    pub service_group: ServiceGroup,
    pub start_style: StartStyle,
    pub composite: Option<String>,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} ({}), {}, group:{}, style:{}",
            self.pkg.ident,
            self.composite.as_ref().unwrap_or(&"standalone".to_string()),
            self.process,
            self.service_group,
            self.start_style
        )
    }
}

#[derive(Debug)]
struct SuitabilityLookup;

impl Suitability for SuitabilityLookup {
    fn get(&self, service_group: &ServiceGroup) -> u64 {
        0
    }
}

fn deserialize_time<'de, D>(d: D) -> result::Result<TimeDuration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct FromTimespec;

    impl<'de> serde::de::Visitor<'de> for FromTimespec {
        type Value = TimeDuration;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a i64 integer")
        }

        fn visit_u64<R>(self, value: u64) -> result::Result<TimeDuration, R>
        where
            R: serde::de::Error,
        {
            let tspec = Timespec {
                sec: (value as i64),
                nsec: 0,
            };
            Ok(time::get_time() - tspec)
        }
    }

    d.deserialize_u64(FromTimespec)
}

fn obtain_process_lock(fs_cfg: &FsCfg) -> Result<()> {
    match write_process_lock(&fs_cfg.proc_lock_file) {
        Ok(()) => Ok(()),
        Err(_) => {
            match read_process_lock(&fs_cfg.proc_lock_file) {
                Ok(pid) => {
                    if process::is_alive(pid) {
                        return Err(sup_error!(Error::ProcessLocked(pid)));
                    }
                    release_process_lock(&fs_cfg);
                    write_process_lock(&fs_cfg.proc_lock_file)
                }
                Err(SupError { err: Error::ProcessLockCorrupt, .. }) => {
                    release_process_lock(&fs_cfg);
                    write_process_lock(&fs_cfg.proc_lock_file)
                }
                Err(err) => Err(err),
            }
        }
    }
}

fn read_process_lock<T>(lock_path: T) -> Result<Pid>
where
    T: AsRef<Path>,
{
    match File::open(lock_path.as_ref()) {
        Ok(file) => {
            let reader = BufReader::new(file);
            match reader.lines().next() {
                Some(Ok(line)) => {
                    match line.parse::<Pid>() {
                        Ok(pid) => Ok(pid),
                        Err(_) => Err(sup_error!(Error::ProcessLockCorrupt)),
                    }
                }
                _ => Err(sup_error!(Error::ProcessLockCorrupt)),
            }
        }
        Err(err) => Err(sup_error!(
            Error::ProcessLockIO(lock_path.as_ref().to_path_buf(), err)
        )),
    }
}

fn release_process_lock(fs_cfg: &FsCfg) {
    if let Err(err) = fs::remove_file(&fs_cfg.proc_lock_file) {
        debug!("Couldn't cleanup Supervisor process lock, {}", err);
    }
}

fn write_process_lock<T>(lock_path: T) -> Result<()>
where
    T: AsRef<Path>,
{
    match OpenOptions::new().write(true).create_new(true).open(
        lock_path
            .as_ref(),
    ) {
        Ok(mut file) => {
            let pid = match env::var(LAUNCHER_PID_ENV) {
                Ok(pid) => pid.parse::<Pid>().expect("Unable to parse launcher pid"),
                Err(_) => process::current_pid(),
            };
            match write!(&mut file, "{}", pid) {
                Ok(()) => Ok(()),
                Err(err) => {
                    Err(sup_error!(
                        Error::ProcessLockIO(lock_path.as_ref().to_path_buf(), err)
                    ))
                }
            }
        }
        Err(err) => Err(sup_error!(
            Error::ProcessLockIO(lock_path.as_ref().to_path_buf(), err)
        )),
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{Manager, ManagerConfig, STATE_PATH_PREFIX};

    #[test]
    fn manager_state_path_default() {
        let cfg = ManagerConfig::default();
        let path = Manager::state_path_from(&cfg);

        assert_eq!(
            PathBuf::from(format!("{}/default", STATE_PATH_PREFIX.to_string_lossy())),
            path
        );
    }

    #[test]
    fn manager_state_path_with_name() {
        let mut cfg = ManagerConfig::default();
        cfg.name = Some(String::from("peanuts"));
        let path = Manager::state_path_from(&cfg);

        assert_eq!(
            PathBuf::from(format!("{}/peanuts", STATE_PATH_PREFIX.to_string_lossy())),
            path
        );
    }

    #[test]
    fn manager_state_path_custom() {
        let mut cfg = ManagerConfig::default();
        cfg.custom_state_path = Some(PathBuf::from("/tmp/peanuts-and-cake"));
        let path = Manager::state_path_from(&cfg);

        assert_eq!(PathBuf::from("/tmp/peanuts-and-cake"), path);
    }

    #[test]
    fn manager_state_path_custom_beats_name() {
        let mut cfg = ManagerConfig::default();
        cfg.custom_state_path = Some(PathBuf::from("/tmp/partay"));
        cfg.name = Some(String::from("nope"));
        let path = Manager::state_path_from(&cfg);

        assert_eq!(PathBuf::from("/tmp/partay"), path);
    }
}
