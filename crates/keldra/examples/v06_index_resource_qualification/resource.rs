use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Ingest = 1,
    InitialBuild = 2,
    ColdQuery = 3,
    WarmQuery = 4,
    Mutation = 5,
    IncrementalBuild = 6,
    IncidentQuery = 7,
}

impl Phase {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Ingest,
            2 => Self::InitialBuild,
            3 => Self::ColdQuery,
            4 => Self::WarmQuery,
            5 => Self::Mutation,
            6 => Self::IncrementalBuild,
            7 => Self::IncidentQuery,
            _ => Self::Ingest,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Reading {
    pub rss_bytes: u64,
    pub anonymous_bytes: u64,
    pub sampled_processes: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PhasePeak {
    pub samples: u64,
    pub peak_rss_bytes: u64,
    pub peak_anonymous_bytes: u64,
    pub minimum_sampled_processes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceReport {
    pub targets: Vec<ResourceTarget>,
    pub baseline: Reading,
    pub final_reading: Reading,
    pub peaks: BTreeMap<Phase, PhasePeak>,
}

impl ResourceReport {
    pub fn peak_rss_growth_bytes(&self) -> u64 {
        self.peaks
            .values()
            .map(|peak| peak.peak_rss_bytes)
            .max()
            .unwrap_or(self.baseline.rss_bytes)
            .saturating_sub(self.baseline.rss_bytes)
    }

    pub fn peak_anonymous_growth_bytes(&self) -> u64 {
        self.peaks
            .values()
            .map(|peak| peak.peak_anonymous_bytes)
            .max()
            .unwrap_or(self.baseline.anonymous_bytes)
            .saturating_sub(self.baseline.anonymous_bytes)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceTarget {
    pub label: String,
    pub pid: u32,
}

pub struct ResourceMonitor {
    targets: Arc<Vec<ResourceTarget>>,
    baseline: Reading,
    phase: Arc<AtomicU8>,
    peaks: Arc<Mutex<BTreeMap<Phase, PhasePeak>>>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ResourceMonitor {
    pub fn start(
        pids: &[u32],
        containers: &[String],
        interval: Duration,
        require_all: bool,
    ) -> io::Result<Option<Self>> {
        let mut targets = pids
            .iter()
            .copied()
            .map(|pid| ResourceTarget {
                label: format!("pid:{pid}"),
                pid,
            })
            .collect::<Vec<_>>();
        for container in containers {
            targets.push(ResourceTarget {
                label: format!("container:{container}"),
                pid: container_pid(container)?,
            });
        }
        targets.sort_by_key(|target| target.pid);
        targets.dedup_by_key(|target| target.pid);
        if targets.is_empty() {
            return Ok(None);
        }
        let baseline = read_all(&targets);
        if require_all && baseline.sampled_processes != targets.len() {
            return Err(io::Error::other(format!(
                "sampled {} of {} configured resource processes",
                baseline.sampled_processes,
                targets.len()
            )));
        }

        let targets = Arc::new(targets);
        let phase = Arc::new(AtomicU8::new(Phase::Ingest as u8));
        let peaks = Arc::new(Mutex::new(BTreeMap::new()));
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task_targets = targets.clone();
        let task_phase = phase.clone();
        let task_peaks = peaks.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(10)));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let reading = read_all(&task_targets);
                        record_peak(
                            &task_peaks,
                            Phase::from_u8(task_phase.load(Ordering::Relaxed)),
                            reading,
                        );
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        Ok(Some(Self {
            targets,
            baseline,
            phase,
            peaks,
            shutdown,
            task,
        }))
    }

    pub fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Relaxed);
        let reading = read_all(&self.targets);
        record_peak(&self.peaks, phase, reading);
    }

    pub fn read_now(&self) -> Reading {
        read_all(&self.targets)
    }

    pub async fn finish(self) -> ResourceReport {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
        let final_reading = read_all(&self.targets);
        let peaks = self
            .peaks
            .lock()
            .expect("resource peak lock poisoned")
            .clone();
        ResourceReport {
            targets: self.targets.as_ref().clone(),
            baseline: self.baseline,
            final_reading,
            peaks,
        }
    }
}

fn record_peak(peaks: &Mutex<BTreeMap<Phase, PhasePeak>>, phase: Phase, reading: Reading) {
    let mut peaks = peaks.lock().expect("resource peak lock poisoned");
    let peak = peaks.entry(phase).or_insert_with(|| PhasePeak {
        minimum_sampled_processes: reading.sampled_processes,
        ..PhasePeak::default()
    });
    peak.samples += 1;
    peak.peak_rss_bytes = peak.peak_rss_bytes.max(reading.rss_bytes);
    peak.peak_anonymous_bytes = peak.peak_anonymous_bytes.max(reading.anonymous_bytes);
    peak.minimum_sampled_processes = peak
        .minimum_sampled_processes
        .min(reading.sampled_processes);
}

fn read_all(targets: &[ResourceTarget]) -> Reading {
    targets
        .iter()
        .filter_map(|target| read_process(target.pid).ok())
        .fold(Reading::default(), |mut total, reading| {
            total.rss_bytes = total.rss_bytes.saturating_add(reading.rss_bytes);
            total.anonymous_bytes = total
                .anonymous_bytes
                .saturating_add(reading.anonymous_bytes);
            total.sampled_processes += 1;
            total
        })
}

fn read_process(pid: u32) -> io::Result<Reading> {
    let root = PathBuf::from("/proc").join(pid.to_string());
    let status = fs::read_to_string(root.join("status"))?;
    let rss_bytes = parse_kib(&status, "VmRSS:")?.saturating_mul(1024);
    let anonymous_bytes = fs::read_to_string(root.join("smaps_rollup"))
        .ok()
        .and_then(|rollup| {
            parse_kib(&rollup, "Anonymous:")
                .or_else(|_| parse_kib(&rollup, "Pss_Anon:"))
                .ok()
        })
        .or_else(|| parse_kib(&status, "RssAnon:").ok())
        .unwrap_or(0)
        .saturating_mul(1024);
    Ok(Reading {
        rss_bytes,
        anonymous_bytes,
        sampled_processes: 1,
    })
}

fn parse_kib(input: &str, key: &str) -> io::Result<u64> {
    input
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix(key)?.trim();
            value.split_whitespace().next()?.parse().ok()
        })
        .ok_or_else(|| io::Error::other(format!("{key} is missing")))
}

fn container_pid(name: &str) -> io::Result<u32> {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Pid}}", name])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "docker inspect failed for {name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map_err(|error| io::Error::other(format!("invalid PID for {name}: {error}")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Phase, PhasePeak, Reading, ResourceReport, parse_kib};

    #[test]
    fn parses_proc_memory_lines() {
        let input = "Name:\tkeldra\nVmRSS:\t   1234 kB\nRssAnon:\t987 kB\n";
        assert_eq!(parse_kib(input, "VmRSS:").unwrap(), 1_234);
        assert_eq!(parse_kib(input, "RssAnon:").unwrap(), 987);
        assert!(parse_kib(input, "Missing:").is_err());
    }

    #[test]
    fn reports_growth_above_the_pre_ingest_baseline() {
        let report = ResourceReport {
            targets: Vec::new(),
            baseline: Reading {
                rss_bytes: 100,
                anonymous_bytes: 60,
                sampled_processes: 1,
            },
            final_reading: Reading::default(),
            peaks: BTreeMap::from([
                (
                    Phase::Ingest,
                    PhasePeak {
                        peak_rss_bytes: 150,
                        peak_anonymous_bytes: 75,
                        ..PhasePeak::default()
                    },
                ),
                (
                    Phase::InitialBuild,
                    PhasePeak {
                        peak_rss_bytes: 180,
                        peak_anonymous_bytes: 110,
                        ..PhasePeak::default()
                    },
                ),
            ]),
        };

        assert_eq!(report.peak_rss_growth_bytes(), 80);
        assert_eq!(report.peak_anonymous_growth_bytes(), 50);
    }
}
