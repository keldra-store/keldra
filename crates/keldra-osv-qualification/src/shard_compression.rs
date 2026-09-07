use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::mpsc as tokio_mpsc;

use super::{PreparedShard, ShardBuilder};

pub(super) struct ShardCompressor {
    output: tokio_mpsc::Sender<PreparedShard>,
    jobs: Option<mpsc::SyncSender<(u64, ShardBuilder)>>,
    completed: mpsc::Receiver<(u64, Result<PreparedShard>)>,
    workers: Vec<thread::JoinHandle<()>>,
    pub(super) in_flight: usize,
    pub(super) maximum_in_flight: usize,
    next_submit: u64,
    next_emit: u64,
    ready: BTreeMap<u64, Result<PreparedShard>>,
}

impl ShardCompressor {
    pub(super) fn start(
        worker_count: usize,
        output: tokio_mpsc::Sender<PreparedShard>,
    ) -> Result<Self> {
        ensure!(
            worker_count > 0,
            "shard compression requires at least one worker"
        );
        let (jobs, job_receiver) = mpsc::sync_channel::<(u64, ShardBuilder)>(worker_count);
        let job_receiver = Arc::new(Mutex::new(job_receiver));
        let (completed_sender, completed) = mpsc::channel();
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let jobs = Arc::clone(&job_receiver);
            let completed = completed_sender.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("osv-shard-compressor-{worker_index}"))
                    .spawn(move || {
                        loop {
                            let job = {
                                let receiver = match jobs.lock() {
                                    Ok(receiver) => receiver,
                                    Err(_) => return,
                                };
                                receiver.recv()
                            };
                            let Ok((job_index, builder)) = job else {
                                return;
                            };
                            if completed
                                .send((job_index, builder.finish().context("compress OSV shard")))
                                .is_err()
                            {
                                return;
                            }
                        }
                    })
                    .context("start OSV shard compression worker")?,
            );
        }
        drop(completed_sender);
        Ok(Self {
            output,
            jobs: Some(jobs),
            completed,
            workers,
            in_flight: 0,
            maximum_in_flight: worker_count.saturating_mul(2),
            next_submit: 0,
            next_emit: 0,
            ready: BTreeMap::new(),
        })
    }

    pub(super) fn submit(&mut self, builder: ShardBuilder) -> Result<()> {
        self.jobs
            .as_ref()
            .context("OSV shard compression pool is closed")?
            .send((self.next_submit, builder))
            .map_err(|_| anyhow::anyhow!("OSV shard compression pool stopped"))?;
        self.next_submit = self
            .next_submit
            .checked_add(1)
            .context("OSV shard compression job identity overflowed")?;
        self.in_flight += 1;
        if self.in_flight >= self.maximum_in_flight {
            self.receive_one()?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<()> {
        self.jobs.take();
        let mut first_error = None;
        while self.in_flight != 0 {
            if let Err(error) = self.receive_one()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        for worker in self.workers {
            if worker.join().is_err() && first_error.is_none() {
                first_error = Some(anyhow::anyhow!("OSV shard compression worker panicked"));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn receive_one(&mut self) -> Result<()> {
        let (job_index, result) = match self.completed.recv() {
            Ok(completed) => completed,
            Err(_) => {
                // Every sender is owned by a worker. Disconnection therefore
                // means no remaining in-flight job can ever produce a result.
                self.in_flight = 0;
                bail!("OSV shard compression pool stopped early");
            }
        };
        self.in_flight -= 1;
        ensure!(
            self.ready.insert(job_index, result).is_none(),
            "OSV shard compression completed one job twice"
        );
        while let Some(result) = self.ready.remove(&self.next_emit) {
            let shard = result?;
            self.output.blocking_send(shard).map_err(|_| {
                anyhow::anyhow!("OSV shard consumer stopped before parsing completed")
            })?;
            self.next_emit += 1;
        }
        Ok(())
    }
}
