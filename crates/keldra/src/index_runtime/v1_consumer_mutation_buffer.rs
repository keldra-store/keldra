use super::*;

pub(super) fn prepare_page(
    writer: &Writer,
    dispatches: Vec<V1SourceDispatch>,
    safe_next: u64,
) -> Result<(u64, Vec<Mutation>), Status> {
    prepare_dispatches(writer.pending_next, dispatches, safe_next)
}

pub(super) fn prepare_dispatches(
    first: u64,
    dispatches: Vec<V1SourceDispatch>,
    safe_next: u64,
) -> Result<(u64, Vec<Mutation>), Status> {
    if safe_next <= first {
        return Ok((0, Vec::new()));
    }
    let mut units = Vec::new();
    for dispatch in dispatches {
        let (atomic, mut group) = dispatch_mutations(dispatch)?;
        group.retain(|mutation| mutation.offset >= first && mutation.offset < safe_next);
        if !group.is_empty() {
            units.push((atomic, group));
        }
    }
    coalesce_units(0, units)
}

pub(super) fn queue_mutations(
    writer: &mut Writer,
    mutations: Vec<Mutation>,
    safe_next: u64,
) -> Result<(), Status> {
    let arms_age = !mutations.is_empty();
    let needed = mutation_window_needed(
        &writer.pending_mutations,
        writer.pending_mutation_bytes,
        &mutations,
    )?;
    if needed > writer.pending_mutation_capacity {
        return Err(Status::resource_exhausted(format!(
            "v1 mutation window requires {needed} bytes but admits {}",
            writer.pending_mutation_capacity
        )));
    }
    match writer.pending_mutation_permit.grow_to(needed.max(1)) {
        Ok(()) => {}
        Err(MemoryAdmission::ReplayRequired {
            needed_bytes,
            available_bytes,
        }) => {
            return Err(Status::resource_exhausted(format!(
                "v1 mutation window needs {needed_bytes} additional bytes but only {available_bytes} are available"
            )));
        }
        Err(MemoryAdmission::Admitted) => unreachable!(),
    }
    apply_mutation_window(
        &mut writer.pending_mutations,
        &mut writer.pending_mutation_bytes,
        &mut writer.pending_operations,
        &mut writer.pending_next,
        needed,
        mutations,
        safe_next,
    )?;
    writer
        .pending_mutation_permit
        .shrink_to(needed.max(1))
        .map_err(index_status)?;
    if arms_age {
        writer.since.get_or_insert_with(Instant::now);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn queue_mutation_window(
    latest: &mut BTreeMap<String, Mutation>,
    resident_bytes: &mut usize,
    observed_operations: &mut u64,
    next_offset: &mut u64,
    capacity: usize,
    mutations: Vec<Mutation>,
    safe_next: u64,
) -> Result<(), Status> {
    let needed = mutation_window_needed(latest, *resident_bytes, &mutations)?;
    if needed > capacity {
        return Err(Status::resource_exhausted(format!(
            "v1 mutation window requires {needed} bytes but admits {}",
            capacity
        )));
    }
    apply_mutation_window(
        latest,
        resident_bytes,
        observed_operations,
        next_offset,
        needed,
        mutations,
        safe_next,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_mutation_window(
    latest: &mut BTreeMap<String, Mutation>,
    resident_bytes: &mut usize,
    observed_operations: &mut u64,
    next_offset: &mut u64,
    needed: usize,
    mutations: Vec<Mutation>,
    safe_next: u64,
) -> Result<(), Status> {
    let operations = u64::try_from(mutations.len())
        .map_err(|_| Status::resource_exhausted("v1 mutation-window operations overflow"))?;
    for mutation in mutations {
        let replace = latest.get(&mutation.path).is_none_or(|previous| {
            (mutation.offset, mutation.ordinal) > (previous.offset, previous.ordinal)
        });
        if replace {
            let mut mutation = mutation;
            if let Some(previous) = latest.get(&mutation.path) {
                mutation.predecessor_absent_at_window_start =
                    previous.predecessor_absent_at_window_start;
            }
            latest.insert(mutation.path.clone(), mutation);
        }
    }
    *resident_bytes = needed;
    *observed_operations = observed_operations.saturating_add(operations);
    *next_offset = safe_next;
    Ok(())
}

pub(super) fn mutation_window_needed(
    latest: &BTreeMap<String, Mutation>,
    resident_bytes: usize,
    mutations: &[Mutation],
) -> Result<usize, Status> {
    mutations
        .iter()
        .try_fold(resident_bytes, |mut needed, mutation| {
            if let Some(previous) = latest.get(&mutation.path) {
                if (mutation.offset, mutation.ordinal) <= (previous.offset, previous.ordinal) {
                    return Ok(needed);
                }
                needed = needed.saturating_sub(mutation_window_bytes(previous));
            }
            needed
                .checked_add(mutation_window_bytes(mutation))
                .ok_or_else(|| Status::resource_exhausted("v1 mutation-window size overflow"))
        })
}

pub(super) fn mutation_window_bytes(mutation: &Mutation) -> usize {
    std::mem::size_of::<Mutation>()
        .saturating_add(mutation.path.capacity().saturating_mul(2))
        .saturating_add(
            mutation
                .canonical_path
                .as_ref()
                .map_or(0, |path| path.capacity()),
        )
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(4))
}
