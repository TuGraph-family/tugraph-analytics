use std::sync::atomic::{AtomicU64, Ordering};

use crate::{gen::manifest::Manifest, INITIAL_SECOND_KEY_SEQUENCE_ID};

pub struct SequenceId {
    seq_id: AtomicU64,
}

impl Default for SequenceId {
    fn default() -> Self {
        Self {
            seq_id: AtomicU64::new(INITIAL_SECOND_KEY_SEQUENCE_ID),
        }
    }
}

impl SequenceId {
    pub fn archive(&self) -> u64 {
        self.seq_id.load(Ordering::SeqCst)
    }

    pub fn recover(&self, manifest: &Manifest) {
        self.seq_id.store(manifest.seq_id, Ordering::SeqCst);
    }

    pub fn get_next(&self) -> u64 {
        self.seq_id.fetch_add(1, Ordering::SeqCst)
    }
}
