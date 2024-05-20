use anyhow::Result;
use policy_fetcher::sigstore::trust::ManualTrustRoot;
use policy_fetcher::sources::Sources;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use super::{oci, sigstore_verification};
use crate::callback_handler::{kubernetes::Client, CallbackHandler};
use crate::callback_requests::CallbackRequest;

const DEFAULT_CHANNEL_BUFF_SIZE: usize = 100;

/// Helper struct that creates CallbackHandler objects
pub struct CallbackHandlerBuilder {
    oci_sources: Option<Sources>,
    channel_buffer_size: usize,
    shutdown_channel: oneshot::Receiver<()>,
    trust_root: Option<Arc<ManualTrustRoot<'static>>>,
    kube_client: Option<kube::Client>,
    db_pool: sqlx::SqlitePool,
}

impl CallbackHandlerBuilder {
    pub fn new(shutdown_channel: oneshot::Receiver<()>) -> Self {
        CallbackHandlerBuilder {
            oci_sources: None,
            shutdown_channel,
            channel_buffer_size: DEFAULT_CHANNEL_BUFF_SIZE,
            trust_root: None,
            kube_client: None,
            db_pool: sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
        }
    }

    /// Provide all the information needed to access OCI registries. Optional
    pub fn registry_config(mut self, sources: Option<Sources>) -> Self {
        self.oci_sources = sources;
        self
    }

    pub fn trust_root(mut self, trust_root: Option<Arc<ManualTrustRoot<'static>>>) -> Self {
        self.trust_root = trust_root;
        self
    }

    /// Set the size of the channel used by the sync world to communicate with
    /// the CallbackHandler. Optional
    pub fn channel_buffer_size(mut self, size: usize) -> Self {
        self.channel_buffer_size = size;
        self
    }

    /// Set the `kube::Client` to be used by context aware policies.
    /// Optional, but strongly recommended to have context aware policies
    /// work as expected
    pub fn kube_client(mut self, client: kube::Client) -> Self {
        self.kube_client = Some(client);
        self
    }

    pub fn db_pool(mut self, pool: sqlx::SqlitePool) -> Self {
        self.db_pool = pool;
        self
    }

    /// Create a CallbackHandler object
    pub async fn build(self) -> Result<CallbackHandler> {
        let (tx, rx) = mpsc::channel::<CallbackRequest>(self.channel_buffer_size);
        let oci_client = Arc::new(oci::Client::new(self.oci_sources.clone()));
        let sigstore_client =
            sigstore_verification::Client::new(self.oci_sources.clone(), self.trust_root.clone())
                .await?
                .to_owned();

        let kubernetes_client = if let Some(kube_client) = self.kube_client {
            Some(Client::new(kube_client, self.db_pool))
        } else {
            None
        };

        Ok(CallbackHandler {
            oci_client,
            sigstore_client,
            kubernetes_client,
            tx,
            rx,
            shutdown_channel: self.shutdown_channel,
        })
    }
}
