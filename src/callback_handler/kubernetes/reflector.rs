use anyhow::{anyhow, Result};
use futures::{future::ready, Stream, StreamExt, TryStreamExt};
use kube::{
    core::DynamicObject,
    discovery::ApiResource,
    runtime::{watcher, WatchStreamExt},
    ResourceExt,
};
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::{sync::watch, time::Instant};
use tracing::{debug, error, info, warn};

use crate::callback_handler::kubernetes::store::Store;

/// A reflector fetches kubernetes objects based on filtering criteria.
/// When created, the list is populated slowly, to prevent hammering the Kubernetes API server.
/// The items are stored in-memory. The `managedFields` attribute is stripped from all the objects
/// to reduce memory consumption. All the other fields are retained.
/// A Kubernetes Watch is then created to keep the contents of the list updated.
///
/// This is code relies heavily on the `kube::runtime::reflector` module.
///
/// ## Stale date
///
/// There's always some delay involved with Kubernetes notifications. That depends on
/// different factors like: the load on the Kubernetes API server, the number of watchers to be
/// notifies,... That means, changes are not propagated immediately, hence the cache can have stale
/// data.
///
/// Finally, when started, the Reflector takes some time to make the loaded data available to
/// consumers.
pub(crate) struct Reflector {
    pub(crate) store: Store,
    last_change_seen_at: watch::Receiver<Instant>,
}

impl Reflector {
    /// Create the reflector and start a tokio task in the background that keeps
    /// the contents of the Reflector updated
    pub(crate) async fn create_and_run(
        kube_client: kube::Client,
        db_pool: sqlx::SqlitePool,
        api_resource: &ApiResource,
    ) -> Result<Self> {
        let store = Store::new(api_resource.clone(), db_pool).await?;

        let group = api_resource.group.clone();
        let version = api_resource.version.clone();
        let kind = api_resource.kind.clone();

        info!(group, version, kind, "creating new reflector");

        let api = kube::api::Api::<kube::core::DynamicObject>::all_with(kube_client, api_resource);

        let stream = watcher(api, watcher::Config::default()).map_ok(|ev| {
            ev.modify(|obj| {
                // clear managed fields to reduce memory usage
                obj.managed_fields_mut().clear();
            })
        });

        // this is a watch channel that tracks the last time the reflector saw a change
        let (updated_at_watch_tx, updated_at_watch_rx) = watch::channel(Instant::now());
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
        let rf = reflector(store.clone(), stream);

        let ready_tx = Mutex::new(Some(ready_tx));

        tokio::task::spawn(async move {
            let infinite_watch = rf
                .take_while(|obj| match obj {
                    Err(watcher::Error::InitialListFailed(err)) => {
                        error!(error = ?err, "watcher error: initial list failed");
                        if let Some(ready_tx) = ready_tx.lock().unwrap().take() {
                            ready_tx.send(Err(anyhow!(err.to_string()))).unwrap();
                        }

                        ready(false)
                    }
                    _ => {
                        if let Err(err) = updated_at_watch_tx.send(Instant::now()) {
                            warn!(error = ?err, "failed to set last_change_seen_at");
                        };

                        if let Some(ready_tx) = ready_tx.lock().unwrap().take() {
                            ready_tx.send(Ok(())).unwrap();
                        }

                        ready(true)
                    }
                })
                .default_backoff()
                .touched_objects()
                .for_each(|obj| {
                    match obj {
                        Ok(o) => debug!(
                            group,
                            version,
                            kind,
                            object=?o,
                            "watcher saw object"
                        ),
                        Err(e) => error!(
                            group,
                            version,
                            kind,
                            error=?e,
                            "watcher error"

                        ),
                    };
                    ready(())
                });
            infinite_watch.await
        });

        ready_rx.await??;

        Ok(Reflector {
            store,
            last_change_seen_at: updated_at_watch_rx,
        })
    }

    /// Get the last time a change was seen by the reflector
    pub(crate) async fn last_change_seen_at(&self) -> Instant {
        *self.last_change_seen_at.borrow()
    }
}

fn reflector<W>(store: Store, stream: W) -> impl Stream<Item = W::Item>
where
    W: Stream<Item = watcher::Result<watcher::Event<DynamicObject>>>,
{
    stream.and_then(move |event| {
        let store = store.clone();

        async move {
            match event {
                watcher::Event::Applied(ref object) => {
                    store.insert_or_replace_object(object).await.unwrap();
                }
                watcher::Event::Deleted(ref object) => {
                    store.delete_object(object).await.unwrap();
                }
                watcher::Event::Restarted(ref objects) => {
                    store.replace_objects(objects).await.unwrap();
                }
            }
            Ok(event)
        }
    })
}
