use anyhow::{anyhow, Result};
use kube::{
    core::{DynamicObject, ObjectList},
    discovery::ApiResource,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::RwLock, time::Instant};

use crate::callback_handler::kubernetes::{reflector::Reflector, ApiVersionKind, KubeResource};

use super::{selector::Selector, store::Store};

#[derive(Clone)]
pub(crate) struct Client {
    kube_client: kube::Client,
    db_pool: sqlx::SqlitePool,
    kube_resources: Arc<RwLock<HashMap<ApiVersionKind, KubeResource>>>,
    reflectors: Arc<RwLock<HashMap<ApiResource, Reflector>>>,
}

impl Client {
    pub(crate) fn new(kube_client: kube::Client, db_pool: sqlx::SqlitePool) -> Self {
        Self {
            kube_client,
            db_pool,
            kube_resources: Arc::new(RwLock::new(HashMap::new())),
            reflectors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) async fn list_resources_by_namespace(
        &mut self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        label_selector: Option<String>,
        field_selector: Option<String>,
    ) -> Result<ObjectList<DynamicObject>> {
        let resource = self.build_kube_resource(api_version, kind).await?;
        if !resource.namespaced {
            return Err(anyhow!("resource {api_version}/{kind} is cluster wide. Cannot search for it inside of a namespace"));
        }

        let store = self.get_reflector_store(resource.resource).await?;

        let label_selector = label_selector
            .map(|ls| Selector::from_string(&ls))
            .transpose()?;
        let field_selector = field_selector
            .map(|fs| Selector::from_string(&fs))
            .transpose()?;

        let resources = store
            .list_objects(Some(namespace), label_selector, field_selector)
            .await?;

        Ok(resources)
    }

    pub(crate) async fn list_resources_all(
        &mut self,
        api_version: &str,
        kind: &str,
        label_selector: Option<String>,
        field_selector: Option<String>,
    ) -> Result<ObjectList<kube::core::DynamicObject>> {
        let resource = self.build_kube_resource(api_version, kind).await?;

        let store = self.get_reflector_store(resource.resource).await?;

        let label_selector = label_selector
            .map(|ls| Selector::from_string(&ls))
            .transpose()?;
        let field_selector = field_selector
            .map(|fs| Selector::from_string(&fs))
            .transpose()?;

        let resources = store
            .list_objects(None, label_selector, field_selector)
            .await?;

        Ok(resources)
    }

    pub(crate) async fn has_list_resources_all_result_changed_since_instant(
        &mut self,
        api_version: &str,
        kind: &str,
        since: Instant,
    ) -> Result<bool> {
        let resource = self.build_kube_resource(api_version, kind).await?;

        Ok(self
            .have_reflector_resources_changed_since(&resource, since)
            .await)
    }

    pub(crate) async fn get_resource(
        &mut self,
        api_version: &str,
        kind: &str,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<DynamicObject> {
        let resource = self.build_kube_resource(api_version, kind).await?;

        if resource.namespaced && namespace.is_none() {
            return Err(anyhow!(
                "Resource {}/{} is namespaced, but no namespace was provided",
                api_version,
                kind
            ));
        };

        let store = self.get_reflector_store(resource.resource).await?;
        let resource = store.get_object(name, namespace).await?;

        Ok(resource)

        // api.get_opt(name)
        //     .await
        //     .map_err(anyhow::Error::new)?
        //     .ok_or_else(|| anyhow!("Cannot find {api_version}/{kind} named '{name}' inside of namespace '{namespace:?}'"))
    }

    pub(crate) async fn get_resource_plural_name(
        &mut self,
        api_version: &str,
        kind: &str,
    ) -> Result<String> {
        let resource = self.build_kube_resource(api_version, kind).await?;
        Ok(resource.resource.plural)
    }

    /// Build a KubeResource using the apiVersion and Kind "coordinates" provided.
    /// The result is then cached locally to avoid further interactions with
    /// the Kubernetes API Server
    async fn build_kube_resource(&mut self, api_version: &str, kind: &str) -> Result<KubeResource> {
        let avk = ApiVersionKind {
            api_version: api_version.to_owned(),
            kind: kind.to_owned(),
        };

        // take a reader lock and search for the resource inside of the
        // known resources
        let kube_resource = {
            let known_resources = self.kube_resources.read().await;
            known_resources.get(&avk).map(|r| r.to_owned())
        };
        if let Some(kr) = kube_resource {
            return Ok(kr);
        }

        // the resource is not known yet, we have to search it
        let resources_list = match api_version {
            "v1" => {
                self.kube_client
                    .list_core_api_resources(api_version)
                    .await?
            }
            _ => self
                .kube_client
                .list_api_group_resources(api_version)
                .await
                .map_err(|e| anyhow!("error finding resource {api_version} / {kind}: {e}"))?,
        };

        let resource = resources_list
            .resources
            .iter()
            .find(|r| r.kind == kind)
            .ok_or_else(|| anyhow!("Cannot find resource {api_version}/{kind}"))?
            .to_owned();

        let (group, version) = match api_version {
            "v1" => ("", "v1"),
            _ => api_version
                .split_once('/')
                .ok_or_else(|| anyhow!("cannot determine group and version for {api_version}"))?,
        };

        let kube_resource = KubeResource {
            resource: kube::api::ApiResource {
                group: group.to_owned(),
                version: version.to_owned(),
                api_version: api_version.to_owned(),
                kind: kind.to_owned(),
                plural: resource.name,
            },
            namespaced: resource.namespaced,
        };

        // Take a writer lock and cache the resource we just found
        let mut known_resources = self.kube_resources.write().await;
        known_resources.insert(avk, kube_resource.clone());

        Ok(kube_resource)
    }

    async fn get_reflector_store(&mut self, api_resource: ApiResource) -> Result<Store> {
        let store = {
            let reflectors = self.reflectors.read().await;
            reflectors
                .get(&api_resource)
                .map(|reflector| reflector.store.clone())
        };
        if let Some(store) = store {
            return Ok(store);
        }

        let reflector = Reflector::create_and_run(
            self.kube_client.clone(),
            self.db_pool.clone(),
            &api_resource,
        )
        .await?;
        let store = reflector.store.clone();

        {
            let mut reflectors = self.reflectors.write().await;
            reflectors.insert(api_resource, reflector);
        }

        Ok(store)
    }

    /// Check if the resources cached by the reflector have changed since the provided instant
    async fn have_reflector_resources_changed_since(
        &mut self,
        resource: &KubeResource,
        since: Instant,
    ) -> bool {
        let last_change_seen_at = {
            let reflectors = self.reflectors.read().await;
            match reflectors.get(&resource.resource) {
                Some(reflector) => reflector.last_change_seen_at().await,
                None => return true,
            }
        };

        last_change_seen_at > since
    }
}
