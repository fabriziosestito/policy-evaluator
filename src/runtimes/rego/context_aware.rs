use kube::api::ObjectList;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::{mpsc, oneshot};

use crate::{
    callback_requests::{CallbackRequest, CallbackRequestType, CallbackResponse},
    policy_metadata::ContextAwareResource,
    runtimes::rego::{
        errors::{RegoRuntimeError, Result},
        gatekeeper_inventory::GatekeeperInventory,
        opa_inventory::OpaInventory,
    },
};

#[derive(serde::Serialize)]
#[serde(untagged)]
pub(crate) enum KubernetesContext {
    Empty,
    Opa(OpaInventory),
    Gatekeeper(GatekeeperInventory),
}

/// Uses the callback channel to get all the Kubernetes resources defined inside of
/// the cluster whose type is mentioned inside of `allowed_resources`.
///
/// The resources are returned based on the actual RBAC privileges of the client
/// used by the runtime.
pub(crate) fn get_allowed_resources(
    callback_channel: &mpsc::Sender<CallbackRequest>,
    allowed_resources: &BTreeSet<ContextAwareResource>,
) -> Result<BTreeMap<ContextAwareResource, ObjectList<kube::core::DynamicObject>>> {
    let mut kube_resources: BTreeMap<ContextAwareResource, ObjectList<kube::core::DynamicObject>> =
        BTreeMap::new();

    for resource in allowed_resources {
        let resource_list = get_all_resources_by_type(callback_channel, resource)?;
        kube_resources.insert(resource.to_owned(), resource_list);
    }

    Ok(kube_resources)
}

fn get_all_resources_by_type(
    callback_channel: &mpsc::Sender<CallbackRequest>,
    resource_type: &ContextAwareResource,
) -> Result<ObjectList<kube::core::DynamicObject>> {
    let req_type = CallbackRequestType::KubernetesListResourceAll {
        api_version: resource_type.api_version.to_owned(),
        kind: resource_type.kind.to_owned(),
        label_selector: None,
        field_selector: None,
    };

    let response = make_request_via_callback_channel(req_type, callback_channel)?;
    serde_json::from_value::<ObjectList<kube::core::DynamicObject>>(response.payload)
        .map_err(RegoRuntimeError::CallbackConvertList)
}

/// Creates a map that has ContextAwareResource as key, and its plural name as value.
/// For example, the key for {`apps/v1`, `Deployment`} will have `deployments` as value.
/// The map is built by making request via the given callback channel.
pub(crate) fn get_plural_names(
    callback_channel: &mpsc::Sender<CallbackRequest>,
    allowed_resources: &BTreeSet<ContextAwareResource>,
) -> Result<BTreeMap<ContextAwareResource, String>> {
    let mut plural_names_by_resource: BTreeMap<ContextAwareResource, String> = BTreeMap::new();

    for resource in allowed_resources {
        let req_type = CallbackRequestType::KubernetesGetResourcePluralName {
            api_version: resource.api_version.to_owned(),
            kind: resource.kind.to_owned(),
        };

        let response = make_request_via_callback_channel(req_type, callback_channel)?;
        let plural_name = serde_json::from_value::<String>(response.payload)
            .map_err(RegoRuntimeError::CallbackGetPluralName)?;

        plural_names_by_resource.insert(resource.to_owned(), plural_name);
    }

    Ok(plural_names_by_resource)
}

/// Internal helper function that sends a request over the callback channel and returns the
/// response
fn make_request_via_callback_channel(
    request_type: CallbackRequestType,
    callback_channel: &mpsc::Sender<CallbackRequest>,
) -> Result<CallbackResponse> {
    let (tx, mut rx) = oneshot::channel::<std::result::Result<CallbackResponse, wasmtime::Error>>();
    let req = CallbackRequest {
        request: request_type,
        response_channel: tx,
    };
    callback_channel
        .try_send(req)
        .map_err(|e| RegoRuntimeError::CallbackSend(e.to_string()))?;

    loop {
        // Note: we cannot use `rx.blocking_recv`. The code would compile, but at runtime we would
        // have a panic because this function is used inside of an async block. The `blocking_recv`
        // method causes the tokio reactor to stop, which leads to a panic
        match rx.try_recv() {
            Ok(msg) => return msg.map_err(RegoRuntimeError::CallbackRequest),
            Err(oneshot::error::TryRecvError::Empty) => {
                //  do nothing, keep waiting for a reply
            }
            Err(e) => return Err(RegoRuntimeError::CallbackResponse(e.to_string())),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use anyhow::{anyhow, Result};
    use assert_json_diff::assert_json_eq;
    use std::path::Path;

    pub fn dynamic_object_from_fixture(
        resource_type: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<kube::core::DynamicObject> {
        let path = Path::new("tests/data/fixtures/kube_context")
            .join(resource_type)
            .join(namespace.unwrap_or_default())
            .join(format!("{name}.json"));
        let contents = std::fs::read(path.clone())
            .map_err(|e| anyhow!("canont read fixture from path: {path:?}: {e}"))?;
        serde_json::from_slice::<kube::core::DynamicObject>(&contents)
            .map_err(|e| anyhow!("json conversion error: {e}"))
    }

    pub fn object_list_from_dynamic_objects(
        objs: &[kube::core::DynamicObject],
    ) -> Result<ObjectList<kube::core::DynamicObject>> {
        let obj_type = objs[0].types.clone().expect("object types should be set");
        Ok(ObjectList {
            items: objs.to_owned(),
            types: obj_type,
            metadata: Default::default(),
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_all_resources_success() {
        let (callback_tx, mut callback_rx) = mpsc::channel::<CallbackRequest>(10);
        let resource = ContextAwareResource {
            api_version: "v1".to_string(),
            kind: "Service".to_string(),
        };
        let expected_resource = resource.clone();
        let services = [
            dynamic_object_from_fixture("services", Some("kube-system"), "kube-dns").unwrap(),
            dynamic_object_from_fixture("services", Some("kube-system"), "metrics-server").unwrap(),
        ];
        let services_list = object_list_from_dynamic_objects(&services).unwrap();

        tokio::spawn(async move {
            let req = match callback_rx.recv().await {
                Some(r) => r,
                None => return,
            };
            match req.request {
                CallbackRequestType::KubernetesListResourceAll {
                    api_version,
                    kind,
                    label_selector,
                    field_selector,
                } => {
                    assert_eq!(api_version, expected_resource.api_version);
                    assert_eq!(kind, expected_resource.kind);
                    assert!(label_selector.is_none());
                    assert!(field_selector.is_none());
                }
                _ => {
                    panic!("not the expected request type");
                }
            };

            let services_list = object_list_from_dynamic_objects(&services).unwrap();
            let callback_response = CallbackResponse {
                payload: serde_json::to_value(services_list).unwrap(),
            };

            req.response_channel.send(Ok(callback_response)).unwrap();
        });

        let actual = get_all_resources_by_type(&callback_tx, &resource).unwrap();
        let actual_json = serde_json::to_value(actual).unwrap();
        let expected_json = serde_json::to_value(services_list).unwrap();
        assert_json_eq!(actual_json, expected_json);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_resource_plural_name_success() {
        let (callback_tx, mut callback_rx) = mpsc::channel::<CallbackRequest>(10);
        let resource = ContextAwareResource {
            api_version: "v1".to_string(),
            kind: "Service".to_string(),
        };
        let plural_name = "services";

        let mut resources: BTreeSet<ContextAwareResource> = BTreeSet::new();
        resources.insert(resource.clone());

        let mut expected_names: BTreeMap<ContextAwareResource, String> = BTreeMap::new();
        expected_names.insert(resource.clone(), plural_name.to_string());

        let expected_resource = resource.clone();

        tokio::spawn(async move {
            let req = match callback_rx.recv().await {
                Some(r) => r,
                None => return,
            };
            match req.request {
                CallbackRequestType::KubernetesGetResourcePluralName { api_version, kind } => {
                    assert_eq!(api_version, expected_resource.api_version);
                    assert_eq!(kind, expected_resource.kind);
                }
                _ => {
                    panic!("not the expected request type");
                }
            };

            let callback_response = CallbackResponse {
                payload: serde_json::to_value(plural_name).unwrap(),
            };

            req.response_channel.send(Ok(callback_response)).unwrap();
        });

        let actual = get_plural_names(&callback_tx, &resources).unwrap();
        assert_eq!(actual, expected_names);
    }
}
