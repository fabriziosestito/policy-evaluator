use kube::{
    core::{DynamicObject, ObjectList},
    discovery::ApiResource,
};
use sqlx::{Execute, QueryBuilder, Result, Row, Sqlite};

use super::selector::{Operator, Selector};

#[derive(Clone)]
pub(crate) struct Store {
    api_resource: ApiResource,
    pool: sqlx::SqlitePool,
}

impl Store {
    pub(crate) async fn new(api_resource: ApiResource, pool: sqlx::SqlitePool) -> Result<Self> {
        let store = Self { api_resource, pool };
        store.create_table().await?;

        Ok(store)
    }

    pub(crate) async fn insert_or_replace_object(&self, object: &DynamicObject) -> Result<()> {
        sqlx::query(&format!(
            r#"
        INSERT OR REPLACE INTO {} (name, namespace, object) VALUES (?, ?, ?);
        "#,
            self.table(),
        ))
        .bind(object.metadata.name.clone())
        .bind(object.metadata.namespace.clone())
        .bind(serde_json::to_value(object).unwrap())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn delete_object(&self, object: &DynamicObject) -> Result<()> {
        sqlx::query(&format!(
            r#"
        DELETE FROM {} WHERE name = ? AND namespace = ?;
        "#,
            self.table(),
        ))
        .bind(object.metadata.name.clone())
        .bind(object.metadata.namespace.clone())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn replace_objects(&self, objects: &[DynamicObject]) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(&format!(
            r#"
        DELETE FROM {};
        "#,
            self.table(),
        ))
        .execute(&mut *tx)
        .await?;

        for object in objects {
            sqlx::query(
                format!(
                    r#"
            INSERT INTO {} (name, namespace, object) VALUES (?, ?, ?);
            "#,
                    self.table(),
                )
                .as_str(),
            )
            .bind(object.metadata.name.clone())
            .bind(object.metadata.namespace.clone())
            .bind(serde_json::to_value(object).unwrap())
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub(crate) async fn list_objects(
        &self,
        namespace: Option<&str>,
        label_selector: Option<Selector>,
        field_selector: Option<Selector>,
    ) -> Result<ObjectList<DynamicObject>> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(&format!(
            r#"
            SELECT object FROM {}
            "#,
            self.table(),
        ));

        build_filters_query(
            &mut query_builder,
            namespace,
            label_selector,
            field_selector,
        );

        let query = query_builder.build();
        dbg!(query.sql());
        let rows = query.fetch_all(&self.pool).await?;

        let objects: Vec<DynamicObject> = rows
            .iter()
            .map(|row| serde_json::from_slice(row.get("object")).unwrap())
            .collect();

        Ok(ObjectList {
            types: kube::core::TypeMeta {
                api_version: self.api_resource.api_version.clone(),
                kind: format!("{}List", self.api_resource.kind),
            },
            metadata: Default::default(),
            items: objects,
        })
    }

    pub(crate) async fn get_object(
        &self,
        name: &str,
        namespace: Option<&str>,
    ) -> Result<DynamicObject> {
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(&format!(
            r#"
            SELECT object FROM {}
            "#,
            self.table(),
        ));

        query_builder
            .push(" WHERE name =")
            .push_bind(name.to_owned());

        if let Some(namespace) = namespace {
            query_builder
                .push(" AND namespace =")
                .push_bind(namespace.to_owned());
        }

        let query = query_builder.build();
        let row = query.fetch_one(&self.pool).await?;

        let object: DynamicObject = serde_json::from_slice(row.get("object")).unwrap();

        Ok(object)
    }

    fn table(&self) -> String {
        format!(
            "{}_{}",
            self.api_resource.api_version.replace(['/', '.'], "_"),
            self.api_resource.plural
        )
    }

    async fn create_table(&self) -> Result<()> {
        sqlx::query(&format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            name VARCHAR(250) NOT NULL,
            namespace VARCHAR(250),
            object JSON NOT NULL,
            PRIMARY KEY(name, namespace)
        );
        "#,
            self.table(),
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

fn build_filters_query(
    query_builder: &mut QueryBuilder<Sqlite>,
    namespace: Option<&str>,
    label_selector: Option<Selector>,
    field_selector: Option<Selector>,
) {
    let mut has_where = false;
    if let Some(namespace) = namespace {
        query_builder
            .push(" WHERE namespace =")
            .push_bind(namespace.to_owned());
        has_where = true;
    }

    if let Some(label_selector) = label_selector {
        for (key, value, operator) in label_selector.iter() {
            if has_where {
                query_builder.push(" AND ");
            } else {
                query_builder.push(" WHERE ");
                has_where = true;
            }

            query_builder
                .push("json_extract(object, ")
                .push_bind(format!("$.metadata.labels.{}", key.to_owned()));

            match operator {
                Operator::Equals => query_builder.push(") ="),
                Operator::NotEquals => query_builder.push(") !="),
            };

            query_builder.push_bind(value.to_owned());
        }
    }

    if let Some(field_selector) = field_selector {
        for (key, value, operator) in field_selector.iter() {
            if has_where {
                query_builder.push(" AND ");
            } else {
                query_builder.push(" WHERE ");
                has_where = true;
            }

            query_builder
                .push("json_extract(object, ")
                .push_bind(format!("${}", key.to_owned()));

            match operator {
                Operator::Equals => query_builder.push(") ="),
                Operator::NotEquals => query_builder.push(") !="),
            };

            query_builder.push_bind(value.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use kube::{core::ObjectMeta, discovery::ApiResource};
    use sqlx::sqlite::SqlitePool;

    #[tokio::test]
    async fn test_new() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let api_resource = ApiResource {
            group: "core".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
            plural: "pods".to_string(),
            api_version: "v1".to_string(),
        };

        let store = Store::new(api_resource, pool.clone()).await.unwrap();
        assert_eq!(store.table(), "v1_pods");

        let table_exists: String =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
                .bind(&store.table())
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(table_exists, store.table());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

        let api_resource = ApiResource {
            group: "core".to_string(),
            version: "v1".to_string(),
            kind: "Pod".to_string(),
            plural: "pods".to_string(),
            api_version: "v1".to_string(),
        };

        let store = Store::new(api_resource, pool.clone()).await.unwrap();
        let pod = DynamicObject {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                labels: Some(BTreeMap::from_iter(vec![(
                    "key".to_string(),
                    "value".to_string(),
                )])),
                ..Default::default()
            },
            types: Default::default(),
            data: Default::default(),
        };

        store.insert_or_replace_object(&pod).await.unwrap();

        let objects = store.list_objects(None, None, None).await.unwrap().items;

        assert_eq!(objects.len(), 1);

        let object = &objects[0];
        assert_eq!(object.metadata.name, Some("test".to_string()));
        assert_eq!(object.metadata.namespace, Some("default".to_string()));

        let objects = store
            .list_objects(Some("default"), None, None)
            .await
            .unwrap()
            .items;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].metadata.namespace, Some("default".to_string()));
        assert_eq!(objects[0].metadata.name, Some("test".to_string()));

        let objects = store
            .list_objects(
                None,
                Some(Selector::from_string("key=value").unwrap()),
                None,
            )
            .await
            .unwrap()
            .items;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].metadata.namespace, Some("default".to_string()));
        assert_eq!(objects[0].metadata.name, Some("test".to_string()));
    }
}
