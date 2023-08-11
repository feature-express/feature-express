/// these tests are designed in such a way that they test consistency between the storages
/// in theory each operation on an event store should give you the same results
/// nevermind what implementation we are using
#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::map::HashSet;
    use crate::sstring::SmallString;
    use rstest::*;
    use rstest_reuse::{self, *};
    use serial_test::serial;

    use crate::datetime_utils::parse_utc_from_str;
    use crate::event::{Entity, EntityType, Event, EventType};
    use crate::event_index::{EventContext, EventQueryConfig, Query};
    use crate::event_store::postgres::postgres_event_store::PostgresEventStoreConfig;
    use crate::event_store::EventStoreSettings;
    use crate::obs_dates::{Fixed, ObservationDatesConfig};
    use crate::value::Value;

    fn get_postgres_config() -> PostgresEventStoreConfig {
        PostgresEventStoreConfig {
            host: "localhost".into(),
            port: "5432".into(),
            username: "postgres".into(),
            password: "postgres".into(),
            db_name: "python".into(),
        }
    }

    #[fixture]
    fn postgres_context() -> Arc<RwLock<EventContext>> {
        unimplemented!()
        // let config = get_postgres_config();
        // let store = PostgresEventStore::new(config.clone());
        // store.drop_tables();
        // store.create_tables();
        // let postgres_context = EventContext::new_postgres(EventStoreSettings::default(), config);
        // Arc::new(RwLock::new(postgres_context))
    }

    #[fixture]
    fn memory_context() -> Arc<RwLock<EventContext>> {
        Arc::new(RwLock::new(EventContext::default()))
    }

    #[template]
    #[rstest]
    // #[case(postgres_context())]
    #[case(memory_context())]
    #[serial]
    fn all_event_stores(#[case] store: Arc<RwLock<EventContext>>) {}

    #[apply(all_event_stores)]
    pub fn test_query_event_type_entity(#[case] store: Arc<RwLock<EventContext>>) {
        let mut store = store.write().unwrap();

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-01T00:00:00+00:00"),
            entities: btreemap!["a".into() => "a".into()],
            event_id: Some("1".into()),
            experiment_id: None,
            attrs: Some(hashmap!["pressure".into() => Value::Num(100.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test2".into()),
            event_time: parse_utc_from_str("2020-01-02T00:00:00+00:00"),
            entities: btreemap!["a".into() => "a".into()],
            event_id: Some("2".into()),
            experiment_id: None,
            attrs: Some(hashmap!["pressure".into() => Value::Num(200.0)]),
        };
        store.new_event(event);

        let event = Event {
            event_type: EventType("test".into()),
            event_time: parse_utc_from_str("2020-01-03T00:00:00+00:00"),
            entities: btreemap!["a".into() => "a".into()],
            event_id: Some("3".into()),
            experiment_id: Some(from_string!("experiment_1".to_string())),
            attrs: Some(hashmap!["pressure".into() => Value::Num(300.0)]),
        };
        store.new_event(event);

        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType(SmallString::from("a")));
        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-04T00:00:00".into()],
        ));
        let features_def =
            vec!["AVG(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let entity_query = EventQueryConfig::RelatedEntitiesEvents(vec![EntityType("".into())]);
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query.clone(),
                Query::VecExpr(features_def),
                None,
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(100.0)]);

        let mut entity_types = HashSet::new();
        entity_types.insert(EntityType(SmallString::from("a")));
        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-04T00:00:00".into()],
        ));
        let features_def =
            vec!["AVG(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query.clone(),
                Query::VecExpr(features_def),
                Some("experiment_1".into()),
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(200.0)]);

        let obs_dates = ObservationDatesConfig::Fixed(Fixed::new_from_str_vec(
            entity_types.clone(),
            vec!["2020-01-04T00:00:00".into()],
        ));
        let features_def =
            vec!["AVG(pressure) OVER past WHERE event_type = 'test' as f".to_string()];
        let features = store
            .extract_features_from_expr(
                &obs_dates,
                entity_query.clone(),
                Query::VecExpr(features_def),
                Some("experiment_2".into()),
                None,
            )
            .unwrap();
        assert_eq!(features.get("f").unwrap().clone(), vec![Value::Num(100.0)]);
    }
}
