#[cfg(test)]
mod tests {
    use crate::dataframe::DataFrame;
    use crate::event::{AttributeName, EntityID, EntityType, Event, EventType};
    use crate::event_index::{EventContext, EventScopeConfig, QueryConfig, RawQuery};
    use crate::event_store::EventStore;
    use crate::map::HashMap;
    use crate::obs_dates::ObservationDatesConfig;
    use crate::tests::fake_nba::generate_nba_game_events;
    use crate::tests::test_utils::random_datetime_within_last_year;
    use crate::types::{Entities, EventID};
    use crate::value::Value;
    use chrono::NaiveDateTime;
    use prettytable::Attr;
    use rand::prelude::ThreadRng;
    use rand::Rng;
    use regex::Regex;
    use std::collections::BTreeMap;

    #[test]
    fn test_nba_games_features() {
        let event_store_settings = QueryConfig {
            parallel: true,
            include_events_on_obs_date: true,
        };
        let entity_query = EventScopeConfig::AllEvents;
        let mut event_context = EventContext::new_memory();
        let events = generate_nba_game_events(1000);
        for event in &events {
            event_context.new_event(event.clone());
        }

        println!("{:#?}", event_context.event_store.get_schema());

        // TODO: it turns out that there must be 2 types of stored variables :)
        //       one that is attached to the event if it doesn't depend on the context
        //       second is an aggregate that is attached to the
        //          (observation_date, evaluated entities)
        //
        let obs_dates = ObservationDatesConfig::AllEvents;
        let query =r#"
        SELECT
            obs_dt as obs_dt,
            @entities.home as home,
            @entities.away as away,
            @winning_team := if(game_result = "away", entities.away, entities.home),
            @winning_team as winning_team,
            COUNT(*) OVER past WHERE (game_result = "away" and entities.away = @entities.away) or (game_result = "home" and entities.home = @entities.away) as count_wins_away,
            COUNT(*) OVER past WHERE (game_result = "away" and entities.away = @entities.home) or (game_result = "home" and entities.home = @entities.home) as count_wins_home,
            LAST(game_result) over past as last_game_result,
            AVG(game_result = "away") OVER past as win_perc_away,
            AVG(game_result = "home") OVER past as win_perc_home,
            LAST(entities.away) OVER past as last_away,
            LAST(entities.away) OVER past WHERE entities.home = @entities.home as last_away,
            AVG(game_result = "away") OVER past WHERE entities.away = @entities.home as home_win_perc_away,
            AVG(game_result = "away") OVER past WHERE entities.away = @entities.away as home_win_perc_away,
            AVG(game_result = "home") OVER past WHERE entities.home = @entities.home as home_win_perc_away,
            AVG(game_result = "home") OVER past WHERE entities.home = @entities.away as home_win_perc_away,
            AVG(home_stats.points) over past as avg_home_points,
            AVG(away_stats.points) over past as avg_away_points,
            AVG(away_stats.points) over YTD as avg_away_points_ytd,
        FOR
            @entities := home
        "#.trim().replace(" past ", " last 30 day ").to_string();

        let comment_regex = Regex::new(r"//.*").unwrap();
        let query = comment_regex.replace_all(&query, "");
        let query_config = QueryConfig {
            include_events_on_obs_date: true,
            parallel: false,
        };

        let features = event_context
            .extract_records_from_expr(
                obs_dates.clone(),
                entity_query.clone(),
                RawQuery::SelectExpr(query.into()),
                &query_config,
                None,
                None,
            )
            .unwrap();

        let dataframe = DataFrame::new(features.0, features.1);
        dataframe.head(10).display();
    }
}
