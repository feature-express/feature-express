use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use rand::prelude::ThreadRng;
use rand::Rng;

use crate::event::{AttributeName, EntityID, EntityType, Event, EventType};
use crate::map::HashMap;
use crate::tests::test_utils::random_datetime_within_last_year;
use crate::types::{Entities, EventID};
use crate::value::Value;

fn generate_player_stats() -> HashMap<AttributeName, Box<Value>> {
    let mut rng = rand::thread_rng();
    let mut stats = HashMap::new();

    stats.insert(
        a!("points".to_string()),
        Box::new(Value::Int(rng.gen_range(0..30))),
    );
    stats.insert(
        a!("minutes".to_string()),
        Box::new(Value::Int(rng.gen_range(20..40))),
    );

    stats
}

fn generate_team_stats() -> HashMap<AttributeName, Box<Value>> {
    let mut rng = rand::thread_rng();
    let mut stats = HashMap::new();

    stats.insert(
        a!("points".to_string()),
        Box::new(Value::Int(rng.gen_range(80..130))),
    );
    stats.insert(
        a!("rebounds".to_string()),
        Box::new(Value::Int(rng.gen_range(30..60))),
    );

    // generate stats for players
    let mut players_stats = HashMap::new();
    for player_id in 1..6 {
        players_stats.insert(
            a!(format!("player_{}", player_id)),
            Box::new(Value::Map(generate_player_stats())),
        );
    }

    stats.insert(
        a!("players".to_string()),
        Box::new(Value::Map(players_stats)),
    );

    stats
}

pub fn generate_nba_game_events(n: usize) -> Vec<Event> {
    let mut rng = rand::thread_rng();
    let mut events = Vec::new();

    for _ in 0..n {
        let mut attrs = HashMap::new();

        let home_stats = generate_team_stats();
        let away_stats = generate_team_stats();

        let home_points = if let Value::Int(points) = **(home_stats.get(&a!("points")).unwrap()) {
            points
        } else {
            0
        };

        let away_points = if let Value::Int(points) = **(away_stats.get(&a!("points")).unwrap()) {
            points
        } else {
            0
        };

        attrs.insert(a!("home_stats".to_string()), Value::Map(home_stats));
        attrs.insert(a!("away_stats".to_string()), Value::Map(away_stats));
        attrs.insert(
            a!("game_result".to_string()),
            Value::Str(if home_points > away_points {
                "home".to_string()
            } else {
                "away".to_string()
            }),
        );

        let entities = make_nba_teams(&mut rng);

        let event = Event {
            event_type: EventType("game".to_string()),
            event_time: random_datetime_within_last_year(),
            entities,
            event_id: Some(EventID::from(format!("event_{}", rng.gen::<u32>()))),
            experiment_id: None,
            attrs: Some(attrs),
        };

        events.push(event);
    }

    events
}

fn make_nba_teams(rng: &mut ThreadRng) -> BTreeMap<EntityType, EntityID> {
    let nba_teams = vec![
        "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW", "HOU", "IND", "LAC",
        "LAL", "MEM", "MIA", "MIL", "MIN", "NOP", "NYK", "OKC", "ORL", "PHI", "PHX", "POR", "SAC",
        "SAS", "TOR", "UTA", "WAS",
    ];

    let home_team_index = rng.gen_range(0..nba_teams.len());
    let mut away_team_index = rng.gen_range(0..nba_teams.len());

    // Ensure that home_team and away_team are different
    while home_team_index == away_team_index {
        away_team_index = rng.gen_range(0..nba_teams.len());
    }

    let mut entities = Entities::new();
    entities.insert(
        EntityType("home".to_string()),
        EntityID(nba_teams[home_team_index].to_string()),
    );
    entities.insert(
        EntityType("away".to_string()),
        EntityID(nba_teams[away_team_index].to_string()),
    );
    entities
}
