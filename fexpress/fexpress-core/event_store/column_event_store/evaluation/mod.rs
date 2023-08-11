use crate::ast::core::AggrExpr;
use crate::event_store::column_event_store::ColumnStore;

pub struct ColumnStoreEvalContext<'a> {
    pub store: &'a ColumnStore,
}

pub fn eval_agg(agg: AggrExpr, context: &ColumnStoreEvalContext) {}

#[cfg(test)]
mod tests {
    use crate::ast::analyze::sort_sub_expressions;
    use crate::ast::core::{AggrExpr, AggregateFunction, Expr};
    use crate::event::AttributeKey;
    use crate::event_store::column_event_store::evaluation::ColumnStoreEvalContext;
    use crate::event_store::column_event_store::{ColumnStore, Settings};
    use crate::interval::{Direction, FixedInterval, NewInterval, Unit};
    use crate::parser::expr_parser::{generate_ast, ExprParser, Rule};
    use crate::tests::fake_nba::generate_nba_game_events;
    use pest::Parser;

    #[test]
    fn test_eval_agg() {
        let settings = Settings {
            block_size: 100,
            enable_compression: true,
        };
        let mut store = ColumnStore::new(settings);
        let mut context = ColumnStoreEvalContext { store: &store };
        let mut games = generate_nba_game_events(1000);
        games.sort_by_key(|event| event.event_time);
        for game in games {
            store
                .insert_new_event_incremental_incremental(&game)
                .unwrap();
        }

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
        FOR
            @entities := home
        "#.trim().replace(" past ", " last 30 day ").to_string();

        let successful_parse = ExprParser::parse(Rule::full_query, &query);
        let ast = match successful_parse {
            Ok(parsed) => generate_ast(parsed),
            Err(e) => panic!(e.to_string()),
        };

        let subexpressions = sort_sub_expressions(ast).unwrap();
        for expr in subexpressions {
            println!("\n\n{:?}", expr.0);
            for e in expr.1 {
                println!("- {:?}", e);
            }
        }
    }
}
