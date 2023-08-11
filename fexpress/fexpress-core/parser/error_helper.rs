use pest::error::{Error, ErrorVariant, InputLocation};
use pest::iterators::Pair;
use strsim::normalized_levenshtein;

use crate::parser::expr_parser::Rule;

fn describe_rule(pair: Pair<Rule>) -> String {
    match pair.as_rule() {
        Rule::symbol => "A symbol can be any letter, number, underscore (_), or space.".to_string(),
        Rule::attr_name => {
            "An attribute name can be any letter, number, underscore (_), or period (.)."
                .to_string()
        }
        Rule::integer => "An integer is a sequence of digits.".to_string(),
        Rule::symbol_untyped_attr => "A symbol_untyped_attr is an attribute name.".to_string(),
        Rule::alias_symbol => "An alias_symbol is a symbol.".to_string(),
        Rule::obs_dt => "obs_dt is a keyword which represents observation date.".to_string(),
        // Rule::entity_id => "entity_id is a keyword which represents entity id.".to_string(),
        Rule::event_type => "event_type is a keyword which represents event type.".to_string(),
        Rule::event_time => "event_time is a keyword which represents event time.".to_string(),
        Rule::attr => "An attr is a symbol_untyped_attr.".to_string(),
        Rule::literal => "A literal can be a float, integer, string, or boolean.".to_string(),
        Rule::comparison_op => {
            "A comparison operator can be '>=', '<=', '<', '>', '==', '=', '!=', or '<>'."
                .to_string()
        }
        Rule::math_op => "A math operator can be '+', '-', '*', '/', or '^'.".to_string(),
        Rule::logical_op => "A logical operator can be 'and' or 'or'.".to_string(),
        Rule::binary_expr => {
            "A binary expression is a term followed by one or more binary operations and terms."
                .to_string()
        }
        Rule::aggfunc_name => "An aggfunc_name is a symbol.".to_string(),
        Rule::interval => {
            "An interval can be a fixed_interval, direction_only, or keyword_interval.".to_string()
        }
        Rule::aggfunc0 => {
            "An aggfunc0 is a type of aggregate function with one argument and optional clauses."
                .to_string()
        }
        Rule::aggfunc1 => {
            "An aggfunc1 is a type of aggregate function with two arguments and optional clauses."
                .to_string()
        }
        Rule::expr => {
            "An expr can be a binary_expr, literal, aggregate function, or one of several keywords."
                .to_string()
        }
        Rule::alias => "An alias is the keyword 'as' followed by an alias_symbol.".to_string(),
        _ => "This is a rule not described.".to_string(),
    }
}

fn rule_to_strings(pair: &Rule) -> Vec<String> {
    match pair {
        Rule::over_keyword => vec!["over".to_string()],
        Rule::keyword_interval => vec!["mtd".to_string(), "ytd".to_string(), "wtd".to_string()],
        Rule::unit => vec![
            "millisecond".to_string(),
            "milliseconds".to_string(),
            "second".to_string(),
            "seconds".to_string(),
            "minute".to_string(),
            "minutes".to_string(),
            "hour".to_string(),
            "hours".to_string(),
            "day".to_string(),
            "days".to_string(),
            "week".to_string(),
            "weeks".to_string(),
        ],
        Rule::direction => vec![
            "next".to_string(),
            "last".to_string(),
            "previous".to_string(),
            "future".to_string(),
            "past".to_string(),
        ],
        Rule::direction_only => vec!["future".to_string(), "past".to_string()],
        Rule::logical_op => vec!["and".to_string(), "or".to_string()],
        Rule::comparison_op => vec![
            "==".to_string(),
            "=".to_string(),
            ">".to_string(),
            ">=".to_string(),
            "<".to_string(),
            "<=".to_string(),
            "!=".to_string(),
            "<>".to_string(),
        ],
        Rule::math_op => vec![
            "+".to_string(),
            "-".to_string(),
            "*".to_string(),
            "/".to_string(),
            "^".to_string(),
        ],
        Rule::having_keyword => vec!["having".to_string()],
        Rule::group_by_keyword => vec!["group by".to_string()],
        Rule::where_keyword => vec!["where".to_string()],
        _ => vec![],
    }
}

pub fn friendly_pest_error(error: &Error<Rule>) -> String {
    let mut message = String::new();
    message += &format!(
        "There was an error parsing your expression: {}\n",
        error.line()
    );
    match &error.variant {
        ErrorVariant::ParsingError {
            positives,
            negatives: _,
        } => {
            // fuzzy matching: suggest a "did you mean?" correction
            let mut best_distance = 0.0; // normalized Levenshtein distance is always between 0 and 1
            let mut best_match = "".to_string();
            for rule in positives {
                let possible_values = rule_to_strings(rule); // previously defined function
                let line = error.line().to_string();
                let error_location = match error.location {
                    InputLocation::Pos(start) => start,
                    InputLocation::Span((start, _)) => start,
                };
                if !possible_values.is_empty() {
                    for value in &possible_values {
                        if error_location + value.len() <= line.len() {
                            let error_text_slice =
                                &line[error_location..(error_location + value.len())];
                            let distance = normalized_levenshtein(error_text_slice, value);
                            if distance > best_distance {
                                best_distance = distance;
                                best_match = value.clone();
                            }
                        }
                    }
                }
            }

            if best_distance > 0.5 {
                // threshold for suggestion: tune this as necessary
                message += &format!("Did you mean `{}`?\n", best_match);
            }

            if message.is_empty() {
                message = "Unexpected error during parsing. Please check your input.".to_string();
            }

            message
        }
        ErrorVariant::CustomError { message } => {
            format!("Error: {}", message)
        }
    }
}
