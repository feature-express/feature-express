LLM_PROMPT = """
{schema}

I use FeatureExpress library (you don't know it because it is new).
The library implements a feature engineering DSL to define the features based on events.
The schema of the ingested events is represented like this

{
  "[event_name]": {
    "[numerical attribute]": "Num",
    "[categorical attribute]": "Str",
  }
}

Where the main key is the event name and the sub-dictionary has the schema for the attributes of the events.

The way you construct the features is like this

[AggregateFunction]([Expression]) OVER [IntervalDefinition] WHERE [CONDITIONS]

Optionally you can add

GROUP BY [Expression] - this creates a nested dictionary of aggregates (think a sparse matrix)

"last(event_time) OVER past"
"first(event_time) OVER future"
"last(event_time) OVER past WHERE event_type = 'PurchaseEvent'"
"first(event_time) OVER future WHERE event_type = 'PurchaseEvent'"
"COUNT(*) OVER past WHERE (game_result = "away" and entities.away = @entities.away) or (game_result = "home" and entities.home = @entities.away) as count_wins_away"
"COUNT(*) OVER past WHERE (game_result = "away" and entities.away = @entities.home) or (game_result = "home" and entities.home = @entities.home) as count_wins_home"
"COUNT(*) OVER past WHERE event_type = 'PurchaseEvent'"
"COUNT(*) OVER future WHERE event_type = 'PurchaseEvent'"
"sum(purchaseAmount) over future where event_type = 'PurchaseEvent' as target"
"sum(1) over last 3 days where event_type = 'PurchaseEvent' "
"last(event_time) over last 3 days where event_type = 'ViewEvent' group by viewedItem"
"last(event_time) over last 3 days where event_type = 'ViewEvent' group by viewedItem"
"sum(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent'"
"avg(purchaseAmount) over last 3 days where event_type = 'PurchaseEvent'"

Please implement features for this schema

{
  "reading": {
    "MaxTemp": "Num",
    "Humidity9am": "Num",
    "WindGustSpeed": "Num",
    "Humidity3pm": "Num",
    "WindDir3pm": "Str",
    "Temp3pm": "Num",
    "Pressure9am": "Num",
    "WindDir9am": "Str",
    "Temp9am": "Num",
    "Cloud9am": "Num",
    "MinTemp": "Num",
    "WindSpeed9am": "Num",
    "WindSpeed3pm": "Num",
    "Rainfall": "Num",
    "WindGustDir": "Str",
    "Pressure3pm": "Num",
    "RainToday": "Str",
    "Cloud3pm": "Num"
  }
}

Please return the list of features as a Python list. Be creative and adapt the features to the domain
"""
