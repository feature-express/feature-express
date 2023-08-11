---
sidebar_position: 6
---

# Multiple entities

Sometimes one entity per event is not enough.
Think about games where there are always 2 entities like: NBA games,
chess matches, etc.

In cases like these the Event data must be accompagnied by the entities:

```Python
Event(
    entities={"home": "Los Angeles Lakers", "away": "Goldenstate Warriors"},
    event_type="game",
    time="2020-01-01T16:39:57+00:00",
    attrs={
        "home_win": true,
        "away_win": false,
        "home_pts": 123,
        "away_pts": 115
    }
)
```

The features

```Python
features = [
    Feature("@home"),
    Feature("@away"),
    Feature("@home_win"),
    Feature("@time"),
    # last result where the teams played in exactly the same configuration
    Feature("last(home_win) over past where entities.home = @home and entities.away = @away"),
    # last result where the teams played in reverse configuration
    Feature("last(home_win) over past where entities.home = @away and entities.away = @home"),
    # last result where the teams played each other independently on who was away or home
    Feature("last(home_win) over past where @away in entities and @home in entities"),
]
```

Also, notice that in case of multiple entities the observation date is the same 
as the game date. This means that the context from which we take the combinations
of home and away teams is not random but it represents the actual games.