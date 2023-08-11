---
sidebar_position: 6
---

# Observation Dates

In Feature Express, observation dates, often represented as DATETIME, play a crucial role. The observation dates are the specific dates relative to which each feature is calculated. This notion allows users to declare features in a time-relative context, providing valuable insights for time-series data analysis and modeling.

### Types of Observation Dates

Feature Express currently supports three types of observation dates:

1. **NBetween**: This type of observation date generates a list of dates between a start and end date. The dates are generated at a regular interval specified by the user. The interval (or `nth`) can be in terms of milliseconds, seconds, minutes, hours, days, or weeks.

2. **ConstantDates**: This type allows users to define a constant set of observation dates. The dates are specified as a vector of timestamps and apply uniformly across all entities.

3. **EntityObsDatesMap**: This type allows users to provide a map of entity identifiers to a vector of timestamps. This way, each entity can have its specific set of observation dates. This can be particularly useful when different entities have different relevant observation periods.

Here is a brief description of each type and how they are implemented in the code:

#### 1. NBetween

In `NBetween`, you specify a `date_part` and an `nth` value. The `date_part` represents the granularity of the intervals (e.g., Day, Hour), and the `nth` value indicates the length of each interval. The function `generate_from_start_end` generates the dates at every nth interval from the start date until the end date.

Example:
```rust
let nbetween = NBetween {
    date_part: DatePart::Day,
    nth: 10,
};
let start_dt = Utc.ymd(2021, 1, 1).and_hms(0, 0, 0).naive_utc();
let end_dt = Utc.ymd(2021, 2, 1).and_hms(0, 0, 0).naive_utc();
let datetimes = nbetween.generate_from_start_end(start_dt, end_dt);
assert!(datetimes.len() == 5);
```
In the example above, `n_between` generates a list of dates every 10 days starting from January 1, 2021, until February 1, 2021.

#### 2. ConstantDates

The `ConstantDates` struct contains a vector of dates that are used as observation dates for all entities. The `new_from_str_vec` function is used to convert a vector of strings into a `ConstantDates` object.

Example:
```rust
let constant_dates = ConstantDates::new_from_str_vec(vec!["2021-01-01T00:00:00", "2021-02-01T00:00:00", "2021-03-01T00:00:00"]);
```
In this example, `constant_dates` will have three observation dates: January 1, 2021, February 1, 2021, and March 1, 2021.

#### 3. EntityObsDatesMap

`EntityObsDatesMap` contains a map where each key-value pair represents an entity and its associated observation dates. The `new_from_map` function is used to convert a hashmap of entity IDs and their respective observation dates (in string format) into an `EntityObsDatesMap` object.

Example:
```rust
let mut entity_dates_map = HashMap::new();
entity_dates_map.insert("entity1", vec!["2021-01-01T00:00:00", "2021-02-01T00:00:00"]);
entity

_dates_map.insert("entity2", vec!["2021-02-01T00:00:00", "2021-03-01T00:00:00"]);
let entity_obs_dates_map = EntityObsDatesMap::new_from_map(entity_dates_map);
```
In this example, `entity_obs_dates_map` assigns the dates January 1, 2021, and February 1, 2021, to "entity1" and the dates February 1, 2021, and March 1, 2021, to "entity2".

In summary, these three types of observation dates give you the flexibility to generate features based on your needs. You can create observation dates for different entities and different time intervals, allowing for precise control over the temporal aspects of feature extraction.
