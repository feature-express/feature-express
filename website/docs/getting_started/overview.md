---
sidebar_position: 1
---

# Overview 

## The concept
Feature Express is a cutting-edge tool that is a blend of a feature store and a feature engineering library. It provides a fully declarative framework similar to SQL, ensuring users do not have to concern themselves with the materialization of features, as is often the case with other feature stores. Designed around the concept of events, Feature Express is an integral tool in the workflow of data science and machine learning professionals.

## Event-oriented
At the core of Feature Express is the event data structure. Users define events from their data, which forms the basis of all subsequent operations. An event is defined as follows:

```python
Event(
    entity_ids="Location1",
    event_type="weather",
    time="2020-01-01T16:39:57+00:00",
    attrs={
        "temperature": 10.0,  # in degrees Celsius
        "weather_conditions": ['sunny', 'windy', 'clear'],  # list of conditions
        "precipitation_type": "none",  # can be 'rain', 'snow', 'hail', etc.
        "wind_data": {"speed": 5.0, "direction": "NW"},  # wind data includes speed (in km/h) and direction
    },
)
```
Each event is associated with an entity, has a specific type, and is timestamped. Additional attributes provide flexibility and expressivity, supporting various data types such as numbers, lists, strings, and dictionaries.

## FQL Language
Feature Express introduces its own declarative language, FQL (Feature Query Language), which bears a strong resemblance to SQL. It has a rich suite of aggregation functions that are incredibly valuable for machine learning and data science tasks, enhancing productivity and capabilities of the user.

## Precision Control Over Feature Calculation
Feature Express emphasizes the separation of feature definitions from the definitions of observation dates. This distinction provides users with precise control over when the features are calculated, ensuring accuracy and reducing the chances of data leakage.

## In-Memory Event Store
Feature Express utilizes an in-memory event store which is remarkably quick, allowing for real-time operations. While it might currently be limited to medium-sized problems, it nonetheless offers speed and performance that are advantageous for most use-cases.

## Rust Implementation with Python Interface
Feature Express is implemented in Rust, a language known for its speed and memory safety characteristics. However, it offers an easy-to-use Python interface. Thus, users can leverage the power and speed of Rust while enjoying the simplicity and versatility of Python, making it an accessible tool for a wide array of applications.
