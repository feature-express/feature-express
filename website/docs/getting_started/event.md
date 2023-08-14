---
sidebar_position: 6
---

# Events in Feature Express

In Feature Express, an event is a distinctive record of data associated with a specific point in time. Each event is connected with one or more entities and carries a set of attributes related to that entity or entities.

## Structure of an Event

An event in Feature Express is constructed using the `Event` class. This class contains the following parameters:

- `entites`: Represents the ID or IDs of the entity or entities that the event is related to.
- `event_type`: Denotes the type of the event.
- `time`: The timestamp indicating when the event took place.
- `attrs`: A dictionary comprising attributes relevant to the event.

Here is a simple instance of an event representing weather data:

```python
Event(
    entities={"location": "Warsaw"},
    event_type="weather",
    time="2020-01-01T16:39:57+00:00",
    attrs={
        "temperature": 10.0,  # Numeric value
        "weather_conditions": ['sunny', 'windy', 'clear'],  # List of strings
        "precipitation_type": "none",  # String
        "wind_data": {"speed": 5.0, "direction": "NW"},  # Dictionary with numeric and string values
    },
)
```

## Attribute Data Types

The `attrs` parameter in an event is a dictionary that can hold a variety of different data types. Each type of data corresponds to a specific attribute type:

- **Boolean**: A binary value, true or false.
- **Numeric**: A floating-point number.
- **Integer**: An integer value.
- **String**: A sequence of characters.
- **Dictionary**: A collection of key-value pairs.
- **Dictionary with Numeric Values (MapNum)**: A dictionary where keys are strings and values are numeric.
- **Dictionary with String Values (MapStr)**: A dictionary where both keys and values are strings.
- **List of Booleans (VecBool)**: A list containing boolean values.
- **List of Numeric Values (VecNum)**: A list containing floating-point numbers.
- **List of Integers (VecInt)**: A list containing integer values.
- **List of Strings (VecStr)**: A list containing string values.
- **Date**: A specific date, without a time component.
- **DateTime**: A specific date and time.
- **Nested Dictionary (Nested Map)**: A dictionary within another dictionary, enabling the creation of complex and nested data structures.

Let's create a more comprehensive example of a weather event that uses all the possible attribute types, including a deeply nested attribute:

```python
Event(
    entities={"Location": "Warsaw"},
    event_type="weather",
    time="2020-01-01T16:39:57+00:00",
    attrs={
        "temperature": 10.0,  # Numeric value
        "weather_conditions": ['sunny', 'windy', 'clear'],  # List of strings
        "precipitation_type": "none",  # String
        "wind_data": {  # Dictionary with nested data
            "speed": 5.0,  # Numeric value
            "direction": "NW",  # String
            "gusts": {  # Nested dictionary with numeric values (MapNum)
                "morning": 7.0, 
                "afternoon": 10.0, 
                "evening": 5.0,
            }
        },
        "forecast_data": {  # Nested dictionary with list of numeric values (for future predictions)
            "next_temperatures": [11.0, 12.0, 10.5, 11.0, 10.0],  # List of numeric values
            "next_conditions": ["cloudy", "cloudy", "sunny", "sunny", "rain"],  # List of strings
        },
        "sensor_status": {  # Dictionary with string values
            "sensor1": "active", 
            "sensor2": "inactive"
        },
    },
)
```

This example provides a more sophisticated structure of weather data that uses all possible attribute types, including deeply nested data structures.

# Consistent Attribute Type Schema in Feature Express

In traditional databases and data structures, we typically define a **schema**, which is a structure defining how data is organized. The schema typically contains information about tables, fields, data types, and relationships between tables.

In Feature Express, we introduce a dynamic yet consistent approach to handle schema for events. It allows you to flexibly add new attributes to each event type on the fly, which is akin to dynamically adding new columns to a table in a traditional database. This dynamic flexibility can be exceptionally beneficial when dealing with real-world data, which is often messy and evolves over time.

However, with this flexibility comes the responsibility of maintaining data consistency, especially when it comes to data types of each attribute. Once an attribute is defined for a particular event type, its data type should remain consistent across all instances of that event type. For example, if you've defined an attribute `temperature` as a float for the event type `weather`, all `weather` events should maintain `temperature` as a float.

This attribute consistency is crucial for machine learning and data science work, as inconsistent data types can lead to bugs and confusion in downstream processing, model training, and inference.

In summary, Feature Express introduces a flexible schema that allows you to dynamically add new attributes to each event type, while maintaining consistent data types for existing attributes. This characteristic supports the robust and dynamic nature of real-world data while ensuring data consistency, which is essential for reliable machine learning and data science operations.