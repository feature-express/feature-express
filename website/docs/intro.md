---
sidebar_position: 1
---

# Intro

## The Complexities of Time in Data Science

When dealing with the fabric of reality in the context of data, time is an essential dimension. However, this dimension introduces complexity that is challenging to navigate.

- **Complexity of Time Modeling**: When time is involved, everything changes and becomes more intricate. Validating models is hard, and data leaks are subtle and often unnoticed.
- **Difficulty in Feature Engineering**: Traditional SQL-like operations are challenging and lengthy to write, with a high propensity for mistakes. Feature engineering around time is not a trivial task.
- **Necessity for Event Data**: To accurately reflect reality and make robust features, timestamped event data is essential. Why not use a dedicated data structure to represent this more clearly?

## Why FeatureExpress?

FeatureExpress is born out of the necessity for precise time and event modeling in the context of customer data, recommendations, and other event-driven analytics.

### A Hybrid Approach: Feature Engineering Library & Feature Store

FeatureExpress stands at the intersection of a feature engineering library and a feature store, providing a robust framework to define and process event-based data.

### Why Another Feature Engineering Library?

Current feature stores present significant challenges:

- **Explicit Materialization and Caching of Features**: This approach moves the burden of materializing features to data scientists.
- **Declarative Over Imperative**: Existing stores require answering how and when the features are materialized. Instead, a declarative approach (like SQL) focuses on 'what.' FeatureExpress is declarative, employing a SQL-like DSL to offer an intuitive way to declare features through time.

### In-Memory Processing

Written in Rust, FeatureExpress provides:

- **Fast Materialization of Features**: Enjoy the rapid processing speed of an in-memory system.
- **Parallelism**: Utilize the concurrent processing capabilities offered by Rust.
- **Future Expansion**: More backends will be added in the future for more permanent storage options.

### Dealing with Time: Past, Present, and Future

FeatureExpress's expressive DSL allows clear separation between past and future, thus almost eliminating the risk of data leaks.

```SQL
last(event_time) OVER past
first(event_time) OVER future
COUNT(*) OVER past WHERE event_type = 'PurchaseEvent'
```

### Performance-Oriented Architecture

Utilize the in-memory event store, indices, and incremental aggregates for performance optimization.

## Conclusion

FeatureExpress is a novel tool that tackles the complexities of time and event data, offering a more streamlined and intuitive approach to feature engineering. Whether you're modeling customer behavior, generating recommendations, or dealing with any time-stamped data, FeatureExpress is designed to make your life easier.

Give it a try and unlock the power of event-driven feature engineering. It's time to express your features with FeatureExpress!