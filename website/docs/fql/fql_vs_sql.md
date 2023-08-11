---
sidebar_position: 6
---

# FQL vs SQL

## Comparison, Similarities and Differences

Feature Query Language (FQL) and Structured Query Language (SQL) both serve as powerful languages for querying databases. The primary purpose of these languages is to fetch, manipulate, and analyze data, yet they have a few key differences and similarities. The comparison becomes even more interesting when considering FQL's primary usage, which is to produce data for machine learning purposes, a function not typically attributed to SQL.

### Similarities

1. **Aggregation**: Both FQL and SQL support the concept of aggregation, which involves operations such as count, sum, min, max, and average that provide summary statistics of data.

2. **From**: Both FQL and SQL support selecting tables. In FQL it is optional. There are rare cases where you might want to use all event types for some calculation.

3. **Where clause**: Both languages support a "where" clause, which allows the user to specify conditions that the data must meet.

4. **Group by clause**: Both SQL and FQL utilize "group by" clauses to categorize data and conduct operations within these categories.

5. **Having clause**: Similar to SQL, FQL supports the "having" clause that allows filtering results of aggregate functions.

### Differences

1. **Time-oriented structure**: Unlike SQL, FQL provides a unique "over" clause, which specifies an interval relative to the observation date. This feature makes FQL more suited for time-series data and temporal operations.

2. **Mandatory aggregation**: Unlike SQL, where the aggregation function is optional, FQL requires an obligatory aggregation function. This mandatory requirement is key to FQL's purpose of generating features for machine learning models.

3. **Data Structure**: FQL's "group by" clause creates a map data structure, whereas SQL's "group by" merely groups the result set.

4. **Having min/max**: FQL provides additional functionality to limit conditions to the extreme values of an expression using the optional clauses `having min` or `having max`.
