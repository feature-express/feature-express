---
sidebar_position: 6
---

# FQL (Feature Query Language)

The Feature Query Language (FQL) is a query language used in Feature Express for defining feature engineering operations. FQL allows users to express complex feature transformations and aggregations on their data. This section provides an overview of FQL and its syntax.

### Syntax

FQL expressions are written using a combination of keywords, operators, literals, and function calls. Here are the key components of the FQL syntax:

#### Keywords

- `in`: A keyword used for specifying inclusion in a set.
- `not in`: A keyword used for specifying exclusion from a set.
- `and`: A logical operator for performing logical conjunction.
- `or`: A logical operator for performing logical disjunction.
- `over`: A keyword used to specify the window over which an aggregation is performed.
- `where`: A keyword used to define filtering conditions.
- `group by`: A keyword used to specify grouping of aggregated results.
- `having`: A keyword used to define filtering conditions on aggregated results.
- `as`: A keyword used to specify an alias for a feature.

#### Operators

- Comparison operators: `==`, `!=`, `<`, `>`, `<=`, `>=` used for comparing values.
- Mathematical operators: `+`, `-`, `*`, `/`, `^` for performing arithmetic operations.

#### Literals

- Numeric literals: Integer and floating-point numbers.
- String literals: Enclosed in single quotes (`'`) or double quotes (`"`).
- Boolean literals: `true` and `false`.
- Null literal: `null`.

#### Functions

- Aggregation functions: Functions like `count`, `sum`, `min`, `max`, `median`, etc., used for aggregating values.
- Time-based functions: Functions like `time_of_first`, `avg_time_between`, etc., used for time-related calculations.

### Example FQL Expressions

Here are some examples of FQL expressions:

```sql
count(type) over past
count(type) over past where type in ('a', 'b', 'c', 'd')
count(type) over past where type not in ('a', 'b', 'c', 'd')
count(type) over past where tempint in (1, 2, 3, 4)
count(type) over past where tempint not in (1, 2, 3, 4)
count(type) over past where temp in (1.0, 2.0, 3.0, 4.0)
count(type) over past where temp not in (1.0, 2.0, 3.0, 4.0)
last(dict.m) over past
count(type) over past group by dict.m
last(dict) over past
nth(temp, 0) over past
nth(temp, 1) over past
nth(temp, -1) over past
nth(temp, -2) over past
nth(temp, -7) over past
sum(temp) over past
min(temp) over past
max(temp) over past
max(type) over past
last(type) over past
min(event_time) over past
max(event_time) over past
median(temp) over past
first(temp) over past
last(temp) over past
first(type) over past having min temp
first(type) over past having max temp
first(event_time) over past having max pressure
first(event_time) over past having max temp
```