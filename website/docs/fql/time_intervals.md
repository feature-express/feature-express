---
sidebar_position: 6
---

# Intervals in FQL

FQL, implemented in Feature Express, allows defining intervals over which to calculate aggregations and extract features.

## Interval Types

The main types of intervals that can be defined in FQL include Direction Only, Fixed, and Keyword intervals.

### 1. **Direction Only**

Direction Only intervals extract features over a relative period before or after the current row. They include:

- **`past`** - from the beginning of time until the current row
- **`future`** - from the current row until the end of time

Example:

```fql
avg(price) over past // average price from beginning until current row
```

### 2. **Fixed Intervals**

Fixed intervals are defined with a direction, number of units, and unit:

- **`last`** or **`next`** - look back or forward from the current row
- **`N`** - number of units
- **`units`** - `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`

Examples:

```fql
avg(price) over last 5 days // look back 5 days
avg(price) over next 2 weeks // look forward 2 weeks
```

These can also be expressed with variations in terminology, such as `"previous"` or `"future"`, and units may optionally be followed by an 's', like `"last 5 days"`.

### 3. **Keyword Intervals**

Keyword intervals represent common predefined periods and relative periods:

- `yesterday`, `lastweek`, `lastmonth`, `lastyear`, `ytd`, `wtd`, `mtd`
- `tomorrow`, `nextweek`, `nextmonth`, `nextyear`

Example:

```fql
avg(sales) over lastweek // last week from current row
```

Below is a table providing examples of how keyword intervals translate into specific dates for a given date (2023-05-17):

| Keyword Interval | Description                    | Start Date | End Date   |
|------------------|--------------------------------|------------|------------|
| YTD              | Year-to-Date                   | 2023-01-01 | 2023-05-17 |
| MTD              | Month-to-Date                  | 2023-05-01 | 2023-05-17 |
| WTD              | Week-to-Date                   | 2023-05-15 | 2023-05-17 |
| Yesterday        | Previous day                   | 2023-05-16 | 2023-05-16 |
| LastWeek         | Previous calendar week         | 2023-05-08 | 2023-05-14 |
| LastMonth        | Previous calendar month        | 2023-04-01 | 2023-04-30 |
| LastQuarter      | Previous calendar quarter      | 2023-01-01 | 2023-03-31 |
| LastYear         | Previous calendar year         | 2022-01-01 | 2022-12-31 |
| SameDayLastWeek  | Same day of the previous week  | 2023-05-10 | 2023-05-10 |
| SameDayLastMonth | Same day of the previous month | 2023-04-17 | 2023-04-17 |
| SameDayLastYear  | Same day of the previous year  | 2022-05-17 | 2022-05-17 |
| Tomorrow         | Next day                       | 2023-05-18 | 2023-05-18 |
| NextWeek         | Next calendar week             | 2023-05-22 | 2023-05-28 |
| NextMonth        | Next calendar month            | 2023-06-01 | 2023-06-30 |
| NextQuarter      | Next calendar quarter          | 2023-07-01 | 2023-09-30 |
| NextYear         | Next calendar year             | 2024-01-01 | 2024-12-31 |
| SameDayNextWeek  | Same day of the next week      | 2023-05-24 | 2023-05-24 |
| SameDayNextMonth | Same day of the next month     | 2023-06-17 | 2023-06-17 |
| SameDayNextYear  | Same day of the next year      | 2024-05-17 | 2024-05-17 |
| NextWorkDay      | Next business day              | 2023-05-18 | 2023-05-18 |
| PreviousWorkDay  | Previous business day          | 2023-05-16 | 2023-05-16 |

## Using Intervals

Intervals are used in FQL inside aggregation and window functions:

```fql
avg(price) over last 5 days // average price over last 5 days
min(price) over next 2 weeks // min price over next 2 weeks
sum(revenue) over lastyear // total revenue over previous year
```

Where clauses can filter data over the interval:

```fql
avg(sales) over lastweek where category = 'electronics' // for electronics
```

Intervals can be combined with grouping:

```fql
avg(price) over lastmonth by category // averages grouped by category
```

## Future Plans

Additional interval types like session and event-based intervals will be added to FQL in the future.

## Syntax and Conventions

Defining time intervals in Feature Express is done using string definitions of the intervals based on PEST grammar. Please make sure to follow the specific syntax and conventions when defining time intervals in Feature Express.

## Conclusion

Intervals in FQL enable powerful time-based queries and aggregations. By understanding and using these interval types, you can unlock advanced analytics capabilities within Feature Express.
