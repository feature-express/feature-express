---
sidebar_position: 6
---

# Time intervals

Defining time intervals in Feature Express is done using string definitions of the intervals based on PEST grammar.

1. **Fixed Intervals**: These are defined with a direction, a number, and a unit. The direction can be "next", "last", "previous", "future" or "past". The number specifies the quantity, and the unit can be "millisecond", "second", "minute", "hour", "day", or "week". Units may optionally be followed by an 's'. For example, "last 5 days" would be represented as `"last 5 days"`.

2. **Direction Only**: These intervals only contain a direction ("future" or "past") and are used to specify all future or all past time. For example, to represent all past time, you would simply write `"past"`.

3. **Keyword Intervals**: These are represented using specific keywords "ytd" (year to date), "mtd" (month to date), and "wtd" (week to date). For example, to represent the time period from the start of the current year until now, you would write `"ytd"`.

A time interval can be a fixed interval, direction only, or a keyword interval. Here are some examples:

- `"last 1 week"` would represent the past week.
- `"past"` would represent all past time.
- `"ytd"` would represent the current year to date.

Please make sure to follow the specific syntax and conventions when defining time intervals in Feature Express.

Apologies for the confusion. Here's the updated documentation with a table explaining each keyword interval and providing an example:

## Time Intervals

Defining time intervals in Feature Express is done using string definitions of the intervals based on PEST grammar.

For example for the date 2023-05-17 the intervals would be. The Start Date starts at 00:00:00 and End Date ends at 23:59:59.

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
