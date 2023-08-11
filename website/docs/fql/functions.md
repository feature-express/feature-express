---
sidebar_position: 6
---

# Functions

## Aggregate functions

| Function name        | Description                                           |
|----------------------|-------------------------------------------------------|
| `count`              | count                                                 |
| `sum`                | sum                                                   |
| `min`                | minimum                                               |
| `max`                | maximum                                               |
| `avg`                | average                                               |
| `median`             | median                                                |
| `var`                | variance                                              |
| `stdev`              | standard deviation                                    |
| `first`              | first value (over the events are sorted by time)      |
| `last`               | last value (over the events are sorted by time)       |
| `values`             | extracts raw vector of values to be aggregated        |
| `nth(expression, n)` | returns nth value counting from 0                     |
| `time_of_last`       | time where the expression was true for the last time  |
| `time_of_first`      | time where the expression was true for the first time |
| `avg_days_between`   | average days between expression was true              |

## Math

| Function name | Description                                                                                                                                 |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `floor`       | Returns the largest integer less than or equal to a number.                                                                                 |
| `ceil`        | Returns the smallest integer greater than or equal to a number.                                                                             |
| `round`       | Returns the nearest integer to a number. Round half-way cases away from `0.0`.                                                              |
| `trunc`       | Returns the integer part of a number.                                                                                                       |
| `fract`       | Returns the fractional part of a number.                                                                                                    |
| `abs`         | Computes the absolute value. Returns NAN if the number is NAN.                                                                              |
| `signum`      | Returns a number that represents the sign of self. 1.0 if the number is positive, +0.0 or INFINITY. -1.0 if the number is negative, -0.0 or | NEG_INFINITY. |  if the number is NAN. |
| `sqrt`        | Returns the square root of a number. Returns NaN if it is a negative number other than `-0.0`.                                              |
| `exp`         | Returns e^x, (the exponential function).                                                                                                    |
| `exp2`        | Returns 2^x.                                                                                                                                |
| `ln`          | Returns the natural logarithm of the number.                                                                                                |
| `log2`        | Returns the base 2 logarithm of the number.                                                                                                 |
| `log10`       | Returns the base 10 logarithm of the number.                                                                                                |
| `sin`         | Computes the sine of a number (in radians).                                                                                                 |
| `cos`         | Computes the cosine of a number (in radians).                                                                                               |
| `tan`         | Computes the tangent of a number (in radians).                                                                                              |
| `asin`        | Computes the arcsine of a number. Return value is in radians in the range [-pi/2, pi/2] or NaN if the number is outside the range [-1, 1].  |
| `acos`        | Computes the arccosine of a number. Return value is in radians in the range [0, pi] or NaN if the number is outside the range [-1, 1].      |
| `atan`        | Computes the arctangent of a number. Return value is in radians in the range [-pi/2, pi/2];                                                 |
| `expm1`       | Returns e^x - 1 in a way that is accurate even if the number is close to zero.                                                              |
| `ln1p`        | Returns ln(1+n) (natural logarithm) more accurately than if the operations were performed separately.                                       |
| `sinh`        | Hyperbolic sine function.                                                                                                                   |
| `cosh`        | Hyperbolic cosine function.                                                                                                                 |
| `asinh`       | Inverse hyperbolic sine function.                                                                                                           |
| `acosh`       | Inverse hyperbolic cosine function.                                                                                                         |
| `atanh`       | Inverse hyperbolic tangent function.                                                                                                        |
| `divEuclid`   | Division rounded toward negative infinity.                                                                                                  |
| `remEuclid`   | The remainder of a division, rounded towards negative infinity.                                                                             |
| `powf`        | Returns self raised to the power of other.                                                                                                  |
| `clamp`       | Returns the maximum of the lower limit and the minimum of the upper limit and the value.                                                    |

## Text

| Function name | Description                                                                                                                |
|---------------|----------------------------------------------------------------------------------------------------------------------------|
| `len`         | Returns the length of the string.                                                                                          |
| `substr`      | Extracts characters from a string, beginning at a specified start position, and through the specified number of character. |
| `concat`      | Concatenates two strings.                                                                                                  |
| `trim`        | Removes leading and trailing whitespace from a string.                                                                     |
| `lower`       | Converts all uppercase letters in a string to lowercase.                                                                   |
| `upper`       | Converts all lowercase letters in a string to uppercase.                                                                   |
| `replace`     | Replaces all occurrences of a substring within a string, with a new substring.                                             |
| `startsWith`  | Checks whether a string starts with specified string.                                                                      |
| `endsWith`    | Checks whether a string ends with specified string.                                                                        |
| `contains`    | Checks whether a string contains the specified string/characters.                                                          |

## Regex

| Function name  | Description                                                        |
|----------------|--------------------------------------------------------------------|
| `regexMatch`   | Checks whether a string matches a regular expression.              |
| `regexExtract` | Extracts a portion of a string that matches a regular expression.  |
| `regexReplace` | Replaces a portion of a string that matches a regular expression.  |
| `regexSplit`   | Splits a string around matches of a regular expression.            |
| `regexCount`   | Returns the number of matches of a regular expression in a string. |

## Null Handling

| Function name | Description                                 |
|---------------|---------------------------------------------|
| `coalesce`    | Returns the first non-null value in a list. |

## Dates

| Function name    | Description                                                 |
|------------------|-------------------------------------------------------------|
| `year`           | returns year - year('2006-01-01') will return 2006          |
| `month`          | returns month - month('2012-05-01') will return 5           |
| `day`            | returns month - day('2012-05-10') will return 10            |
| `week`           | return calendar week('2021-01-06) will return 1             |
| `dateAdd`        | Adds an interval to a date.                                 |
| `dateSub`        | Subtracts an interval from a date.                          |
| `hour`           | Returns the hour of a date.                                 |
| `minute`         | Returns the minute of a date.                               |
| `second`         | Returns the second of a date.                               |
| `microsecond`    | Returns the microsecond of a date.                          |
| `datePart`       | Returns the specified part of a date.                       |
| `extract`        | Extracts a part of a date.                                  |
| `formatDate`     | Formats a date as a string according to a specified format. |
| `now`            | Returns the current date and time.                          |
| `currentDate`    | Returns the current date.                                   |
| `currentTime`    | Returns the current time.                                   |
| `toDate`         | Converts a string to a date.                                |
| `dateDiff`       | Returns the difference between two dates.                   |
| `weekday`        | Returns the weekday of a date.                              |
| `dayOfYear`      | Returns the day of the year of a date.                      |
| `quarter`        | Returns the quarter of a date.                              |
| `isStartOfMonth` | Returns whether a date is the start of a month.             |
| `isEndOfMonth`   | Returns whether a date is the end of a month.               |
| `isWeekend`      | Returns whether a date is a weekend.                        |

## Control Flow

| Function name | Description                                                   |
|---------------|---------------------------------------------------------------|
| `if`          | Returns one value if a condition is true and another value if |
