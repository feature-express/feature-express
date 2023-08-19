We can differentiate and name different types of partial aggregates by creating separate traits for each category.
Here's how you can name and categorize them:

a) SequentialPartialAggregate: Partial aggregates that can be merged/evaluated fast only in a sequential order. The
update function must take sequential data, and merge can only merge states that are "next" to each other.

b) DateTimeLinkedPartialAggregate: Partial aggregates that need to be linked with DateTime for functions like argmax,
argmin.

c) Composite

There is also a type of a partial aggregate that uses the result of some other aggregation as input.
For example `count below mean` needs the mean value calculated otherwise it would be very hard to calculate this
operation as pure partial aggregate.

It is probably the easiest to include `mean` value as the parameter for a new instance of partial aggregate.

In short it can look like some sort of a closure:

```Python
mean = PartialSum.apply(values)
count_below_mean = PartialCountBelowMean::new(mean)
```

More ideas

Median Absolute Deviation (MAD): Even if the median itself might be tricky to compute efficiently with a subtractive operation, the absolute deviations from a median (or mean, if median is too cumbersome) can be stored and adjusted.
Autocorrelation at Lag k: This measures the relationship between a series and a lagged version of itself. The subtraction should involve removing the effect of the earliest value in the aggregate and adding the effect of the new value.
Historical Returns: In the financial domain, it's the profit or loss made over a particular period, expressed as a percentage of original investment. It's relatively simple as it involves the initial and the current value.
Percentage Change: Shows the percent change from one time step to the next in a series. This could be updated with new data points and adjusted when removing old ones.
Cumulative Sum: While you might have sum, the running total (or subtraction of a particular value from this total) could be valuable in specific contexts.
Entropy: If you can maintain a histogram or frequency count of unique values in the series, you can compute entropy. Subtracting would involve decreasing the count of the value being removed.
Longest Strike Above Mean and Longest Strike Below Mean: This can be updated by keeping track of current streaks and max streaks. When adding a value, update the streak. When subtracting, you'd need to potentially reevaluate the streak, which might be the tricky part.
Count Above Mean and Count Below Mean: Easy to update. When a new value is added that's above/below the mean, increment the count. When a value is subtracted, decrement the count. (Adjusting the mean could be the challenging part here).
Coefficient of Variation: Given that it's the ratio of the standard deviation to the mean, and you already calculate both, this should be straightforward.
Zero-crossing Rate: Track the number of times values change sign. It should be relatively simple to adjust when adding or removing values from the series.
Peak-to-Peak: It is essentially maximum - minimum, which you already compute.
Range Count: Given two values, a and b, it counts the number of times the series value falls within this range.


Aggregation Primitives (feature tools)

Implemented

All() - Calculates if all values are 'True' in a list.
Any() - Determines if any value is 'True' in a list.
Count() - Determines the total number of values, excluding NaN.
AvgTimeBetween([unit]) - Computes the average number of seconds between consecutive events.
First() - Determines the first value in a list.
Last() - Determines the last value in a list.
Max() - Calculates the highest value, ignoring NaN values.
Median() - Determines the middlemost number in a list of values.
Min() - Calculates the smallest value, ignoring NaN values.
Sum() - Calculates the total addition, ignoring NaN.
Std() - Computes the dispersion relative to the mean value, ignoring NaN.
NumUnique() - Determines the number of distinct values, ignoring NaN values.

Not implemented

CountAboveMean([skipna]) - Calculates the number of values that are above the mean.
CountBelowMean([skipna]) - Determines the number of values that are below the mean.
CountGreaterThan([threshold]) - Determines the number of values greater than a controllable threshold.
CountInsideNthSTD([n]) - Determines the count of observations that lie inside
CountInsideRange([lower, upper, skipna]) - Determines the number of values that fall within a certain range.
CountLessThan([threshold]) - Determines the number of values less than a controllable threshold.
CountOutsideNthSTD([n]) - Determines the number of observations that lie outside
CountOutsideRange([lower, upper, skipna]) - Determines the number of values that fall outside a certain range.
Entropy([dropna, base]) - Calculates the entropy for a categorical column
MaxConsecutiveTrue() - Determines the maximum number of consecutive True values in the input
MaxConsecutiveFalse() - Determines the maximum number of consecutive False values in the input
MaxConsecutiveNegatives([skipna]) - Determines the maximum number of consecutive negative values in the input
MaxConsecutivePositives([skipna]) - Determines the maximum number of consecutive positive values in the input
MaxConsecutiveZeros([skipna]) - Determines the maximum number of consecutive zero values in the input
Mode() - Determines the most commonly repeated value.
NMostCommon([n]) - Determines the n most common elements.
NumConsecutiveGreaterMean([skipna]) - Determines the length of the longest subsequence above the mean.
NumConsecutiveLessMean([skipna]) - Determines the length of the longest subsequence below the mean.
NumTrue() - Counts the number of True values.
PercentTrue() - Determines the percent of True values.
Skew() - Computes the extent to which a distribution differs from a normal distribution.
TimeSinceFirst([unit]) - Calculates the time elapsed since the first datetime (in seconds).
TimeSinceLast([unit]) - Calculates the time elapsed since the last datetime (default in seconds).
TimeSinceLastFalse() - Calculates the time since the last False value.
TimeSinceLastMax() - Calculates the time since the maximum value occurred.
TimeSinceLastMin() - Calculates the time since the minimum value occurred.
TimeSinceLastTrue() - Calculates the time since the last True value.
Trend() - Calculates the trend of a column over time.
