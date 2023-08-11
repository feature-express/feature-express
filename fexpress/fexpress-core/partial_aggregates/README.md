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