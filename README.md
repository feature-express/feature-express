<img src="https://feature.express/img/logo_blocky.png" width="100%"/>

# FeatureExpress: Time-aware Feature Engineering Library

[![PyPI version](https://badge.fury.io/py/fexpress.svg)](https://badge.fury.io/py/fexpress)

## Overview

FeatureExpress is a groundbreaking in-memory feature engineering library designed for processing time-based event data. It is a hybrid between a feature engineering library and a feature store, aiming to address the complex challenges of dealing with temporal data in machine learning applications.

- Website: https://feature.express
- Discord: [![](https://dcbadge.vercel.app/api/server/zGWGRtG4)](https://discord.gg/zGWGRtG4)

## Prerelease

> :warning: **Alpha Release Warning:** This library is currently in an **alpha** stage. As such, it is subject to:
>
> - **Changes:** The API is still evolving, so you can expect many breaking changes. If you depend on this library in your project, be prepared to update your code as new versions are released.
> - **Performance Issues:** There may be inefficiencies or other performance issues that have not yet been resolved.
> - **Unstable API:** Functionality might be added, changed, or removed without notice. Documentation may be incomplete or out of date.
>
> This version is more like a **pre-release**, and it's primarily intended for developers who are interested in experimenting with the latest features or contributing to the project.

## Why FeatureExpress?

### Why Another Feature Engineering Library / Feature Store?

The necessity of this unique library grew from years of struggling with event-driven data, especially in customer interactions and recommendations. Time adds complexity, subtlety, and depth to data analysis and modeling. The challenges include:

- Time makes everything complex.
- Model validation becomes harder.
- Data leaks are subtle and hard to trace.
- SQL-like operations are prone to errors and hard to write.
- Existing feature stores move the burden of materialization to data scientists.

### Event Data for Superior Features

Event data encapsulates reality with timestamped information, and it's pivotal in creating meaningful features. Unlike other methods that often obscure temporal aspects, FeatureExpress utilizes a dedicated data structure to make the connection between events and features clearer and more explicit.

### Overcoming Problems with Current Feature Stores

Current feature stores often rely on explicit materialization and caching, leading to increased complexity for data scientists. FeatureExpress adopts a declarative approach (similar to SQL) with a DSL (Domain Specific Language) to define features, allowing for a more intuitive and error-free process.

### In-Memory Processing

Built in Rust and interfaced in Python, FeatureExpress leverages in-memory processing to enable:

- Fast materialization of features.
- Parallel computations for efficiency.
- Flexibility to expand to more permanent storage solutions in the future.

Though the current version is limited to datasets that fit in memory, FeatureExpress's performance and robustness make it a valuable tool for data scientists and engineers working with time-series data.

## Installation

You can install FeatureExpress via pip:

```bash
pip install fexpress
```

## Features

1. **Event-Driven Design:** Utilizes events as core data structures for accurate modeling.
2. **Time-Aware DSL:** Introduces a SQL-like DSL for expressive and complex feature declarations.
3. **No Data Leaks:** The clear separation between past and future guarantees against inadvertent data leaks.
4. **Flexible Observation Dates:** Allows custom definitions of observation dates including intervals, fixed, conditional, and more.
5. **Time-based Joins:** Enables complicated joins in time, like aggregations over specific periods.
6. **Optimized Performance:** Implements performance tricks like partial aggregates for efficient calculations.
7. **Rich Value Representation:** Accommodates various data types for broad applications.
8. **Indices and In-memory Store:** Ensures optimized querying and manipulation of time-based data.

## Documentation

Full documentation, including tutorials and examples, can be found at https://feature.express.

## Contributing

Interested in contributing to FeatureExpress? See our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to help!

## License

FeatureExpress is under MIT. See [LICENSE](LICENSE) for more details.

## Development

```
env VIRTUAL_ENV=$(python3 -c 'import sys; print(sys.base_prefix)') maturin develop
```

or

```
maturin develop
```

# development (optimized code)

```
maturin develop --release
```

# building Python wheel

```
maturin build --release -i python
```

This should create a wheel in `target/wheels`

# installing Python wheel

```
pip install target/wheels/fexpress_rs-0.1.0-cp38-cp38-linux_x86_64.whl -U
```

Note that the file name can be different depending on your system.



