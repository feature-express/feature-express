import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Efficient In-Memory Processing',
//    img: require('@site/static/img/r_letter_5.png').default,
    description: (
      <>
        Built with Rust, FeatureExpress offers a high-performance in-memory engine, enabling fast calculations and minimizing latency. Benefit from performance tricks like incremental updates of overlapping aggregates.
      </>
    ),
  },
  {
    title: 'Powerful Time-based Queries',
//    img: require('@site/static/img/feature2.png').default,
    description: (
      <>
        Design complex temporal queries and calculate features over predefined intervals, fixed dates, conditional dates, or custom observation times. Enjoy the clarity and robustness with a separation between past and future to avoid data leaks.
      </>
    ),
  },
  {
    title: 'Flexible Domain-Specific Language (DSL)',
//    img: require('@site/static/img/undraw_programming_language.svg').default,
    description: (
      <>
        Express your feature engineering logic using FeatureExpress's own DSL. Tailored for data scientists, the DSL provides a clear syntax to define a wide variety of queries, including time-based JOINS, aggregation functions, and filtering criteria.
      </>
    ),
  },
  {
    title: 'Versatile Value Representation',
//    img: require('@site/static/img/undraw_data_structure.svg').default,
    description: (
      <>
        FeatureExpress supports a broad array of value types including numbers, strings, dates, and complex structures like maps and vectors, allowing you to handle various data formats and create rich feature sets.
      </>
    ),
  },
  {
    title: 'Convenient Indexing',
//    img: require('@site/static/img/undraw_indexing_data.svg').default,
    description: (
      <>
        The in-memory event store is equipped with multiple indices, including global and entity-specific indices, ensuring optimal retrieval and efficient management of event data.
      </>
    ),
  },
];


function Feature({img, title, description}) {
      //<div className="text--center">
      //  <img width="200px" src={img} className={styles.featureImg} alt={title} /> {/* Change this line */}
      //</div>
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}