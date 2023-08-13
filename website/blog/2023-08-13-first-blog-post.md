---
slug: feature-express-introduction
title: FeatureExpress introduction
authors:
  name: Paweł Jankiewicz 
  title: Data Scientist, Kaggle Grandmasters
  url: https://github.com/pjankiewicz
  image_url: https://github.com/pjankiewicz.png
tags: [hello, feature-express]
---

# Introducing FeatureExpress: A Kaggle Grandmaster's Journey into Event-Driven Feature Engineering

Hello Data Enthusiasts!

My name is Paweł Jankiewicz, a Kaggle competition grandmaster (https://www.kaggle.com/paweljankiewicz), and a fervent advocate for the power of event data. My passion for customer analytics and recommendation systems has led me down a path where handling event data became a daily affair. The challenges and complexities of modeling time and events have always intrigued me, and Rust - a language I've grown to love - has been my partner in solving performance-intensive calculations.

Today, I'm thrilled to introduce you to my latest creation: **FeatureExpress**.

## What is FeatureExpress?

FeatureExpress is an in-memory feature engineering library that leverages Rust's efficiency and provides an easy-to-use Python interface. This alpha release of FeatureExpress is not perfect and still has some missing pieces, especially around incremental features. However, the decision to release it now is aimed at gauging interest and finding collaborators and investors to help me take this project to the next level.

### The Power of Event Data

Event data is the beating heart of any analytical system that deals with time. From capturing user interactions in real-time to processing and making predictions, handling event data correctly is key to unlocking the full potential of any dataset.

### Unique Features of FeatureExpress

- **Clear Separation of Time**: Avoid subtle data leaks with distinct handling of past and future.
- **Complex Time-Based Joins**: Implement JOINS in time effortlessly.
- **High Performance**: Written in Rust, expect parallel and speedy materialization of features.
- **Declarative Syntax**: Define what you want, not how you want it, using our SQL-like DSL.

### What's Missing?

This being an alpha release, there are certain aspects that are still under development, particularly around some computation modes like incremental features. But fear not! The core functionalities are robust, and I believe it's time to share this innovative tool with the community.

## Why FeatureExpress?

If you, like me, find joy in taming the intricacies of event data, especially in customer analytics and recommendation systems, FeatureExpress will resonate with you. It embodies my years of experience, struggle, and learning in dealing with time-centric data.

## Collaboration and Investment Opportunities

The journey of FeatureExpress is just beginning, and I am actively seeking collaborators who share this vision. Whether you are an aspiring contributor or an investor looking for the next big thing in data science, FeatureExpress offers a unique opportunity.

## Conclusion

FeatureExpress is a love letter to event data, Rust, and the pursuit of effective feature engineering. I invite you to explore this alpha release and share your feedback, criticisms, or even a virtual high-five.

With FeatureExpress, we're taking a significant step towards making feature engineering a more expressive and event-driven endeavor. Join me in this exciting journey!

Happy data wrangling!

Paweł Jankiewicz,
Kaggle Grandmaster and Creator of FeatureExpress
