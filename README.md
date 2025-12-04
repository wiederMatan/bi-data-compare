# BI Data Compare

This project provides tools for comparing data between different business intelligence (BI) sources. Its primary goal is to facilitate the identification of discrepancies and ensure data consistency across various BI platforms.

## Table of Contents
- [Features](#features)
    - [Comparison Modes](#comparison-modes)

## Features

### Comparison Modes

The following table outlines the different comparison modes available, highlighting their speed, capabilities, and typical use cases:

| Feature | QUICK | STANDARD | DEEP |
|---|---|---|---|
| Speed | Fastest (seconds) | Moderate (minutes) | Slowest (comprehensive) |
| Schema comparison | ✅ Yes | ✅ Yes | ✅ Yes |
| Data comparison | Checksum only | Row-by-row | Row-by-row |
| Shows specific differences | ❌ No | ✅ Yes | ✅ Yes |
| Shows which rows differ | ❌ No | ✅ Yes | ✅ Yes |
| Shows which columns differ | ❌ No | ✅ Yes | ✅ Yes |
| Indexes comparison | ❌ No | ❌ No | ✅ (planned, not implemented) |
| Constraints comparison | ❌ No | ❌ No | ✅ (planned, not implemented) |
| Use case | "Are tables different?" | "What exactly is different?" | "Complete audit"