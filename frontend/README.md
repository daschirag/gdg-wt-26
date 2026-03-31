# AQE Dashboard - Storage-Aware Approximate Query Engine

A modern, high-performance Rust-based analytics engine with an intuitive web dashboard.

## Overview

AQE (Approximate Query Engine) is designed for low-latency analytics through intelligent data sampling and multi-modal pipeline execution. This repository contains the web dashboard frontend built with Astro and Tailwind CSS.

## Features

- **Storage Layer**: LSM-tree with memtable, SSTables, and columnar segments
- **Query Layer**: SQL parser, planner, and pipelines (row, columnar, mixed)
- **AQP Layer**: Sampling, estimators, and accuracy control with 0.70 target accuracy
- **Real-time Config**: Live configuration snapshot showing engine metrics
- **Responsive Design**: Mobile-first approach with modern neomorphic UI

## Tech Stack

- **Framework**: Astro 6.1.2
- **Styling**: Tailwind CSS with Material Design 3 colors
- **Typography**: Space Grotesk (headlines), Inter (body)
- **Icons**: Google Material Symbols

## Getting Started

### Prerequisites
- Node.js 16+ and npm

### Installation

```bash
npm install
```

### Development

```bash
npm run dev
```

The development server will start at `http://localhost:4322/`

### Build

```bash
npm run build
```

## Project Structure

```
.
├── src/
│   ├── pages/
│   │   └── index.astro       # Main dashboard page
│   ├── layouts/
│   │   └── MainLayout.astro  # Root layout with config
│   └── styles/
│       └── global.css         # Global styles
├── astro.config.mjs
├── tsconfig.json
└── package.json
```

## Configuration

The dashboard displays live metrics configured in the Config Snapshot:
- **Accuracy Target**: 0.70
- **Sampling Exponent (k)**: 2.0
- **Memtable Size**: 1,048,576 rows
- **Resource Utilization**: 70%

## Design System

The UI uses a custom Material Design 3 color palette with neomorphic styling:
- Primary Color: `#2c50cd`
- Surface: `#f6f9ff`
- Neomorphic shadows and depth effects for modern UI experience

## License

Rust 2024 | AQE Engine v0.1.0
