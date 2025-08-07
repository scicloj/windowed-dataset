# Windowed Dataset

A Clojure library for efficient time-series analysis with fixed-memory streaming data processing.

## Docs
[Here](https://scicloj.github.io/windowed-dataset)

## Overview

Working with streaming data often means dealing with two competing needs: analyzing recent trends while keeping memory usage under control. Windowed Dataset solves this by providing a circular buffer that maintains only the most recent N data points, automatically discarding older data as new data arrives.

Think of it as a sliding window over your data stream - you always see the most recent activity while using constant memory, regardless of how much data flows through your system.

## How It Works

Under the hood, windowed datasets use a **circular buffer**. Imagine an array with a pointer that moves in a circle: when new data arrives, it overwrites the oldest data and moves the pointer forward. When the pointer reaches the end, it wraps back to the beginning.

This simple technique provides powerful benefits:
- **Constant memory**: The buffer never grows beyond your specified size
- **Fast insertion**: Adding new data is always O(1) - just overwrite and increment
- **Automatic cleanup**: No need to manually delete old data - it's automatically replaced
- **Cache-friendly**: Data stays in contiguous memory for better performance

## Status
Alpha stage - API may change

## Key Features

- **Fixed memory usage** - Process unlimited streaming data with constant memory footprint
- **Time-based windows** - Extract data from specific time ranges (e.g., "last 5 minutes")
- **Streaming-friendly** - Insert new data in constant time
- **Seamless integration** - Works naturally with Tablecloth/tech.ml.dataset
- **Zero-copy views** - Extract time windows without data copying

⚠️ **Note**: This library uses mutable data structures for performance. Use with appropriate care in concurrent environments.

## Installation

Add to your `deps.edn`:

```clojure
{:deps {scicloj/windowed-dataset {:local/root "path/to/windowed-dataset"}}}
```

## Quick Example

```clojure
(require '[scicloj.windowed-dataset.api :as wd]
         '[java-time.api :as java-time])

;; Create a buffer for temperature sensor data (keep last 100 readings)
(def sensor-buffer 
  (wd/make-windowed-dataset {:timestamp :instant 
                             :temperature :float64 
                             :location :string} 100))

;; Add streaming data
(def updated-buffer 
  (wd/insert-to-windowed-dataset! 
    sensor-buffer 
    {:timestamp (java-time/instant)
     :temperature 23.5
     :location "greenhouse-1"}))

;; Get all current data as a regular dataset
(wd/windowed-dataset->dataset updated-buffer)

;; Get only the last 5 minutes of data
(wd/windowed-dataset->time-window-dataset updated-buffer :timestamp 300000)
```

## Common Patterns

### Streaming Analytics
Calculate metrics on recent data without storing the entire history:

```clojure
(defn current-average [windowed-ds]
  (let [recent-data (wd/windowed-dataset->dataset windowed-ds)
        temperatures (:temperature recent-data)]
    (when (seq temperatures)
      (/ (reduce + temperatures) (count temperatures)))))
```

### Historical Analysis
Add progressive features to existing time-series data:

```clojure
(wd/add-column-by-windowed-fn 
  historical-data
  {:colname :rolling-average
   :windowed-fn current-average
   :windowed-dataset-size 50})
```

### Real-time Processing
Process continuous data streams with bounded memory:

```clojure
;; Process sensor readings as they arrive
(reduce wd/insert-to-windowed-dataset! 
        sensor-buffer 
        real-time-sensor-stream)
```

## When to Use This

**Great for:**
- Real-time dashboards and monitoring
- Streaming analytics and alerts  
- IoT sensor data processing
- Financial tick data analysis
- Online feature engineering for ML models

**Not ideal for:**
- Small datasets that fit in memory
- Analyses requiring access to full historical data
- Applications needing data persistence across restarts

## Performance

- **Memory**: O(window-size) - constant regardless of total data processed
- **Insertion**: O(1) - constant time to add new data
- **Time queries**: O(log n) - efficient binary search for time ranges
- **Conversion**: Zero-copy - views over existing data

## Dependencies

- [Tablecloth](https://scicloj.github.io/tablecloth/) - For dataset operations
- [java-time](https://github.com/dm3/clojure.java-time) - For time handling

## License

Copyright © 2025 Scicloj

Distributed under the Eclipse Public License version 1.0.
