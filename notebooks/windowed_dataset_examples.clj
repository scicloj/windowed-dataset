(ns windowed-dataset-examples
  "Examples demonstrating windowed dataset functionality for streaming data analysis."
  (:require [scicloj.windowed-dataset.api :as wd]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [java-time.api :as java-time]
            [scicloj.kindly.v4.kind :as kind]
            [tech.v3.datatype.functional :as dfn]))

;; # Windowed Dataset Examples
;; 
;; This notebook shows how to use windowed datasets for streaming data analysis.
;; We'll work with temperature sensor data as a relatable example, but the same 
;; patterns apply to any time-series data.

;; ## The Problem: Streaming Data with Limited Memory

;; Imagine you're processing temperature readings from sensors. New readings arrive
;; continuously, but you only care about recent trends and don't want to store
;; years of historical data in memory.
;;
;; Windowed datasets solve this by maintaining a "sliding window" of recent data.

;; ## Basic Setup

;; Let's define our data structure: timestamp, temperature, and sensor location
(def column-types
  {:timestamp :instant
   :temperature :float64
   :location :string})

;; Create a windowed dataset that keeps only the last 5 readings
(def temperature-buffer
  (wd/make-windowed-dataset column-types 5))

;; **Initial state - empty buffer:**
temperature-buffer

;; ## Simulating Streaming Data

;; Create sample temperature readings (like data from a greenhouse monitoring system)
(def sample-readings
  (let [start-time (java-time/instant)]
    (map (fn [i]
           {:timestamp (java-time/plus start-time (java-time/seconds (* i 30)))
            :temperature (+ 22.0 (* 2.0 (Math/sin (/ i 3.0)))) ; Simulated daily variation
            :location "greenhouse-a"})
         (range 10))))

;; **Sample data - first 5 readings:**
(tc/dataset (take 5 sample-readings))

;; ## Watching the Window in Action

;; Let's insert data one by one and watch how the window behaves
(defn show-window-evolution []
  (let [results (atom [])]
    (reduce (fn [buffer reading]
              (let [updated-buffer (wd/insert-to-windowed-dataset! buffer reading)
                    current-data (wd/windowed-dataset->dataset updated-buffer)]
                (swap! results conj
                       {:reading-number (count @results)
                        :buffer-size (:current-size updated-buffer)
                        :temperatures (vec (:temperature current-data))})
                updated-buffer))
            temperature-buffer
            (take 8 sample-readings))
    @results))

;; **Window evolution as data arrives:**
(tc/dataset (show-window-evolution))

;; Notice how:
;; - The buffer grows until it reaches maximum size (5)
;; - After that, old data is automatically discarded to make room for new data
;; - Memory usage stays constant regardless of how much data we process

;; ## Time-Based Windows

;; Often you don't just want the last N readings - you want "all data from the last X minutes"

;; Fill our buffer with all sample data
(def full-buffer
  (reduce wd/insert-to-windowed-dataset! temperature-buffer sample-readings))

;; **All data in buffer:**
(wd/windowed-dataset->dataset full-buffer)

;; Extract different time windows
(let [now (:timestamp (last sample-readings))]
  {:last-90-seconds (wd/windowed-dataset->time-window-dataset full-buffer :timestamp 90000)
   :last-150-seconds (wd/windowed-dataset->time-window-dataset full-buffer :timestamp 150000)
   :reference-time now})

;; ## Real-World Pattern: Streaming Analytics

;; Here's how you might calculate a running average temperature in a real application

(defn calculate-average-temperature [buffer]
  "Calculate current average temperature from buffer"
  (let [current-data (wd/windowed-dataset->dataset buffer)
        temperatures (:temperature current-data)]
    (when (seq temperatures)
      (dfn/mean temperatures))))

;; Simulate processing readings as they arrive, calculating moving averages
(defn streaming-analytics-demo []
  (let [results (atom [])]
    (reduce (fn [buffer reading]
              (let [updated-buffer (wd/insert-to-windowed-dataset! buffer reading)
                    avg-temp (calculate-average-temperature updated-buffer)]
                (swap! results conj
                       {:timestamp (:timestamp reading)
                        :current-temperature (:temperature reading)
                        :running-average avg-temp
                        :readings-in-buffer (:current-size updated-buffer)})
                updated-buffer))
            temperature-buffer
            sample-readings)
    @results))

;; **Streaming analytics results:**
(tc/dataset (streaming-analytics-demo))

;; ## Historical Analysis: Adding Progressive Features

;; Sometimes you have historical data and want to see how metrics would have
;; evolved over time, as if you were processing it in real-time.

(def historical-temperatures
  (tc/dataset {:timestamp (map #(java-time/plus (java-time/instant)
                                                (java-time/seconds %))
                               (range 12))
               :temperature (map #(+ 20.0 (* 3.0 (Math/sin (/ % 2.0))))
                                 (range 12))}))

;; **Original historical data:**
historical-temperatures

;; Add a progressive moving average column
(def with-moving-average
  (wd/add-column-by-windowed-fn
   historical-temperatures
   {:colname :progressive-average
    :windowed-fn calculate-average-temperature
    :windowed-dataset-size 120}))

;; **Historical data with progressive moving averages:**
with-moving-average

;; Notice how the moving average starts as nil (no data), then becomes the first value,
;; then a true average as more data accumulates.

;; ## Memory Efficiency Demo

;; Let's prove that windowed datasets use constant memory regardless of input size

(defn memory-efficiency-test []
  (let [;; Create lots of data
        lots-of-data (map (fn [i]
                            {:timestamp (java-time/plus (java-time/instant)
                                                        (java-time/millis i))
                             :temperature (+ 20.0 (rand 10.0))
                             :location "test-sensor"})
                          (range 1000))

        ;; Create two buffers of different sizes
        small-buffer (wd/make-windowed-dataset column-types 10)
        large-buffer (wd/make-windowed-dataset column-types 100)]

    ;; Process all data through both buffers
    (let [final-small (reduce wd/insert-to-windowed-dataset! small-buffer lots-of-data)
          final-large (reduce wd/insert-to-windowed-dataset! large-buffer lots-of-data)]

      {:input-data-size (count lots-of-data)
       :small-buffer-final-size (:current-size final-small)
       :large-buffer-final-size (:current-size final-large)
       :memory-usage "Constant - only depends on buffer size, not input size!"})))

;; **Memory efficiency test:**
(memory-efficiency-test)

;; ## Practical Applications

;; **Windowed datasets are perfect for:**
;;
;; 1. **Real-time monitoring** - Track recent system metrics, sensor readings, or user activity
;; 2. **Streaming alerts** - Detect anomalies based on recent patterns without storing everything
;; 3. **Live dashboards** - Show current trends and recent history with bounded memory
;; 4. **IoT data processing** - Handle continuous sensor streams efficiently
;; 5. **Financial analysis** - Calculate technical indicators on streaming market data
;; 6. **ML feature engineering** - Create time-based features for online learning models

;; **Key Benefits:**
;;
;; - **Predictable memory usage** - Never grows beyond your specified window size
;; - **Efficient time queries** - Binary search makes time-based filtering fast
;; - **Streaming-friendly** - Designed for continuous data processing
;; - **Seamless integration** - Works naturally with existing Clojure data tools

;; ## Summary

;; Windowed datasets provide a simple but powerful abstraction for handling streaming
;; time-series data. By maintaining only recent data, you can build efficient real-time
;; analytics systems that don't consume unbounded memory.
;;
;; The key insight is that many analyses only need recent context, not complete history.
;; Windowed datasets make this pattern explicit and efficient.
