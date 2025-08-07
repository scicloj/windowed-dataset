(ns windowed-dataset-examples
  "Examples and demonstrations of windowed dataset functionality.
  
  This notebook showcases the core capabilities of the windowed-dataset library
  for streaming data analysis and time-series processing."
  (:require [scicloj.windowed-dataset.api :as wd]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [java-time.api :as java-time]
            [scicloj.kindly.v4.kind :as kind]
            [tech.v3.datatype.functional :as dfn]))

;; # Windowed Dataset Examples
;; 
;; This notebook demonstrates the core functionality of windowed datasets
;; for streaming data analysis and time-series processing.

;; ## Basic Usage

;; Create a simple windowed dataset with timestamp and value columns
(def sample-column-types
  {:timestamp :instant
   :value :float64
   :sensor-id :string})

;; Create a windowed dataset with maximum size of 5 records
(def windowed-ds
  (wd/make-windowed-dataset sample-column-types 5))

;; **Initial windowed dataset:**
windowed-ds

;; ## Inserting Data

;; Let's create some sample time-series data
(def sample-data
  (let [start-time (java-time/instant)]
    (map (fn [i]
           {:timestamp (java-time/plus start-time (java-time/seconds i))
            :value (+ 10.0 (* 2.0 (Math/sin (/ i 2.0))))
            :sensor-id "sensor-1"})
         (range 10))))

;; **Sample time-series data (first 5 rows):**
(tc/dataset (take 5 sample-data))

;; Insert data progressively and observe the windowed dataset behavior
(defn demonstrate-progressive-insertion []
  (reduce (fn [acc-ds row]
            (let [updated-ds (wd/insert-to-windowed-dataset! acc-ds row)
                  regular-ds (wd/windowed-dataset->dataset updated-ds)]
              (println (str "After inserting row with value " (:value row) ":"))
              (println (str "  Window size: " (:current-size updated-ds)))
              (println (str "  Current position: " (:current-position updated-ds)))
              (println (str "  Data in window: " (vec (:value regular-ds))))
              (println)
              updated-ds))
          windowed-ds
          (take 8 sample-data)))

;; **Progressive insertion demonstration:**
(kind/code
 (with-out-str (demonstrate-progressive-insertion)))

;; ## Time Window Functionality

;; Create a windowed dataset with more data for time window examples
(def full-windowed-ds
  (reduce wd/insert-to-windowed-dataset! windowed-ds sample-data))

;; **Full windowed dataset converted to regular dataset:**
(wd/windowed-dataset->dataset full-windowed-ds)

;; Demonstrate time window extraction
(let [regular-ds (wd/windowed-dataset->dataset full-windowed-ds)
      latest-time (last (:timestamp regular-ds))]
  ;; **Time window examples:**
  {:last-3-seconds (wd/windowed-dataset->time-window-dataset full-windowed-ds :timestamp 3000)
   :last-5-seconds (wd/windowed-dataset->time-window-dataset full-windowed-ds :timestamp 5000)
   :latest-timestamp latest-time})

;; ## Real-time Streaming Simulation

;; Demonstrate how windowed datasets can be used for streaming analysis
(defn calculate-moving-average [windowed-ds]
  "Calculate moving average from a windowed dataset"
  (let [regular-ds (wd/windowed-dataset->dataset windowed-ds)
        values (:value regular-ds)]
    (when (seq values)
      (dfn/mean values))))

;; Simulate streaming analysis with moving averages
(defn streaming-analysis-demo []
  (let [results (atom [])]
    (reduce (fn [acc-ds row]
              (let [updated-ds (wd/insert-to-windowed-dataset! acc-ds row)
                    moving-avg (calculate-moving-average updated-ds)]
                (swap! results conj {:timestamp (:timestamp row)
                                     :value (:value row)
                                     :moving-average moving-avg})
                updated-ds))
            windowed-ds
            sample-data)
    @results))

;; **Streaming analysis with moving averages:**
(tc/dataset (streaming-analysis-demo))

;; ## Advanced Example: Progressive Column Addition

;; Demonstrate add-column-by-windowed-fn for batch processing
(def time-series-data
  (tc/dataset {:timestamp (map #(java-time/plus (java-time/instant) (java-time/seconds %)) (range 10))
               :value (map #(+ 5.0 (* 3.0 (Math/sin (/ % 1.5)))) (range 10))
               :noise (repeatedly 10 #(* 0.5 (- (rand) 0.5)))}))

;; **Original time series:**
time-series-data

;; Add progressive moving average column
(def windowed-moving-avg-fn
  (fn [windowed-ds]
    (calculate-moving-average windowed-ds)))

;; **Time series with progressive moving average:**
(wd/add-column-by-windowed-fn
 time-series-data
 {:colname :progressive-moving-avg
  :windowed-fn windowed-moving-avg-fn
  :windowed-dataset-size 120})

;; ## Performance Characteristics

;; Demonstrate that windowed datasets maintain constant memory usage
(defn memory-usage-demo []
  (let [large-dataset (map (fn [i]
                             {:timestamp (java-time/plus (java-time/instant) (java-time/millis i))
                              :value (rand)
                              :sensor-id "perf-test"})
                           (range 1000))
        windowed-ds-small (wd/make-windowed-dataset sample-column-types 10)
        windowed-ds-large (wd/make-windowed-dataset sample-column-types 100)]

    {:small-window {:max-size 10
                    :final-size (:current-size (reduce wd/insert-to-windowed-dataset! windowed-ds-small large-dataset))}
     :large-window {:max-size 100
                    :final-size (:current-size (reduce wd/insert-to-windowed-dataset! windowed-ds-large large-dataset))}
     :input-data-size (count large-dataset)}))

;; **Memory usage demonstration:**
(memory-usage-demo)

;; ## Summary

;; ## Key Benefits of Windowed Datasets
;;
;; - **Constant Memory Usage**: Fixed-size circular buffer regardless of input stream size
;; - **Efficient Time Windows**: O(log n) binary search for time-based filtering  
;; - **Streaming-Friendly**: Designed for real-time data processing
;; - **Tablecloth Integration**: Seamless conversion to/from regular datasets
;; - **High Performance**: Built on `tech.ml.dataset` for efficient numeric operations
;;
;; ## Common Use Cases
;;
;; 1. **Real-time Analytics**: Moving averages, trend detection, anomaly detection
;; 2. **Streaming ML**: Feature engineering for time-series models
;; 3. **Sensor Data Processing**: IoT and monitoring applications
;; 4. **Financial Data**: Technical indicators and risk metrics
;; 5. **Scientific Computing**: Signal processing and time-series analysis
