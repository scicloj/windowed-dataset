;; # API Reference
;;
(ns windowed-dataset.api-reference
  (:require [scicloj.windowed-dataset.api :as wd]
            [tablecloth.api :as tc]
            [java-time.api :as java-time]
            [clojure.string :as str]))

(defn include-fnvar-as-section [fnvar]
  (let [{:keys [name arglists doc]} (meta fnvar)]
    (str (format "## `%s`\n\n" name)
         (->> arglists
              (map (fn [l]
                     (->> l
                          pr-str
                          (format "`%s`\n\n"))))
              (str/join ""))
         doc)))

;; ## WindowedDataset Record

;; The `WindowedDataset` record implements a circular buffer data structure optimized for time-series analysis:

;; ```clojure
;; (defrecord WindowedDataset
;;           [dataset           ; tech.v3.dataset containing the actual data
;;            column-types      ; map of column names to data types
;;            max-size         ; maximum number of rows the buffer can hold
;;            current-size     ; current number of rows (0 to max-size)
;;            current-position ; current write position (circular index)])
;; ```

;; **Key Characteristics:**

;; - **Mutable** - Use with caution
;; - **Fixed Memory** - Pre-allocates arrays for maximum performance
;; - **Circular Buffer** - New data overwrites oldest when buffer is full
;; - **Chronological Access** - Functions provide data in insertion order
;; - **Zero-Copy Views** - Time windows are extracted without data copying

;; **Usage Pattern:**

;; 1. Create with `make-windowed-dataset` specifying column types and buffer size
;; 2. Insert streaming data with `insert-to-windowed-dataset!` (❗**Caution: mutating the internal dataset.**)
;; 3. Extract time windows with `windowed-dataset->time-window-dataset`
;; 4. Compute metrics over specific time periods

;; ### WindowedDataset Structure Example

(let [;; Create a windowed dataset to examine its structure
      windowed-ds (wd/make-windowed-dataset {:timestamp :instant :value :float64} 3)
      base-time (java-time/instant)

      ;; Add one data point to see the structure
      wd-with-data (wd/insert-to-windowed-dataset! windowed-ds {:timestamp base-time :value 42.5})]

  ;; **WindowedDataset Record Fields:**
  {:dataset "tech.v3.dataset (Internal data storage)"
   :column-types (:column-types wd-with-data)
   :max-size (:max-size wd-with-data)
   :current-size (:current-size wd-with-data)
   :current-position (:current-position wd-with-data)})

;; ### Circular Buffer Behavior

(let [;; Demonstrate circular buffer behavior
      small-wd (wd/make-windowed-dataset {:value :int32} 3)

      ;; Fill beyond capacity to show circular behavior
      test-data (map (fn [i] {:value i}) (range 5))
      final-wd (reduce wd/insert-to-windowed-dataset! small-wd test-data)]

  ;; **Circular Buffer Example (capacity: 3, inserted: 5 values):**
  ;; Final state: size=3, position=2 (values 0,1 were overwritten by 3,4)
  ;; **Data in chronological order:**
  (wd/windowed-dataset->dataset final-wd))

(include-fnvar-as-section #'wd/make-windowed-dataset)

;; ### Example

(let [;; Create a windowed dataset for sensor data with 10-sample capacity
      column-spec {:timestamp :instant
                   :temperature :float64
                   :sensor-id :string}
      windowed-ds (wd/make-windowed-dataset column-spec 10)]

  ;; **Created windowed dataset:**
  {:max-size (:max-size windowed-ds)
   :current-size (:current-size windowed-ds)
   :current-position (:current-position windowed-ds)
   :column-types (:column-types windowed-ds)})

(include-fnvar-as-section #'wd/insert-to-windowed-dataset!)

;; ### Example

(let [;; Create windowed dataset
      windowed-ds (wd/make-windowed-dataset {:timestamp :instant :temperature :float64 :sensor-id :string} 5)
      base-time (java-time/instant)

      ;; Insert some data points
      sample-data [{:timestamp base-time :temperature 22.5 :sensor-id "temp-001"}
                   {:timestamp (java-time/plus base-time (java-time/seconds 30)) :temperature 23.1 :sensor-id "temp-001"}
                   {:timestamp (java-time/plus base-time (java-time/seconds 60)) :temperature 22.8 :sensor-id "temp-001"}]

      ;; Insert data step by step
      wd-step1 (wd/insert-to-windowed-dataset! windowed-ds (first sample-data))
      wd-step2 (wd/insert-to-windowed-dataset! wd-step1 (second sample-data))
      final-wd (wd/insert-to-windowed-dataset! wd-step2 (last sample-data))]

  ;; **Windowed dataset after inserting 3 records:**
  ;; Current size: 3
  ;; **Data view:**
  (wd/windowed-dataset->dataset final-wd))

(include-fnvar-as-section #'wd/windowed-dataset-indices)

;; ### Example

(let [;; Create and populate a small windowed dataset
      windowed-ds (wd/make-windowed-dataset {:value :int32} 4)
      ;; Insert 6 items (will wrap around)
      final-wd (reduce wd/insert-to-windowed-dataset! windowed-ds
                       (map (fn [i] {:value i}) (range 6)))]

  ;; **Windowed dataset with circular buffer behavior:**
  ;; Dataset state: size=4, position=2, max=4
  ;; **Index order for chronological access:**
  {:indices (wd/windowed-dataset-indices final-wd)
   ;; **Data in insertion order:**
   :data (wd/windowed-dataset->dataset final-wd)})

(include-fnvar-as-section #'wd/windowed-dataset->dataset)

;; ### Example

(let [;; Create windowed dataset with sample sensor data
      base-time (java-time/instant)
      sensor-readings (map (fn [i reading]
                             {:timestamp (java-time/plus base-time (java-time/seconds (* i 30)))
                              :temperature reading
                              :reading-id i})
                           (range 8)
                           [22.1 22.5 22.8 23.2 22.9 23.1 22.7 22.4])
      windowed-ds (wd/make-windowed-dataset {:timestamp :instant :temperature :float64 :reading-id :int32} 5)
      final-wd (reduce wd/insert-to-windowed-dataset! windowed-ds sensor-readings)]

  ;; **Converting windowed dataset to regular dataset:**
  ;; Inserted 8 temperature readings into 5-capacity window (last 5 retained):
  (wd/windowed-dataset->dataset final-wd))

(include-fnvar-as-section #'wd/binary-search-timestamp-start)

;; ### Example

(let [;; Create sample timestamp data
      base-time (java-time/instant)
      timestamps (map #(java-time/plus base-time (java-time/seconds (* % 60))) (range 5))
      timestamp-col (vec timestamps)
      indices (vec (range 5))

      ;; Search for different target times
      search-cases [[(java-time/plus base-time (java-time/seconds 90)) "Between timestamps"]
                    [(java-time/plus base-time (java-time/seconds 120)) "Exact match"]
                    [(java-time/minus base-time (java-time/seconds 30)) "Before all timestamps"]
                    [(java-time/plus base-time (java-time/seconds 300)) "After all timestamps"]]]

  ;; **Binary search examples:**
  ;; Timestamps: [formatted times]
  (map (fn [[target-time description]]
         {:target-time (str target-time)
          :description description
          :found-position (wd/binary-search-timestamp-start timestamp-col indices target-time)})
       search-cases))

(include-fnvar-as-section #'wd/windowed-dataset->time-window-dataset)

;; ### Example

(let [;; Create realistic sensor scenario with timestamps
      base-time (java-time/instant)
      readings [22.1 22.3 21.9 22.5 22.2 22.7 22.0 22.4 22.1 21.8 22.2 22.0 22.6 22.1 22.5]

      ;; Create timestamped data (measurements every 30 seconds)
      sensor-data (map-indexed (fn [i reading]
                                 {:timestamp (java-time/plus base-time (java-time/seconds (* i 30)))
                                  :temperature reading
                                  :reading-id i})
                               readings)

      windowed-ds (wd/make-windowed-dataset {:timestamp :instant :temperature :float64 :reading-id :int32} 20)
      final-wd (reduce wd/insert-to-windowed-dataset! windowed-ds sensor-data)]

  ;; **Time window extraction examples:**
  ;; Created 15 temperature readings over ~7.5 minutes

  ;; **Last 2 minutes of data:**
  {:last-2-minutes (wd/windowed-dataset->time-window-dataset final-wd :timestamp 120000)

   ;; **Last 5 minutes of data:**
   :last-5-minutes (wd/windowed-dataset->time-window-dataset final-wd :timestamp 300000)

   ;; **All data (10-minute window):**
   :all-data (-> (wd/windowed-dataset->time-window-dataset final-wd :timestamp 600000)
                 (tc/select-columns [:reading-id :temperature]))})

(include-fnvar-as-section #'wd/copy-windowed-dataset)

;; ### Example

(let [;; Create and populate a windowed dataset
      base-time (java-time/instant)
      original-data [{:timestamp base-time :temperature 22.5}
                     {:timestamp (java-time/plus base-time (java-time/seconds 30)) :temperature 23.1}
                     {:timestamp (java-time/plus base-time (java-time/seconds 60)) :temperature 22.8}]

      windowed-ds (wd/make-windowed-dataset {:timestamp :instant :temperature :float64} 5)
      populated-wd (reduce wd/insert-to-windowed-dataset! windowed-ds original-data)

      ;; Create a deep copy
      copied-wd (wd/copy-windowed-dataset populated-wd)]

  ;; **Deep copy windowed dataset example:**
  {:original-state {:size (:current-size populated-wd)
                    :position (:current-position populated-wd)}
   :copied-state {:size (:current-size copied-wd)
                  :position (:current-position copied-wd)}
   :data-identical (= (tc/rows (wd/windowed-dataset->dataset populated-wd))
                      (tc/rows (wd/windowed-dataset->dataset copied-wd)))})

(include-fnvar-as-section #'wd/add-column-by-windowed-fn)

;; ### Examples

(let [time-series (tc/dataset {:timestamp [(java-time/instant)
                                           (java-time/plus (java-time/instant) (java-time/seconds 30))
                                           (java-time/plus (java-time/instant) (java-time/seconds 60))
                                           (java-time/plus (java-time/instant) (java-time/seconds 90))]
                               :value [10.0 20.0 15.0 25.0]})

      ;; Define a simple moving average function
      moving-avg-fn (fn [windowed-ds]
                      (let [regular-ds (wd/windowed-dataset->dataset windowed-ds)
                            values (:value regular-ds)]
                        (when (seq values)
                          (/ (reduce + values) (count values)))))

      result (wd/add-column-by-windowed-fn time-series
                                           {:colname :moving-avg
                                            :windowed-fn moving-avg-fn
                                            :windowed-dataset-size 10})]
  (tc/select-columns result [:timestamp :value :moving-avg]))

;; ## Smoothing Functions

(include-fnvar-as-section #'wd/moving-average)

;; ### Example

(let [wd (wd/make-windowed-dataset {:x :int32} 10)
      data [{:x 800} {:x 850} {:x 820}]
      populated-wd (reduce wd/insert-to-windowed-dataset! wd data)]
  (double (wd/moving-average populated-wd 3)))

(include-fnvar-as-section #'wd/median-filter)

;; ### Example

(let [wd (wd/make-windowed-dataset {:x :int32} 10)
      data [{:x 800} {:x 1200} {:x 820}] ; middle value is outlier
      populated-wd (reduce wd/insert-to-windowed-dataset! wd data)]
  (wd/median-filter populated-wd 3))

(include-fnvar-as-section #'wd/cascaded-median-filter)

;; ### Example

(let [wd (wd/make-windowed-dataset {:x :int32} 10)
      data [{:x 800} {:x 1200} {:x 820} {:x 1100} {:x 810}]
      populated-wd (reduce wd/insert-to-windowed-dataset! wd data)]
  (wd/cascaded-median-filter populated-wd))

(include-fnvar-as-section #'wd/exponential-moving-average)

;; ### Example

(let [wd (wd/make-windowed-dataset {:x :int32} 10)
      data [{:x 800} {:x 850} {:x 820}]
      populated-wd (reduce wd/insert-to-windowed-dataset! wd data)]
  (wd/exponential-moving-average populated-wd 0.3))

(include-fnvar-as-section #'wd/cascaded-smoothing-filter)

;; ### Example

(let [wd (wd/make-windowed-dataset {:x :int32} 15)
      ;; Data with noise and outliers
      data [{:x 800} {:x 820} {:x 1500} {:x 810}
            {:x 805} {:x 815} {:x 2000} {:x 812}
            {:x 808} {:x 795}]
      populated-wd (reduce wd/insert-to-windowed-dataset! wd data)]

  ;; Compare cascaded smoothing with individual methods
  {:median-only (wd/median-filter populated-wd 5)
   :moving-avg-only (wd/moving-average populated-wd 5)
   :cascaded-5-3 (wd/cascaded-smoothing-filter populated-wd 5 3)
   :cascaded-default (wd/cascaded-smoothing-filter populated-wd)})

(include-fnvar-as-section #'ppi/cascaded-smoothing-filter)

;; ### Example

(let [wd (ppi/make-windowed-dataset {:x :int32} 15)
      ;; Data with noise and outliers
      data [{:x 800} {:x 820} {:x 1500} {:x 810}
            {:x 805} {:x 815} {:x 2000} {:x 812}
            {:x 808} {:x 795}]
      populated-wd (reduce ppi/insert-to-windowed-dataset! wd data)]

  ;; Compare cascaded smoothing with individual methods
  {:median-only (ppi/median-filter populated-wd 5)
   :moving-avg-only (ppi/moving-average populated-wd 5)
   :cascaded-5-3 (ppi/cascaded-smoothing-filter populated-wd 5 3)
   :cascaded-default (ppi/cascaded-smoothing-filter populated-wd)})
