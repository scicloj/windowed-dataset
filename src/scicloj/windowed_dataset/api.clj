(ns scicloj.windowed-dataset.api
  "A general-purpose windowed dataset implementation for streaming data analysis.
  
  Provides a circular buffer-based dataset that maintains a fixed-size window
  of the most recent data, enabling efficient time-series analysis and streaming
  computations on bounded memory."
  (:require [tech.v3.dataset :as ds]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [java-time.api :as java-time]))

(defrecord WindowedDataset [dataset column-types max-size current-size current-position])

(defn make-windowed-dataset
  "Create an empty `WindowedDataset` with a given `max-size`
  and given `column-types` (map).

  **Args:**
  - `column-types` - a map from column name to type
  - `max-size` - maximal window size to keep

  **Returns:**
  The specified `WindowedDataset` structure."
  [column-types max-size]
  (-> column-types
      (update-vals
       (fn [datatype]
         (dtype/make-container :jvm-heap
                               datatype
                               max-size)))
      tc/dataset
      (->WindowedDataset column-types max-size 0 0)))

(defn copy-windowed-dataset
  "Create a deep copy of a windowed dataset.
  
  **Args:**
  - `windowed-dataset` - a `WindowedDataset`
  
  **Returns:**
  New `WindowedDataset` with copied data"
  [{:as windowed-dataset :keys [dataset column-types max-size current-size current-position]}]
  (let [new-dataset (-> column-types
                        (update-vals
                         (fn [datatype]
                           (dtype/make-container :jvm-heap
                                                 datatype
                                                 max-size)))
                        tc/dataset)]
    ;; Copy existing data
    (doseq [[colname _] column-types
            i (range current-size)]
      (let [src-idx (if (< current-size max-size)
                      i
                      (rem (+ current-position i) max-size))
            dest-idx (if (< current-size max-size)
                       i
                       (rem (+ current-position i) max-size))]
        (dtype/set-value! (new-dataset colname)
                          dest-idx
                          (dtype/get-value (dataset colname) src-idx))))
    (->WindowedDataset new-dataset column-types max-size current-size current-position)))

(defn insert-to-windowed-dataset!
  "Insert a new row to a `WindowedDataset`.
  
  **Args:**
  - `windowed-dataset` - a `WindowedDataset`
  - `row` - A row represented as a map structure
  (can be a record or `FastStruct`, etc.)

  **Returns:**
  Updated windowed dataset with its data mutated(!)."
  [{:as windowed-dataset
    :keys [dataset column-types max-size current-position]}
   value]
  ;; Handle edge case: size-0 window does nothing
  (if (zero? max-size)
    windowed-dataset
    (let [;; Create a copy to avoid mutation issues with reductions
          copied-wd (copy-windowed-dataset windowed-dataset)]
      (doseq [[colname _] column-types]
        (dtype/set-value! ((:dataset copied-wd) colname)
                          current-position
                          (value colname)))
      ;; Create a new windowed dataset with the updated copy
      (->WindowedDataset (:dataset copied-wd)
                         column-types
                         max-size
                         (min (inc (:current-size windowed-dataset)) max-size)
                         (rem (inc current-position) max-size)))))

(defn windowed-dataset-indices
  "Extract the row indices for retrieving data from a windowed dataset in insertion order.
  
  This utility function encapsulates the logic for determining which rows to select
  from the underlying dataset to present data in the correct chronological order.
  
  **Args:**
  - `windowed-dataset` - a `WindowedDataset`
  
  **Returns:**
  Vector of integer indices in the correct order for data retrieval"
  [{:keys [max-size current-size current-position]}]
  (cond
    ;; Empty dataset
    (zero? current-size) []

    ;; Haven't wrapped around yet: select from 0 to current-size-1
    (< current-size max-size) (vec (range current-size))

    ;; Have wrapped around: select from current-position for max-size elements, wrapping
    :else (vec (map #(rem % max-size)
                    (range current-position (+ current-position max-size))))))

(defn windowed-dataset->dataset
  "Return a regular dataset as a view over the content of a windowed dataset.

  **Args:**
  - `windowed-dataset` - a `WindowedDataset`"
  [{:as windowed-dataset
    :keys [dataset]}]
  (let [indices (windowed-dataset-indices windowed-dataset)]
    (if (empty? indices)
      ;; Return empty dataset with same columns
      (ds/select-rows dataset [])
      (ds/select-rows dataset indices))))

(defn binary-search-timestamp-start
  "Binary search to find the first index where timestamp >= target-time.
  
  **Args:**
  - `timestamp-col` - the timestamp column from the dataset
  - `indices` - vector of indices in chronological order
  - `target-time` - the target timestamp to search for
  
  **Returns:**
  Index in the indices vector where the search should start"
  [timestamp-col indices target-time]
  (loop [left 0
         right (count indices)]
    (if (>= left right)
      left
      (let [mid (quot (+ left right) 2)
            mid-idx (nth indices mid)
            mid-time (nth timestamp-col mid-idx)]
        (if (java-time/before? mid-time target-time)
          (recur (inc mid) right)
          (recur left mid))))))

(defn windowed-dataset->time-window-dataset
  "Return a regular dataset as a view over the content of a windowed dataset,
  including only a recent time window. Uses binary search for optimal performance.

  **Args:**
  - `windowed-dataset` - a `WindowedDataset`
  - `timestamp-colname` - the name of the column that contains timestamps
  - `time-window` - window length in ms (from most recent timestamp backwards)

  **Returns:**
  Dataset containing only data within the specified time window
  
  **Performance:** O(log n) time complexity using binary search"
  [{:as windowed-dataset
    :keys [dataset]}
   timestamp-colname
   time-window]
  (let [indices (windowed-dataset-indices windowed-dataset)]
    (cond
      ;; Handle empty dataset
      (empty? indices)
      (ds/select-rows dataset [])

      ;; Handle invalid time window
      (or (nil? time-window) (neg? time-window))
      (ds/select-rows dataset [])

      ;; Handle zero time window - return only the most recent point
      (zero? time-window)
      (ds/select-rows dataset [(last indices)])

      ;; Normal case - use binary search for optimal performance
      :else
      (let [timestamp-col (dataset timestamp-colname)]
        ;; Check if timestamp column exists
        (when (nil? timestamp-col)
          (throw (IllegalArgumentException. (str "Timestamp column '" timestamp-colname "' not found in dataset"))))

        (let [;; Get the latest timestamp as reference point
              latest-idx (last indices)
              latest-time (nth timestamp-col latest-idx)
              ;; Calculate start time for the window
              start-time (java-time/minus latest-time (java-time/millis time-window))
              ;; Use binary search to find the first timestamp >= start-time
              start-pos (binary-search-timestamp-start timestamp-col indices start-time)
              ;; Take all indices from start position to end (they're already in chronological order)
              filtered-indices (subvec (vec indices) start-pos)]
          (ds/select-rows dataset filtered-indices))))))

(defn add-column-by-windowed-fn
  "Add a new column to a time-series by applying a windowed function progressively.
  
  This function simulates real-time streaming analysis on historical time-series data.
  For each row in the time-series (processed in timestamp order), it:

  1. Inserts the row into a growing windowed dataset
  2. Applies the windowed function to calculate a result  
  3. Uses that result as the column value for that row
  
  This bridges the gap between streaming windowed analysis and batch processing
  of existing time-series data, allowing you to see how metrics evolve over time
  as if the data were being processed in real-time.
  
  **Args:**
  - `time-series` - a tablecloth dataset with timestamp-ordered data
  - `options` - map with keys:
    - `:colname` - name of the new column to add
    - `:windowed-fn` - function that takes a WindowedDataset and returns a value
    - `:windowed-dataset-size` - size of the windowed dataset buffer (currently ignored, uses 120)
  
  **Returns:**
  The original time-series with the new column added, where each row contains
  the result of applying the windowed function to all data up to that timestamp
  
  **Use Cases:**
  - Adding progressive metrics to time-series
  - Creating trend analysis columns that consider historical context
  - Simulating real-time algorithm behavior on historical data
  - Generating training data with progressive features for ML models"
  [time-series {:keys [colname
                       windowed-fn
                       windowed-dataset-size]}]
  (let [initial-windowed-dataset (-> time-series
                                     (update-vals tcc/typeof)
                                     (make-windowed-dataset
                                      120))
        rows (-> time-series
                 (tc/order-by [:timestamp])
                 (tc/rows :as-maps))]
    (-> time-series
        (tc/add-column colname (->> rows
                                    (reductions
                                     (fn [[windowed-dataset _] row]
                                       (let [new-windowed-dataset
                                             (insert-to-windowed-dataset!
                                              windowed-dataset
                                              row)]
                                         [new-windowed-dataset
                                          (windowed-fn new-windowed-dataset)]))
                                     [initial-windowed-dataset nil])
                                    (map second))))))
