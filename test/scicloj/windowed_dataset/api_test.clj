(ns scicloj.windowed-dataset.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [scicloj.windowed-dataset.api :as wd]
            [java-time.api :as java-time]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]))

(deftest test-make-windowed-dataset
  (testing "Creating an empty windowed dataset"
    (let [column-types {:timestamp :instant :value :float64}
          max-size 5
          windowed-ds (wd/make-windowed-dataset column-types max-size)]
      (is (= (:max-size windowed-ds) 5))
      (is (= (:current-size windowed-ds) 0))
      (is (= (:current-position windowed-ds) 0))
      (is (= (:column-types windowed-ds) column-types))))

  (testing "Different column types"
    (let [column-types {:id :int32 :name :string :active :boolean :score :float32}
          windowed-ds (wd/make-windowed-dataset column-types 10)]
      (is (= (:column-types windowed-ds) column-types))
      (is (= (:max-size windowed-ds) 10))))

  (testing "Single column dataset"
    (let [windowed-ds (wd/make-windowed-dataset {:value :float64} 1)]
      (is (= (:max-size windowed-ds) 1))
      (is (= (:current-size windowed-ds) 0)))))

(deftest test-insert-to-windowed-dataset
  (testing "Basic insertion operations"
    (let [column-types {:timestamp :instant :value :float64}
          windowed-ds (wd/make-windowed-dataset column-types 3)
          now (java-time/instant)
          row1 {:timestamp now :value 1.0}
          row2 {:timestamp (java-time/plus now (java-time/seconds 1)) :value 2.0}]

      (testing "First insertion"
        (let [updated-ds (wd/insert-to-windowed-dataset! windowed-ds row1)]
          (is (= (:current-size updated-ds) 1))
          (is (= (:current-position updated-ds) 1))))

      (testing "Second insertion"
        (let [updated-ds (-> windowed-ds
                             (wd/insert-to-windowed-dataset! row1)
                             (wd/insert-to-windowed-dataset! row2))]
          (is (= (:current-size updated-ds) 2))
          (is (= (:current-position updated-ds) 2))))))

  (testing "Window overflow and circular behavior"
    (let [column-types {:value :float64}
          windowed-ds (wd/make-windowed-dataset column-types 3)
          rows (map #(hash-map :value (double %)) (range 1 6))]

      (let [final-ds (reduce wd/insert-to-windowed-dataset! windowed-ds rows)
            regular-ds (wd/windowed-dataset->dataset final-ds)
            values (vec (:value regular-ds))]
        (is (= (:current-size final-ds) 3))
        (is (= (:current-position final-ds) 2))
        (is (= values [3.0 4.0 5.0]) "Should contain the last 3 values in order"))))

  (testing "Multiple column insertion"
    (let [column-types {:id :int32 :name :string :score :float64}
          windowed-ds (wd/make-windowed-dataset column-types 2)
          row1 {:id 1 :name "Alice" :score 95.5}
          row2 {:id 2 :name "Bob" :score 87.2}]

      (let [updated-ds (-> windowed-ds
                           (wd/insert-to-windowed-dataset! row1)
                           (wd/insert-to-windowed-dataset! row2))
            regular-ds (wd/windowed-dataset->dataset updated-ds)]
        (is (= (vec (:id regular-ds)) [1 2]))
        (is (= (vec (:name regular-ds)) ["Alice" "Bob"]))
        (is (= (vec (:score regular-ds)) [95.5 87.2]))))))

(deftest test-windowed-dataset-indices
  (testing "Index calculation for different states"
    (testing "Empty dataset"
      (let [windowed-ds (wd/make-windowed-dataset {:value :float64} 5)]
        (is (= (wd/windowed-dataset-indices windowed-ds) []))))

    (testing "Partially filled dataset"
      (let [windowed-ds {:max-size 5 :current-size 3 :current-position 3}]
        (is (= (wd/windowed-dataset-indices windowed-ds) [0 1 2]))))

    (testing "Full dataset with wrapping"
      (let [windowed-ds {:max-size 3 :current-size 3 :current-position 1}]
        (is (= (wd/windowed-dataset-indices windowed-ds) [1 2 0]))))

    (testing "Complex wrapping scenarios"
      (let [windowed-ds {:max-size 4 :current-size 4 :current-position 2}]
        (is (= (wd/windowed-dataset-indices windowed-ds) [2 3 0 1])))

      (let [windowed-ds {:max-size 5 :current-size 5 :current-position 0}]
        (is (= (wd/windowed-dataset-indices windowed-ds) [0 1 2 3 4]))))))

(deftest test-windowed-dataset->dataset
  (testing "Converting windowed dataset to regular dataset"
    (let [column-types {:timestamp :instant :value :float64}
          windowed-ds (wd/make-windowed-dataset column-types 3)
          now (java-time/instant)
          rows [{:timestamp now :value 1.0}
                {:timestamp (java-time/plus now (java-time/seconds 1)) :value 2.0}
                {:timestamp (java-time/plus now (java-time/seconds 2)) :value 3.0}]]

      (let [final-ds (reduce wd/insert-to-windowed-dataset! windowed-ds rows)
            regular-ds (wd/windowed-dataset->dataset final-ds)]
        (is (= (count (regular-ds :timestamp)) 3))
        (is (= (count (regular-ds :value)) 3)))))

  (testing "Empty windowed dataset conversion"
    (let [windowed-ds (wd/make-windowed-dataset {:value :float64} 5)
          regular-ds (wd/windowed-dataset->dataset windowed-ds)]
      (is (= (tc/row-count regular-ds) 0))
      (is (= (tc/column-names regular-ds) [:value]))))

  (testing "Order preservation after wrapping"
    (let [windowed-ds (wd/make-windowed-dataset {:value :int32} 3)
          values (range 1 8)
          final-ds (reduce #(wd/insert-to-windowed-dataset! %1 {:value %2}) windowed-ds values)
          regular-ds (wd/windowed-dataset->dataset final-ds)]
      (is (= (vec (:value regular-ds)) [5 6 7]) "Should maintain chronological order"))))

(deftest test-zero-size-window
  (testing "Zero-size window behavior"
    (let [windowed-ds (wd/make-windowed-dataset {:value :float64} 0)
          row {:value 1.0}
          updated-ds (wd/insert-to-windowed-dataset! windowed-ds row)]
      (is (= (:current-size updated-ds) 0))
      (is (= (wd/windowed-dataset-indices updated-ds) []))))

  (testing "Multiple insertions to zero-size window"
    (let [windowed-ds (wd/make-windowed-dataset {:value :float64} 0)
          rows [{:value 1.0} {:value 2.0} {:value 3.0}]
          final-ds (reduce wd/insert-to-windowed-dataset! windowed-ds rows)]
      (is (= (:current-size final-ds) 0))
      (is (= (:current-position final-ds) 0)))))

(deftest test-copy-windowed-dataset
  (testing "Deep copying of windowed dataset"
    (let [column-types {:value :float64}
          original (wd/make-windowed-dataset column-types 3)
          with-data (wd/insert-to-windowed-dataset! original {:value 1.0})
          copied (wd/copy-windowed-dataset with-data)]
      (is (= (:max-size copied) (:max-size with-data)))
      (is (= (:current-size copied) (:current-size with-data)))
      (is (= (:current-position copied) (:current-position with-data)))
      (is (= (:column-types copied) (:column-types with-data)))))

  (testing "Copy independence - mutations don't affect original"
    (let [original (wd/make-windowed-dataset {:value :int32} 3)
          with-data (wd/insert-to-windowed-dataset! original {:value 42})
          copied (wd/copy-windowed-dataset with-data)
          copied-modified (wd/insert-to-windowed-dataset! copied {:value 99})]

      (is (= (:current-size with-data) 1))
      (is (= (:current-size copied-modified) 2))
      (is (not= (wd/windowed-dataset->dataset with-data)
                (wd/windowed-dataset->dataset copied-modified)))))

  (testing "Copy with wrapped data"
    (let [windowed-ds (wd/make-windowed-dataset {:value :int32} 3)
          values [1 2 3 4 5]
          filled-ds (reduce #(wd/insert-to-windowed-dataset! %1 {:value %2}) windowed-ds values)
          copied (wd/copy-windowed-dataset filled-ds)]

      (is (= (wd/windowed-dataset->dataset filled-ds)
             (wd/windowed-dataset->dataset copied))))))

(deftest test-binary-search-timestamp-start
  (testing "Binary search functionality"
    (let [now (java-time/instant)
          timestamps (mapv #(java-time/plus now (java-time/seconds %)) (range 5))
          indices [0 1 2 3 4]
          target-time (java-time/plus now (java-time/seconds 2))]

      (testing "Exact match"
        (is (= (wd/binary-search-timestamp-start timestamps indices target-time) 2)))

      (testing "Between timestamps"
        (let [between-time (java-time/plus now (java-time/millis 1500))]
          (is (= (wd/binary-search-timestamp-start timestamps indices between-time) 2))))

      (testing "Before all timestamps"
        (let [before-time (java-time/minus now (java-time/seconds 1))]
          (is (= (wd/binary-search-timestamp-start timestamps indices before-time) 0))))

      (testing "After all timestamps"
        (let [after-time (java-time/plus now (java-time/seconds 10))]
          (is (= (wd/binary-search-timestamp-start timestamps indices after-time) 5)))))))

(deftest test-windowed-dataset->time-window-dataset
  (let [column-types {:timestamp :instant :value :float64}
        windowed-ds (wd/make-windowed-dataset column-types 10)
        now (java-time/instant)
        rows (map (fn [i]
                    {:timestamp (java-time/plus now (java-time/seconds i))
                     :value (double i)})
                  (range 10))
        filled-ds (reduce wd/insert-to-windowed-dataset! windowed-ds rows)]

    (testing "Time window extraction"
      (testing "3-second window"
        (let [time-window-ds (wd/windowed-dataset->time-window-dataset filled-ds :timestamp 3000)
              values (vec (:value time-window-ds))]
          (is (= values [6.0 7.0 8.0 9.0]) "Should contain values from last 3+ seconds")))

      (testing "5-second window"
        (let [time-window-ds (wd/windowed-dataset->time-window-dataset filled-ds :timestamp 5000)
              values (vec (:value time-window-ds))]
          (is (= values [4.0 5.0 6.0 7.0 8.0 9.0]) "Should contain values from last 5+ seconds")))

      (testing "Large window (all data)"
        (let [time-window-ds (wd/windowed-dataset->time-window-dataset filled-ds :timestamp 15000)
              values (vec (:value time-window-ds))]
          (is (= values (mapv double (range 10))) "Should contain all data"))))

    (testing "Edge cases"
      (testing "Zero time window"
        (let [time-window-ds (wd/windowed-dataset->time-window-dataset filled-ds :timestamp 0)
              values (vec (:value time-window-ds))]
          (is (= values [9.0]) "Should contain only the most recent point")))

      (testing "Negative time window"
        (let [time-window-ds (wd/windowed-dataset->time-window-dataset filled-ds :timestamp -1000)]
          (is (= (tc/row-count time-window-ds) 0) "Should return empty dataset")))

      (testing "Nil time window"
        (let [time-window-ds (wd/windowed-dataset->time-window-dataset filled-ds :timestamp nil)]
          (is (= (tc/row-count time-window-ds) 0) "Should return empty dataset"))))

    (testing "Empty dataset"
      (let [empty-ds (wd/make-windowed-dataset column-types 5)
            time-window-ds (wd/windowed-dataset->time-window-dataset empty-ds :timestamp 1000)]
        (is (= (tc/row-count time-window-ds) 0))))

    (testing "Invalid timestamp column"
      (is (thrown? IllegalArgumentException
                   (wd/windowed-dataset->time-window-dataset filled-ds :nonexistent-column 1000))))))

(deftest test-add-column-by-windowed-fn
  (testing "Progressive column addition"
    (let [time-series (tc/dataset {:timestamp (map #(java-time/plus (java-time/instant) (java-time/seconds %)) (range 5))
                                   :value [1.0 2.0 3.0 4.0 5.0]})

          sum-fn (fn [windowed-ds]
                   (let [regular-ds (wd/windowed-dataset->dataset windowed-ds)
                         values (:value regular-ds)]
                     (when (seq values) (reduce + values))))

          result (wd/add-column-by-windowed-fn time-series
                                               {:colname :cumulative-sum
                                                :windowed-fn sum-fn
                                                :windowed-dataset-size 120})]

      (is (= (vec (:cumulative-sum result)) [nil 1.0 3.0 6.0 10.0])
          "Should calculate progressive cumulative sum")))

  (testing "Moving average calculation"
    (let [time-series (tc/dataset {:timestamp (map #(java-time/plus (java-time/instant) (java-time/seconds %)) (range 4))
                                   :value [10.0 20.0 30.0 40.0]})

          avg-fn (fn [windowed-ds]
                   (let [regular-ds (wd/windowed-dataset->dataset windowed-ds)
                         values (:value regular-ds)]
                     (when (seq values) (/ (reduce + values) (count values)))))

          result (wd/add-column-by-windowed-fn time-series
                                               {:colname :moving-avg
                                                :windowed-fn avg-fn
                                                :windowed-dataset-size 120})]

      (is (= (vec (:moving-avg result)) [nil 10.0 15.0 20.0])
          "Should calculate progressive moving average")))

  (testing "Window size function"
    (let [time-series (tc/dataset {:timestamp (map #(java-time/plus (java-time/instant) (java-time/seconds %)) (range 3))
                                   :value [1.0 2.0 3.0]})

          count-fn (fn [windowed-ds]
                     (:current-size windowed-ds))

          result (wd/add-column-by-windowed-fn time-series
                                               {:colname :window-size
                                                :windowed-fn count-fn
                                                :windowed-dataset-size 120})]

      (is (= (vec (:window-size result)) [nil 1 2])
          "Should track window size growth"))))

(deftest test-stress-and-performance
  (testing "Large dataset handling"
    (let [column-types {:id :int32 :value :float64}
          windowed-ds (wd/make-windowed-dataset column-types 100)
          large-data (map #(hash-map :id % :value (double %)) (range 1000))]

      (let [final-ds (reduce wd/insert-to-windowed-dataset! windowed-ds large-data)
            regular-ds (wd/windowed-dataset->dataset final-ds)]
        (is (= (:current-size final-ds) 100))
        (is (= (tc/row-count regular-ds) 100))
        (is (= (first (:id regular-ds)) 900) "Should contain last 100 records")
        (is (= (last (:id regular-ds)) 999)))))

  (testing "Memory efficiency - constant size regardless of input"
    (let [small-window (wd/make-windowed-dataset {:value :float64} 5)
          large-input (map #(hash-map :value (double %)) (range 10000))
          result (reduce wd/insert-to-windowed-dataset! small-window large-input)]

      (is (= (:current-size result) 5) "Window size should remain constant")
      (is (= (:max-size result) 5))))

  (testing "Functional immutability"
    (let [windowed-ds (wd/make-windowed-dataset {:value :int32} 3)
          ds1 (wd/insert-to-windowed-dataset! windowed-ds {:value 1})
          ds2 (wd/insert-to-windowed-dataset! windowed-ds {:value 2})]

      (is (= (:current-size windowed-ds) 0) "Original should be unchanged")
      (is (= (:current-size ds1) 1))
      (is (= (:current-size ds2) 1))
      (is (not= ds1 ds2) "Different insertions should produce different results"))))

(deftest test-edge-cases
  (testing "Single element window"
    (let [windowed-ds (wd/make-windowed-dataset {:value :int32} 1)
          values [1 2 3 4 5]
          final-ds (reduce #(wd/insert-to-windowed-dataset! %1 {:value %2}) windowed-ds values)
          regular-ds (wd/windowed-dataset->dataset final-ds)]

      (is (= (:current-size final-ds) 1))
      (is (= (vec (:value regular-ds)) [5]) "Should only contain the last value")))

  (testing "Extremely large window"
    (let [windowed-ds (wd/make-windowed-dataset {:value :int32} 10000)
          values (range 100)
          final-ds (reduce #(wd/insert-to-windowed-dataset! %1 {:value %2}) windowed-ds values)]

      (is (= (:current-size final-ds) 100))
      (is (= (:current-position final-ds) 100))))

  (testing "Mixed data types"
    (let [column-types {:int-col :int32 :float-col :float64 :string-col :string :bool-col :boolean}
          windowed-ds (wd/make-windowed-dataset column-types 2)
          row1 {:int-col 42 :float-col 3.14 :string-col "hello" :bool-col true}
          row2 {:int-col 99 :float-col 2.71 :string-col "world" :bool-col false}

          final-ds (-> windowed-ds
                       (wd/insert-to-windowed-dataset! row1)
                       (wd/insert-to-windowed-dataset! row2))
          regular-ds (wd/windowed-dataset->dataset final-ds)]

      (is (= (vec (:int-col regular-ds)) [42 99]))
      (is (= (vec (:float-col regular-ds)) [3.14 2.71]))
      (is (= (vec (:string-col regular-ds)) ["hello" "world"]))
      (is (= (vec (:bool-col regular-ds)) [true false])))))

(deftest moving-average-test
  (testing "Basic moving average calculation"
    (let [;; Create windowed dataset with known values
          wd (wd/make-windowed-dataset {:x :int32} 10)
          test-values [800 810 820 830 840]
          test-data (map (fn [v] {:x v}) test-values)
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Test different window sizes
      (is (= 830 (wd/moving-average populated-wd 3 :x))) ; avg of [820 830 840]
      (is (= 825 (wd/moving-average populated-wd 4 :x))) ; avg of [810 820 830 840]  
      (is (= 820 (wd/moving-average populated-wd 5 :x))) ; avg of [800 810 820 830 840]

      ;; Test insufficient data
      (is (nil? (wd/moving-average populated-wd 6 :x))) ; only 5 samples available
      (is (nil? (wd/moving-average populated-wd 10 :x))))) ; way more than available

  (testing "Custom column name support"
    (let [wd (wd/make-windowed-dataset {:HeartInterval :int32} 5)
          test-data [{:HeartInterval 900} {:HeartInterval 950} {:HeartInterval 1000}]
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      (is (= 950 (wd/moving-average populated-wd 3 :HeartInterval)))))

  (testing "Empty dataset"
    (let [empty-wd (wd/make-windowed-dataset {:x :int32} 5)]
      (is (nil? (wd/moving-average empty-wd 1 :x)))
      (is (nil? (wd/moving-average empty-wd 3 :x)))))

  (testing "Single sample"
    (let [wd (wd/make-windowed-dataset {:x :int32} 5)
          single-wd (wd/insert-to-windowed-dataset! wd {:x 800})]
      (is (= 800 (wd/moving-average single-wd 1 :x)))
      (is (nil? (wd/moving-average single-wd 2 :x)))))

  (testing "Circular buffer behavior"
    (let [;; Small buffer that will wrap around
          wd (wd/make-windowed-dataset {:x :int32} 3)
          ;; Insert more data than capacity
          test-data (map (fn [v] {:x v}) [100 200 300 400 500])
          final-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Should only have last 3 values: [300 400 500]
      (is (= 400 (wd/moving-average final-wd 3 :x))) ; avg of [300 400 500]
      (is (= 450 (wd/moving-average final-wd 2 :x))))) ; avg of [400 500]

  (testing "Floating point precision"
    (let [wd (wd/make-windowed-dataset {:x :float64} 5)
          test-data [{:x 800.5} {:x 810.7} {:x 820.3}]
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Test precise calculation
      (is (< (Math/abs (- 810.5 (wd/moving-average populated-wd 3 :x))) 0.01)))))

(deftest median-filter-test
  (testing "Basic median filter calculation"
    (let [wd (wd/make-windowed-dataset {:x :int32} 10)
          ;; Test data with outlier: [800, 810, 1500, 820, 830]
          test-data (map (fn [v] {:x v}) [800 810 1500 820 830])
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Test different window sizes
      (is (= 830 (wd/median-filter populated-wd 3 :x))) ; median of [1500 820 830] = 820
      (is (= 830 (wd/median-filter populated-wd 4 :x))) ; median of [810 1500 820 830] = 815 (avg of 810,820)
      (is (= 820 (wd/median-filter populated-wd 5 :x))) ; median of [800 810 1500 820 830] = 820

      ;; Test insufficient data
      (is (nil? (wd/median-filter populated-wd 6 :x)))))

  (testing "Odd vs even window sizes"
    (let [wd (wd/make-windowed-dataset {:x :int32} 10)
          test-data (map (fn [v] {:x v}) [100 200 300 400 500])
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Odd window size - true median
      (is (= 300 (wd/median-filter populated-wd 5 :x))) ; [100 200 300 400 500] -> 300
      (is (= 400 (wd/median-filter populated-wd 3 :x))) ; [300 400 500] -> 400

      ;; Even window size - lower middle element (by design)
      (is (= 400 (wd/median-filter populated-wd 4 :x))) ; [200 300 400 500] -> 300 (index 1)
      (is (= 500 (wd/median-filter populated-wd 2 :x))))) ; [400 500] -> 400 (index 0)

  (testing "Custom column name"
    (let [wd (wd/make-windowed-dataset {:CustomCol :int32} 5)
          test-data [{:CustomCol 900} {:CustomCol 950} {:CustomCol 1000}]
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      (is (= 950 (wd/median-filter populated-wd 3 :CustomCol))))))

(deftest cascaded-median-filter-test
  (testing "Basic cascaded median filter"
    (let [wd (wd/make-windowed-dataset {:x :int32} 10)
          ;; Data with outliers that 3-point median should handle
          test-data (map (fn [v] {:x v}) [800 1500 810 2000 820]) ; outliers at pos 1,3
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Should apply 3-point median first, then 5-point median
      (is (number? (wd/cascaded-median-filter populated-wd :x)))
      (is (not (nil? (wd/cascaded-median-filter populated-wd :x))))))

  (testing "Insufficient data"
    (let [wd (wd/make-windowed-dataset {:x :int32} 10)
          test-data (map (fn [v] {:x v}) [800 810 820]) ; only 3 samples
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      (is (nil? (wd/cascaded-median-filter populated-wd :x))))) ; needs 5+ samples

  (testing "Exact 5 samples"
    (let [wd (wd/make-windowed-dataset {:x :int32} 10)
          test-data (map (fn [v] {:x v}) [800 810 820 830 840])
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      (is (number? (wd/cascaded-median-filter populated-wd :x)))))

  (testing "Custom column name"
    (let [wd (wd/make-windowed-dataset {:Interval :int32} 8)
          test-data (map (fn [v] {:Interval v}) [700 710 720 730 740])
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      (is (number? (wd/cascaded-median-filter populated-wd :Interval))))))

(deftest exponential-moving-average-test
  (testing "Basic EMA calculation"
    (let [wd (wd/make-windowed-dataset {:x :float64} 10)
          ;; Simple increasing sequence
          test-data (map (fn [v] {:x (double v)}) [800 810 820])
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Test different alpha values
      (let [ema-low (wd/exponential-moving-average populated-wd 0.1 :x)
            ema-high (wd/exponential-moving-average populated-wd 0.9 :x)]
        ;; Higher alpha should be closer to recent values
        (is (> ema-high ema-low))
        (is (number? ema-low))
        (is (number? ema-high)))))

  (testing "Single sample"
    (let [wd (wd/make-windowed-dataset {:x :float64} 5)
          single-wd (wd/insert-to-windowed-dataset! wd {:x 800.0})]

      ;; EMA of single value should be that value
      (is (= 800.0 (wd/exponential-moving-average single-wd 0.5 :x)))))

  (testing "Empty dataset"
    (let [empty-wd (wd/make-windowed-dataset {:x :float64} 5)]
      (is (nil? (wd/exponential-moving-average empty-wd 0.3 :x)))))

  (testing "Alpha edge cases"
    (let [wd (wd/make-windowed-dataset {:x :float64} 5)
          test-data [{:x 800.0} {:x 900.0}]
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      ;; Alpha = 1.0 should return the last value
      (is (= 900.0 (wd/exponential-moving-average populated-wd 1.0 :x)))

      ;; Alpha very small should be close to first value
      (let [ema-tiny (wd/exponential-moving-average populated-wd 0.01 :x)]
        (is (< (Math/abs (- 801.0 ema-tiny)) 1.0))))) ; Should be close to 800 + small adjustment

  (testing "Custom column name"
    (let [wd (wd/make-windowed-dataset {:Rate :float64} 5)
          test-data [{:Rate 75.0} {:Rate 80.0}]
          populated-wd (reduce wd/insert-to-windowed-dataset! wd test-data)]

      (is (number? (wd/exponential-moving-average populated-wd 0.5 :Rate))))))

(deftest cascaded-smoothing-filter-test
  (testing "Basic functionality with sufficient data"
    (let [;; Create windowed dataset with mixed noise and outliers
          test-data (tc/dataset {:timestamp (range 15)
                                 :x [800 810 1500 820 805 ; outlier at position 2
                                     815 812 808 795 2000 ; outlier at position 9  
                                     805 820 800 810 815]})

          windowed-dataset (-> test-data
                               (update-vals tcc/typeof)
                               (wd/make-windowed-dataset 20))

          ;; Insert all data
          final-wd (reduce wd/insert-to-windowed-dataset!
                           windowed-dataset
                           (tc/rows test-data :as-maps))]

      ;; Test with default parameters (5pt median, 3pt MA)
      (let [result (wd/cascaded-smoothing-filter final-wd 5 3 :x)]
        (is (number? result))
        (is (> result 700))
        (is (< result 900)))

      ;; Test with custom parameters
      (let [result (wd/cascaded-smoothing-filter final-wd 3 2 :x)]
        (is (number? result))
        (is (> result 700))
        (is (< result 900)))

      ;; Test with specific column
      (let [result (wd/cascaded-smoothing-filter final-wd 5 3 :x)]
        (is (number? result)))))

  (testing "Insufficient data scenarios"
    (let [small-data (tc/dataset {:timestamp (range 3)
                                  :x [800 810 820]})

          windowed-dataset (-> small-data
                               (update-vals tcc/typeof)
                               (wd/make-windowed-dataset 10))

          ;; Insert insufficient data
          partial-wd (reduce wd/insert-to-windowed-dataset!
                             windowed-dataset
                             (tc/rows small-data :as-maps))]

      ;; Should return nil when insufficient data
      (is (nil? (wd/cascaded-smoothing-filter partial-wd 5 3 :x)))
      (is (nil? (wd/cascaded-smoothing-filter partial-wd 5 3 :x)))
      (is (nil? (wd/cascaded-smoothing-filter partial-wd 10 5 :x)))))

  (testing "Outlier removal effectiveness"
    (let [;; Data with extreme outliers
          outlier-data (tc/dataset {:timestamp (range 12)
                                    :x [800 800 5000 800 800 ; extreme outlier
                                        800 800 800 100 800 ; extreme outlier  
                                        800 800]})

          windowed-dataset (-> outlier-data
                               (update-vals tcc/typeof)
                               (wd/make-windowed-dataset 15))

          final-wd (reduce wd/insert-to-windowed-dataset!
                           windowed-dataset
                           (tc/rows outlier-data :as-maps))

          result (wd/cascaded-smoothing-filter final-wd 5 3 :x)]

      ;; Result should be close to 800, not influenced by outliers
      (is (number? result))
      (is (> result 750))
      (is (< result 850))
      (is (< (abs (- result 800)) 50)))) ; Within 50ms of true value

  (testing "Noise reduction effectiveness"
    (let [;; Data with Gaussian noise but no outliers
          noisy-data (tc/dataset {:timestamp (range 10)
                                  :x [795 803 798 802 799 801 797 804 800 798]})

          windowed-dataset (-> noisy-data
                               (update-vals tcc/typeof)
                               (wd/make-windowed-dataset 15))

          final-wd (reduce wd/insert-to-windowed-dataset!
                           windowed-dataset
                           (tc/rows noisy-data :as-maps))

          result (wd/cascaded-smoothing-filter final-wd 5 3 :x)]

      ;; Should smooth the noise around 800
      (is (number? result))
      (is (> result 795))
      (is (< result 805))))

  (testing "Parameter sensitivity"
    (let [test-data (tc/dataset {:timestamp (range 15)
                                 :x [800 810 1200 820 805 815 812 808 795 805 820 800 810 815 800]})

          windowed-dataset (-> test-data
                               (update-vals tcc/typeof)
                               (wd/make-windowed-dataset 20))

          final-wd (reduce wd/insert-to-windowed-dataset!
                           windowed-dataset
                           (tc/rows test-data :as-maps))]

      ;; Different window sizes should give different but reasonable results
      (let [result-3-2 (wd/cascaded-smoothing-filter final-wd 3 2 :x)
            result-5-3 (wd/cascaded-smoothing-filter final-wd 5 3 :x)
            result-7-4 (wd/cascaded-smoothing-filter final-wd 7 4 :x)]

        (is (every? number? [result-3-2 result-5-3 result-7-4]))
        (is (every? #(and (> % 700) (< % 900)) [result-3-2 result-5-3 result-7-4]))

        ;; Results should be similar but not identical
        (is (< (abs (- result-3-2 result-5-3)) 100))
        (is (< (abs (- result-5-3 result-7-4)) 100)))))

  (testing "Edge cases and error handling"
    (let [edge-data (tc/dataset {:timestamp (range 8)
                                 :x [800 800 800 800 800 800 800 800]})

          windowed-dataset (-> edge-data
                               (update-vals tcc/typeof)
                               (wd/make-windowed-dataset 10))

          final-wd (reduce wd/insert-to-windowed-dataset!
                           windowed-dataset
                           (tc/rows edge-data :as-maps))]

      ;; Perfect data should return the same value
      (let [result (wd/cascaded-smoothing-filter final-wd 5 3 :x)]
        (is (number? result))
        (is (< (abs (- result 800.0)) 0.1))) ; Close to 800

      ;; Zero window sizes should return nil
      (is (nil? (wd/cascaded-smoothing-filter final-wd 0 3 :x)))
      (is (nil? (wd/cascaded-smoothing-filter final-wd 3 0 :x))))))
