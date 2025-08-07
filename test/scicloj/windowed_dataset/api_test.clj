(ns scicloj.windowed-dataset.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [scicloj.windowed-dataset.api :as wd]
            [java-time.api :as java-time]
            [tablecloth.api :as tc]))

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
