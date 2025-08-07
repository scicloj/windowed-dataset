^:kindly/hide-code
(ns index
  (:require [scicloj.kindly.v4.kind :as kind]
            [clojure.string :as str]))

^:kindly/hide-code
(->> "README.md"
     slurp
     str/split-lines
     (drop 2)
     (str/join "\n")
     kind/md)
