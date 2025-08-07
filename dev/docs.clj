(ns docs
  (:require [scicloj.clay.v2.api :as clay]))

(clay/make! {:format [:quarto :html]
             :base-source-path "notebooks"
             :source-path ["index.clj"
                           "windowed_dataset_examples.clj"
                           "api_reference.clj"]
             :base-target-path "docs"
             :book {:title "Windowed dataset"}
             :clean-up-target-dir true})
