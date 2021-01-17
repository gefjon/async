(defsystem "async"
  :version "0.0.1"
  :author "Phoebe Goldman <phoebe@goldman-tribe.org>"
  :class :package-inferred-system
  :depends-on (:async/async)
  :in-order-to ((test-op (test-op "async/test"))))

(defsystem "async/test"
  :defsystem-depends-on ("fiveam-asdf")
  :class :fiveam-tester-system
  :depends-on ("fiveam" "async")
  :components ((:file "test"))
  :test-package :async/test
  :test-names (:async-executor))
