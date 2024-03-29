(uiop:define-package :async/test
  (:mix :async/async :fiveam :cl :iterate)
  (:export #:async-executor))
(in-package :async/test)

(def-suite async-executor)

(def-test one-job-one-thread (:suite async-executor)
  (is (eq :success
          (asynchronously (1)
            :success))))

(def-test await-a-job (:suite async-executor)
  (let* ((status :initial))
    (is (eq :final
            (asynchronously (1)
              (await (async ()
                       (setf status (if (eq status :initial)
                                        :second
                                        :failure))))
              (setf status (if (eq status :second)
                               :final
                               :failure)))))
    (is (eq status :final))))

(def-test multiple-concurrent-jobs (:suite async-executor)
  (is (= 27
         (asynchronously (2)
           (let* ((one (async ()
                          (sleep 1)
                          1))
                  (two (async ()
                         (+ 1 (await one))))
                  (twelve (async ()
                            (* 6 (await two))))
                  (three (async ()
                          (sleep 1)
                          3))
                  (fifteen (async ()
                             (* 5 (await three)))))
             (+ (await twelve) (await fifteen)))))))

(def-test job-seq-syntax (:suite async-executor)
  (is (= 3 (with-executor (1)
             (wait-for (job-seq (async () 1)
                                (lambda (one) (+ one 1))
                                (lambda (two) (* two two))
                                (lambda (four) (- four 1))))))))

(def-test multiple-concurrent-jobs-many-times (:suite async-executor)
  (with-executor (4)
    (labels ((make-job-sums-to-2^ (n)
               (if (= n 0)
                   (async () 1)
                   (let* ((left (make-job-sums-to-2^ (1- n)))
                          (right (make-job-sums-to-2^ (1- n))))
                     (async () (+ (await left) (await right)))))))
      (let* ((jobs (iter outer (for i from 0 to 8)
                     (collect (make-job-sums-to-2^ i)))))
        (iter (for job in jobs)
          (for i from 0 to 8)
          (is (= (expt 2 i) (wait-for job)))
          (finish-output *test-dribble*))))))
