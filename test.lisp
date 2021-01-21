(uiop:define-package async/test
  (:mix async/async fiveam cl)
  (:export async-executor))
(in-package async/test)

(def-suite async-executor)

(defmacro test-with-queue ((nthreads) &body body)
  `(let* ((async/job:*job-queue* (make-instance 'job-queue :nthreads ,nthreads)))
     (unwind-protect (progn ,@body)
       (cancel-job-queue async/job:*job-queue*))))

(def-test one-job-one-thread (:suite async-executor)
  (test-with-queue (1)
      (let* (place
             (job (async ()
                    (setf place :success))))
        (is (eq :success (wait-for job))
            "Job ~a returned ~a" job (async/job::return-values job))
        (is (eq :success place)))))

(def-test await-a-job (:suite async-executor)
  (test-with-queue (1)
    (let* ((status :initial)
           (first-job (async ()
                        (setf status
                              (if (eq status :initial)
                                  :second
                                  :failure))))
           (second-job (async ()
                         (await first-job)
                         (setf status
                               (if (eq status :second)
                                   :final
                                   :failure)))))
      (is (eq :final (wait-for second-job)))
      (is (eq :final status)))))

(def-test multiple-concurrent-jobs (:suite async-executor)
  (test-with-queue (2)
    (let* ((job1 (async ()
                   (sleep 1)
                   1))
           (job2 (async ()
                   (+ 1 (await job1))))
           (job3 (async ()
                   (* 6 (await job2))))
           (joba (async ()
                   (sleep 1)
                   3))
           (jobb (async ()
                   (* 5 (await joba))))
           (final-job (async ()
                        (+ (await job3) (await jobb)))))
      (is (= 27 (wait-for final-job))))))

;; TODO: write more tests lmao
