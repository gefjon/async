(uiop:define-package async/test
  (:mix async/async fiveam cl)
  (:export async-executor))
(in-package async/test)

(def-suite async-executor)

(def-test one-job-one-thread (:suite async-executor)
  (let* (queue)
    (unwind-protect
         (progn (setf queue (make-instance 'job-queue :nthreads 1))
                (let* (place
                       (job (async (queue)
                              (setf place :success))))
                  (is (eq :success (wait-for job))
                      "Job ~a returned ~a" job (async/job::return-values job))
                  (is (eq :success place))))
      (cancel-job-queue queue))))

(def-test await-a-job (:suite async-executor)
  (let* ((queue (make-instance 'job-queue :nthreads 1))
         (status :initial)
         (first-job (async (queue)
                      (setf status
                            (if (eq status :initial)
                                :second
                                :failure))))
         (second-job (async (queue)
                       (await first-job)
                       (setf status
                             (if (eq status :second)
                                 :final
                                 :failure)))))
    (is (eq :final (wait-for second-job)))
    (is (eq :final status))
    (cancel-job-queue queue)))

;; TODO: write more tests lmao
