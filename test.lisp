(uiop:define-package async/test
  (:mix async fiveam cl)
  (:import-from bordeaux-threads
                semaphore make-semaphore signal-semaphore wait-on-semaphore)
  (:export async-executor))
(in-package async/test)

(def-suite async-executor)

(def-test one-job-one-thread (:suite async-executor)
  (let* ((queue (make-executor 1))
         (test-done (make-semaphore))
         place)
    (async (queue)
      (setf place :success)
      (signal-semaphore test-done))
    (wait-on-semaphore test-done)
    (is (eq :success place))
    (cancel-job-queue queue)))

;; TODO: write more tests lmao
