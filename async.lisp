(uiop:define-package async/async
  (:nicknames async)
  (:mix async/monitor async/queue async/job cl)
  (:import-from bordeaux-threads
                join-thread)
  (:import-from gefjon-utils
                typedec func void)
  (:import-from cl-cont
                with-call/cc let/cc)
  (:export
   job-queue job
   async await yield
   cancel-job-queue
   wait-for))
(in-package async/async)

(defmacro async ((&optional (job-queue '*job-queue*))
                 &body body)
  "Enqueue BODY as an asynchronous `job' into JOB-QUEUE.

Within BODY, the `await' and `yield' macros are available to delay computation."
  `(macrolet ((await (job)
                "Block the current job on JOB, causing the current job to not be executed again until JOB finishes, and returning JOB's return values.

May only occur within the lexical and dynamic extent of an `async' block."
                `(let/cc callback
                   (error 'await-condition
                          :upon ,job
                          :callback callback)))
              (yield ()
                "Pause the current job and place it at the end of the job queue.

May only occur within the lexical and dynamic extent of an `async' block."
                '(progn
                  (let/cc callback
                    (error 'yield-condition
                           :callback callback))
                  (values))))
     (make-instance 'job
                    :executor ,job-queue
                    :body (lambda ()
                            (with-call/cc
                              ,@body)))))

(defmacro await (job)
  "Block the current job on JOB, causing the current job to not be executed again until JOB finishes, and returning JOB's return values.

May only occur within the lexical and dynamic extent of an `async' block."
  (declare (ignore job))
  (error "`await' may only appear inside of an `async' block"))

(defmacro yield ()
  "Pause the current job and place it at the end of the job queue.

May only occur within the lexical and dynamic extent of an `async' block."
  (error "`yield' may only appear inside of an `async' block"))

(typedec #'job-queue-exit (func (job-queue) void))
(defun cancel-job-queue (job-queue)
  "Cancel any jobs remaining in JOB-QUEUE and join its worker threads."
  (queue-make-empty (jobs job-queue))
  (dotimes (n (nthreads job-queue))
    (async (job-queue)
      (error 'job-queue-exit)))
  (dolist (worker (workers job-queue))
    (join-thread worker))
  (values))

