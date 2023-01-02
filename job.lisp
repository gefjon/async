(uiop:define-package :async/job
  (:mix :cl :iterate)
  (:import-from :sb-concurrency
                #:queue #:enqueue #:dequeue #:make-queue #:queue-empty-p)
  (:import-from :gefjon-utils
                #:define-class #:define-special
                #:typedec #:func #:void #:list-of
                #:with-slot-accessors)
  (:import-from :alexandria
                #:when-let #:with-gensyms #:once-only)
  (:import-from :sb-thread
                #:thread #:make-thread #:join-thread #:thread-yield #:barrier)
  (:import-from :sb-ext
                #:atomic-push #:compare-and-swap)
  (:export
   #:job-queue #:make-job-queue
   #:job #:make-job
   #:*job-queue*

   #:await-condition #:yield-condition

   #:cancel-job-queue

   #:add-job #:wait-for #:job-seq))
(in-package :async/job)

;;;; Invariants required by this file:
;; - Once a job's status is set to :DONE, it will never be changed, and its JOB-RETURN-VALUES will never be
;;   altered. That is, observing a JOB-STATUS of :DONE means it is safe to read JOB-RETURN-VALUES without
;;   synchronization.
;; - Jobs don't ever move between executors. A job can never await a job from a different executor.

;;; classes

(defstruct (job-queue (:constructor %make-job-queue (nthreads)))
  "A queue of `job's associated with a set of worker threads."
  (jobs (make-queue) :type queue :read-only t)
  (workers nil :type (list-of thread))
  (nthreads (error) :type (and fixnum unsigned-byte (not (eql 0))) :read-only t))

(defstruct (job (:constructor %make-job (executor body)))
  "An asynchronous task to be executed by a worker thread in a `job-queue'."
  (executor (error) :type job-queue :read-only t)
  (status :uninit :type (member :uninit :running :blocked :sleeping :done))

  ;; After this job finishes, a list of its return values as by `multiple-value-list'.
  ;;
  ;; Will be passed as arguments to jobs which are `awaiting' this job, that is, jobs in this job's `awaiters'
  ;; queue.
  (return-values nil :type list)

  ;; Backpointers to jobs which are `awaiting' this job.
  ;;
  ;; Maintained so that we can re-ready and enqueue those jobs when this job finishes.
  (awaiters nil :type (list-of job))
  ;; A job upon which this job is waiting, or nil if this job is ready to proceed.
  (awaiting nil :type (or null job))

  ;; A function which when run will carry out this job.
  ;;
  ;; If BODY returns normally, this job will be marked as `:done', and its return values stored in
  ;; `return-values'.
  ;;
  ;; If BODY signals `await-condition' or `yield-condition', the job will be set in that state,
  ;; and its BODY replaced with that condition's `callback'.
  ;;
  ;; If `awaiting' is set to a non-nil `job', BODY will be invoked with that job's `return-values' as arguments.
  (body (error) :type function))

;;; special vars

(define-special *job-queue* job-queue
    "The current asynchronous executor environment.

Will be specially bound within each worker thread when they are spawned. Should be globally unbound.")

;;; non-local escapes from job bodies

(define-class wait-condition
    ((callback function))
  :condition t)

(define-class await-condition
    ((upon job))
  :condition t
  :superclasses (wait-condition))

(define-class yield-condition
    ()
  :condition t
  :superclasses (wait-condition))

(define-class job-queue-exit ()
  :condition t
  :documentation "Thrown within a worker thread when the owning `job-queue' has been cancelled.")

;;; job queue operations

(typedec #'add-job (func (job) void))
(defun add-job (job &aux (job-queue (job-executor job)))
  "Insert JOB into its EXECUTOR, to be run whenever a worker thread is available."
  (barrier (:memory)
    (setf (job-status job) :sleeping))
  (enqueue job (job-queue-jobs job-queue))
  (values))

(typedec #'get-job (func (&optional job-queue) job))
(defun get-job (&optional (job-queue *job-queue*)
                &aux (jobs (job-queue-jobs job-queue)))
  "Return the next `job' from JOB-QUEUE, blocking if none are available yet."
  (iter
    (for (values next-job presentp) = (dequeue jobs))
    (until presentp)
    (thread-yield)
    (finally (return next-job))))

(typedec #'make-job (func (&key (:awaiting (or null job)) (:executor job-queue) (:body function)) job))
(defun make-job (&key awaiting (executor *job-queue*) body)
  (let* ((job (%make-job executor body)))
    (if awaiting
        (job-make-awaiting job awaiting)
        (add-job job))
    job))

(typedec #'job-make-awaiting (func (job job) void))
(defun job-make-awaiting (waiter awaiting)
  "Make WAITER be awaiting AWAITING, so that it runs when AWAITING finishes and takes AWAITER's return-values as arguments.

WAITER must be in the state :INCONSISTENT"
  (if (barrier (:memory)
        (eq (job-status awaiting) :uninit))
      ;; loop until the job is initialized
      (job-make-awaiting waiter awaiting)
      (progn
        (barrier (:memory)
          (setf (job-awaiting waiter) awaiting))
        (atomic-push waiter (job-awaiters awaiting))
        (ecase (barrier (:memory)
                 (job-status awaiting))
          ((:blocked :sleeping :running)
           (barrier (:memory)
             (setf (job-status waiter) :blocked)))
          ((:done)
           (add-job waiter)))))
  (values))

(typedec #'cancel-job-queue (func (&optional job-queue) void))
(defun cancel-job-queue (&optional (job-queue *job-queue*))
  "Cancel any jobs remaining in JOB-QUEUE and join its worker threads."
  (iter (until (queue-empty-p (job-queue-jobs job-queue)))
    (dequeue (job-queue-jobs job-queue)))
  (dotimes (n (job-queue-nthreads job-queue))
    (make-job :executor job-queue :body (lambda () (error 'job-queue-exit))))
  (dolist (worker (job-queue-workers job-queue))
    (join-thread worker))
  (values))

;;; job queue construction and worker threads

(typedec #'worker-loop (func (job-queue) void))
(defun worker-loop (queue)
  (let* ((*job-queue* queue))
    (handler-case (iter (for job = (get-job queue))
                    (run-job job))
      (job-queue-exit () (return-from worker-loop (values))))))

(typedec #'make-job-queue (func ((and fixnum unsigned-byte (not (eql 0)))) job-queue))
(defun make-job-queue  (nthreads &aux (job-queue (%make-job-queue nthreads)))
  (flet ((worker-body ()
           (worker-loop job-queue)))
    (setf (job-queue-workers job-queue)
          (iter (declare (declare-variables))
            (for (the fixnum i) below nthreads)
            (collect (make-thread #'worker-body
                                  :name (format nil "EXECUTOR-THREAD-~d" i))))))
  job-queue)

;;; job life cycle

(typedec #'job-done (func (job &rest t) void))
(defun job-done (finished-job &rest ret-vals)
  (barrier (:memory)
    (setf (job-return-values finished-job) ret-vals))
  (let* ((jobs-to-run (barrier (:memory)
                        (job-awaiters finished-job))))
    (barrier (:memory)
      (setf (job-status finished-job) :done))
    (dolist (blocked-job jobs-to-run)
      (assert (eq (job-status blocked-job) :blocked))
      (assert (eq (job-awaiting blocked-job) finished-job))
      (add-job blocked-job)))
  (values))

(typedec #'get-return-values (func (job) list))
(defun get-return-values (job)
  (iter (for status = (barrier (:memory)
                        (job-status job)))
    (ecase status
      ((:inconsistent) (next-iteration))
      ((:done) (return (job-return-values job)))
      ((:sleeping :blocked :running) (error "Attempt to get return values of an unfinished job")))))

(typedec #'invoke-job (func (job) (values &rest t)))
(defun invoke-job (job)
  (let* ((awaiting (job-awaiting job))
         (body (job-body job)))
    (apply body
           (if awaiting
               ;; if JOB was `awaiting' another `job', invoke BODY with its `return-values'.
               (get-return-values awaiting)
               ;; if not, invoke JOB with no arguments.
               nil))))

(typedec #'run-job (func (job) void))
(defun run-job (job)
  (let* ((sleeping (compare-and-swap (job-status job) :sleeping :running)))
    (assert (eq :sleeping sleeping)))
  (handler-case (invoke-job job)
    (:no-error (&rest stuff)
      (apply #'job-done job stuff))
    (await-condition (await-condition)
      (job-await job await-condition))
    (yield-condition (yield-condition)
      (job-yield job yield-condition))
    ;; TODO: handle the case where JOB signals a `serious-condition' which denotes an error, rather than one
    ;; of our control-flow-related conditions?
    ))

(typedec #'job-await (func (job await-condition) void))
(defun job-await (waiting-job await-condition)
  (assert (eq :running (barrier (:memory) (job-status waiting-job))))
  (setf (job-body waiting-job) (callback await-condition))
  (job-make-awaiting waiting-job (upon await-condition))
  (values))

(typedec #'job-yield (func (job yield-condition) void))
(defun job-yield (job yield-condition)
  (assert (eq :running (barrier (:memory) (job-status job))))
  (setf (job-body job) (callback yield-condition))
  (add-job job))

;;; misc. job operations

(typedec #'job-done-p (func (job) boolean))
(defun job-done-p (job)
  (eq :done (barrier (:memory) (job-status job))))

(typedec #'wait-for (func (job) (values &rest t)))
(defun wait-for (job)
  "Block the current thread until JOB finishes, then return its values.

Should not be called within an `async' block - intended for non-worker threads which create jobs and then must
wait for their completion."
  (iter (until (job-done-p job))
    (thread-yield)
    (finally (return (values-list (get-return-values job))))))

(typedec #'job-seq (func (job &rest function) job))
(defun job-seq (first-job &rest then-functions)
  "For each of the THEN-FUNCTIONS, create a new `job' in the same executor as FIRST-JOB which awaits its predecessor and takes its return values as arguments.

Analogous to a chain of `Promise.then's in Javascript (only without the error-handling support)."
  (iter (declare (declare-variables))
    (with job = first-job)
    (with executor = (job-executor job))
    (for function in then-functions)
    (setf job (make-job
               :executor executor
               :awaiting job
               :body function))
    (finally (return job))))
