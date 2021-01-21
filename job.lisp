(uiop:define-package async/job
  (:mix cl iterate)
  (:import-from gefjon-utils
                define-class define-special
                typedec func void list-of
                with-slot-accessors)
  (:import-from async/monitor
                monitor with-monitor read-monitored-slots write-monitored-slots monitor-wait-until)
  (:import-from async/queue
                queue push-back pop-front)
  (:import-from bordeaux-threads
                thread make-thread join-thread)
  (:export
   job-queue jobs nthreads workers
   job
   *job-queue*

   await-condition yield-condition
   job-queue-exit

   add-job wait-for))
(in-package async/job)

;;; classes

(define-class job-queue
    ((jobs queue
           :initform (make-instance 'queue)
           :initarg nil)
     (workers (list-of thread)
              :initarg nil
              :may-init-unbound t)
     (nthreads (and fixnum unsigned-byte (not (eql 0)))))
  :documentation "A queue of `job's associated with a set of worker threads.")

(define-class job
    ((status (member :running :not-registered :blocked :sleeping :done)
             :initform :not-registered
             :initarg nil)
     (return-values list
                    :may-init-unbound t
                    :initarg nil
                    :documentation "After this job finishes, a list of its return values as by `multiple-value-list'.

Will be passed as arguments to jobs which are `awaiting' this job, that is, jobs in this job's `awaiters'
queue.")
     (awaiters (list-of job)
               :initform nil
               :initarg nil
               :documentation "Backpointers to jobs which are `awaiting' this job.

Maintained so that we can re-ready and enqueue those jobs when this job finishes.")
     (awaiting (or null job)
               :initform nil
               :initarg nil
               :documentation "A job upon which this job is waiting, or nil if this job is ready to proceed.")
     (body function
           :documentation "A function which when run will carry out this job.

If BODY returns normally, this job will be marked as `:done', and its return values stored in
`return-values'.

If BODY signals `await-condition' or `yield-condition', the job will be set in that state,
and its BODY replaced with that condition's `callback'.

If `awaiting' is set to a non-nil `job', BODY will be invoked with that job's `return-values' as arguments."))
  :superclasses (monitor)
  :documentation "An asynchronous task to be executed by a worker thread in a `job-queue'.

This class's `monitor' instance should be held while reading or writing any of its fields, but not while
evaluating BODY.")

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

(typedec #'add-job (func (job &optional job-queue) void))
(defun add-job (job &optional (job-queue *job-queue*))
  "Insert JOB into JOB-QUEUE, to be run whenever a worker thread is available."
  (with-monitor job
    (ecase (status job)
      ((:blocked :running :done) (error "Attept to add a job with status ~a to work queue" (status job)))
      (:not-registered (setf (status job) :sleeping))
      (:sleeping nil)))
  (push-back job (jobs job-queue))
  (values))

(typedec #'get-job (func (&optional job-queue) job))
(defun get-job (&optional (job-queue *job-queue*)
                  &aux (jobs (jobs job-queue)))
  "Return the next `job' from JOB-QUEUE, blocking if none are available yet."
  (pop-front jobs))

;;; job queue construction and worker threads

(typedec #'worker-loop (func (job-queue) void))
(defun worker-loop (queue)
  (let* ((*job-queue* queue))
    (handler-case (iter (for job = (get-job queue))
                    (run-job job))
      (job-queue-exit () (return-from worker-loop (values))))))

(typedec #'make-worker-thread (func (job-queue) thread))
(defun make-worker-thread (job-queue)
  (flet ((worker-body ()
           (worker-loop job-queue)))
    (make-thread #'worker-body
                 :name (symbol-name (gensym "EXECUTOR-THREAD-")))))

(defmethod initialize-instance :after ((job-queue job-queue) &key &allow-other-keys)
  "Spawn worker threads"
  (flet ((worker-body ()
           (worker-loop job-queue)))
    (setf (workers job-queue)
          (iter (for i below (nthreads job-queue))
            (collect (make-thread #'worker-body
                                  :name (symbol-name (gensym "EXECUTOR-THREAD-"))))))))

;;; job life cycle

(typedec #'job-done (func (job &rest t) void))
(defun job-done (finished-job &rest ret-vals)
  (with-monitor finished-job
    ;; first, mark FINISHED-JOB as completed and record its return values
    (setf (return-values finished-job) ret-vals
          (status finished-job) :done)
    ;; then, for each job in its `awaiters',
    (dolist (blocked-job (awaiters finished-job))
      (with-monitor blocked-job
        (assert (eq (awaiting blocked-job) finished-job))
        (assert (eq (status blocked-job) :blocked))
        ;; change that job's status from `:blocked'
        (setf (status blocked-job) :sleeping))
      ;; and insert it into the `job-queue'.
      (add-job blocked-job)))
  (values))

(typedec #'invoke-job (func (job) (values &rest t)))
(defun invoke-job (job)
  (read-monitored-slots (awaiting body) job
    (apply body
           (if awaiting
               ;; if JOB was `awaiting' another `job', invoke BODY with its `return-values'.
               (read-monitored-slots (status return-values) awaiting
                 (assert (eq status :done))
                 return-values)
               ;; if not, invoke JOB with no arguments.
               nil))))

(typedec #'make-job-running (func (job) void))
(defun make-job-running (job)
  "Set the `status' of JOB to `:running' in a way observable to other threads."
  (with-monitor job
    (assert (eq (status job) :sleeping))
    (setf (status job) :running)))

(typedec #'run-job (func (job) void))
(defun run-job (job)
  (make-job-running job)
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
  (with-slot-accessors (callback upon) await-condition
    (write-monitored-slots waiting-job
      (awaiting upon)
      (status :blocked)
      (body callback))
    (read-monitored-slots (status) upon 
      (ecase status
        ((:blocked :sleeping :running)
         (push waiting-job (awaiters upon)))
        (:not-registered (error "attempt to wait on unregistered job"))
        (:done
         ;; if WAITING-JOB attempted to `await' an already-finished job, run it immediately.
         ;;
         ;; TODO: consider handling this case in the `await' macro to avoid spurious non-local control flow
         ;; and synchronization. That check would be in addition to this branch, but this branch should remain
         ;; here - there would be an edge case where UPON finishes between the `await' check and this check.
         (write-monitored-slots waiting-job
           (status :sleeping))
         (run-job waiting-job)))))
  (values))

(typedec #'job-yield (func (job yield-condition) void))
(defun job-yield (job yield-condition)
  (write-monitored-slots job
    (status :sleeping)
    (body (callback yield-condition))
    (awaiting nil))
  (add-job job))

;;; misc. job operations

(typedec #'unsynchronized-job-done-p (func (job) boolean))
(defun unsynchronized-job-done-p (job)
  (eq (status job) :done))

(typedec #'wait-for (func (job) (values &rest t)))
(defun wait-for (job)
  "Block the current thread until JOB finishes, then return its values.

Should not be called within an `async' block - intended for non-worker threads which create jobs and then must
wait for their completion."
  (monitor-wait-until job (unsynchronized-job-done-p job)
    (values-list (return-values job))))
