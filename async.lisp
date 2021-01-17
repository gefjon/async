(uiop:define-package async/async
  (:nicknames async)
  (:mix cl iterate)
  (:import-from bordeaux-threads
                semaphore make-semaphore signal-semaphore wait-on-semaphore
                recursive-lock make-recursive-lock with-recursive-lock-held
                thread make-thread thread-yield join-thread)
  (:import-from alexandria
                once-only with-gensyms)
  (:import-from gefjon-utils
                define-special
                adjustable-vector
                typedec func void
                define-class
                with-slot-accessors)
  (:import-from cl-cont
                with-call/cc let/cc)
  (:export
   make-executor
   async await yield
   cancel-job-queue))
(in-package async)

(declaim (optimize (safety 3)))

(define-class monitor
    ((lock recursive-lock
           :initform (make-recursive-lock)
           :initarg nil))
  :documentation "A mixin for an object to which access must be synchronized.

Accesses to a monitor object O should be wrapped in (`with-monitor' O `&body' BODY)")

(defmacro with-monitor (monitor &body body)
  "Execute BODY with synchronized-safe access to MONITOR.

MONITOR must be a CLOS object which mixes in `monitor'."
  (once-only (monitor)
    `(progn
       (assert (typep ,monitor 'monitor) ()
               "Attempt to `with-monitor' on non-monitor object ~a" ,monitor)
       (with-recursive-lock-held ((lock ,monitor))
         ,@body))))

(defmacro read-monitored-slots ((&rest slot-names) monitor &body body)
  "Evaluate BODY with each of the SLOT-NAMES bound to the associated accessor-value from MONITOR, but without MONITOR's lock held.

The SLOT-NAMES should be a list of symbols which name accessors to MONITOR. They will be bound as values, but
not as `setf'-able places, within BODY."
  (once-only (monitor)
    `(multiple-value-bind ,slot-names
         (with-monitor ,monitor (with-slot-accessors ,slot-names ,monitor (values ,@slot-names)))
       ,@body)))

(defmacro write-monitored-slots (monitor &body slots-and-values)
  "For each (ACCESSOR VALUE) in SLOTS-AND-VALUES, store VALUE into MONITOR by (`setf' ACCESSOR)."
  (once-only (monitor)
    `(with-monitor ,monitor
       (setf ,@(iter (for (slot value) in slots-and-values)
                 (collect `(,slot ,monitor))
                 (collect value))))))

(define-class queue
    ((head list :initform nil :initarg nil)
     (tail list :initform nil :initarg nil))
  :superclasses (monitor)
  :documentation "A synchronized ordered collection which supports `push-back' and `pop-front'")

(defmacro do-queue ((var queue &optional (result '(values)))
                    &body body)
  "Like `dolist', but drains elements from the `queue' referred to by QUEUE.

When this form exits, QUEUE will likely be empty. It is safe for concurrent mutators to add elements to QUEUE
by `push-back' or to remove them with `pop-front'."
  (with-gensyms (finishedp)
    (once-only (queue)
      `(iter
         (for (values ,var ,finishedp) = (pop-front ,queue))
         (unless ,finishedp (finish))
         ,@body
         (finally (return ,result))))))

(typedec #'push-back (func (t queue) void))
(defun push-back (new-element queue)
  (with-monitor queue
    (with-slot-accessors (head tail) queue
      (if tail
          (progn (assert (null (cdr tail)))
                 (setf (cdr tail) (list new-element)))
          (progn (assert (null head))
                 (setf head (list new-element)
                       tail head)))))
  (values))

(typedec #'pop-front (func (queue) (values t boolean)))
(defun pop-front (queue)
  "Remove and return the next element from QUEUE. Returns VALUE-FOUND-P as a secondary value.

If a value was removed, returns it as a primary value and T as a secondary value.

If QUEUE was empty, returns (`values' nil nil)."
  (with-monitor queue
    (with-slot-accessors (head tail) queue
      (if head
          (progn (let* ((popped (pop head)))
                   (assert tail)
                   (when (not head)
                     (setf tail nil))
                   (values popped t)))
          (values nil nil)))))

(define-class job-queue
    ((jobs-in-queue semaphore
                    :initform (make-semaphore)
                    :initarg nil)
     (jobs queue
           :initform (make-instance 'queue)
           :initarg nil)
     (workers queue
                :initform (make-instance 'queue)
                :initarg nil)
     (nthreads (and fixnum unsigned-byte (not (eql 0))))
     (stop boolean
           :initarg nil
           :initform nil))
  :superclasses (monitor)
  :documentation "A queue of `job's associated with a set of worker threads.

This class's `monitor' instance is intended to apply only to its field `stop'")

(define-special *job-queue* job-queue
    "The current asynchronous executor environment.

Will be specially bound within each worker thread when they are spawned. Should be globally unbound.")

(typedec #'cancel-job-queue (func (job-queue) void))
(defun cancel-job-queue (job-queue)
  "Cancel any jobs remaining in JOB-QUEUE and join its worker threads."
  (write-monitored-slots job-queue
    (stop t))
  (signal-semaphore (jobs-in-queue job-queue)
                    :count (nthreads job-queue))
  (do-queue (thread (workers job-queue) (values))
    (join-thread thread)))

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

(define-class job
    ((status (member :running :not-registered :blocked :sleeping :done)
             :initform :not-registered
             :initarg nil)
     (return-values list
                    :may-init-unbound t
                    :initarg nil
                    :documentation "After this job finishes, a list of its return values as by `multiple-value-list'.

Will be passed as arguments to jobs which are `awaiting' this job, that is, jobs in this job's `blocking'
queue.")
     (blocking queue
               :initform (make-instance 'queue)
               :initarg nil
               :documentation "Backpointers to jobs which are `awaiting' this job.

Maintained so that we can re-ready and enqueue those jobs when this job finishes.

Enqueues and dequeues to and from BLOCKING should not happen while the `job''s `monitor' is held; the `queue'
has its own synchronization.")
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

(typedec #'add-job (func (job &optional job-queue) void))
(defun add-job (job &optional (job-queue *job-queue*))
  "Insert JOB into JOB-QUEUE, to be run whenever a worker thread is available."
  (with-monitor job
    (ecase (status job)
      ((:blocked :running :done) (error "Attept to add a job with status ~a to work queue" (status job)))
      (:not-registered (setf (status job) :sleeping))
      (:sleeping nil))
    (push-back job (jobs job-queue)))
  (signal-semaphore (jobs-in-queue job-queue))
  (values))

(define-class job-queue-exit ()
  :condition t)

(typedec #'get-job (func (&optional job-queue) job))
(defun get-job (&optional (job-queue *job-queue*))
  "Return the next `job' from JOB-QUEUE, blocking if none are available yet.

Signals `job-queue-exit' if JOB-QUEUE has been cancelled."
  (flet ((possibly-stop ()
           (if (read-monitored-slots (stop) job-queue stop)
               (error 'job-queue-exit))))
    (possibly-stop)
    (wait-on-semaphore (jobs-in-queue job-queue))
    ;; check whether to stop both before and after waiting on the semaphore because `cancel-job-queue' signals
    ;; the semaphore to wake sleeping worker threads, so it's possible the `wait-on-semaphore' will return
    ;; when `job-queue' is empty.
    (possibly-stop))
  (values (pop-front (jobs job-queue))))

(typedec #'job-done (func (job &rest t) void))
(defun job-done (finished-job &rest ret-vals)
  ;; first, mark FINISHED-JOB as completed and record its return values
  (write-monitored-slots finished-job
    (return-values ret-vals)
    (status :done))
  ;; then, for each job it was `blocking',
  (do-queue (blocked-job (blocking finished-job))
    (with-monitor blocked-job
      (assert (eq (awaiting blocked-job) finished-job))
      (assert (eq (status blocked-job) :blocked))
      ;; change that job's status from `:blocked'
      (setf (status blocked-job) :sleeping))
    ;; and insert it into the `job-queue'.
    (add-job blocked-job))
  (values))

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
         (push-back waiting-job
                    ;; recall that reads from `blocking' queues need not be synchronized by the `job'
                    ;; `monitor' because `queue' is itself a monitor.
                    ;;
                    ;; FIXME: possible edge case where we read STATUS as `:sleeping', then UPON finishes,
                    ;; empties its `blocking' queue and becomes `:done', and then our `push-back' takes
                    ;; effect. Would this leave WAITING-JOB permanently blocking an already-finished job?
                    (blocking upon)))
        (:not-registered (error "attempt to wait on unregistered job"))
        (:done
         ;; if WAITING-JOB attempted to `await' an already-finished job, run it immediately.
         ;;
         ;; TODO: consider handling this case in the `await' macro to avoid spurious non-local control flow
         ;; and synchronization. That check would be in addition to this branch, but this branch should remain
         ;; here - there would be an edge case where UPON finishes between the `await' check and this check.
         (run-job waiting-job)))))
  (values))

(typedec #'invoke-job (func (job) void))
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
      (with-monitor job
        (setf (status job) :sleeping
              (body job) (callback yield-condition)
              (awaiting job) nil))
      (add-job job))
    ;; TODO: handle the case where JOB signals a `serious-condition' which denotes an error, rather than one
    ;; of our control-flow-related conditions?
    ))

(typedec #'worker-loop (func (job-queue) void))
(defun worker-loop (queue)
  (let* ((*job-queue* queue))
    (handler-case (iter (for job = (get-job queue))
                    (run-job job))
      (job-queue-exit () (return-from worker-loop (values))))))

(typedec #'make-executor (func ((and unsigned-byte fixnum (not (eql 0)))) job-queue))
(defun make-executor (nthreads)
  (let* ((queue (make-instance 'job-queue
                               :nthreads nthreads)))
    (flet ((worker-body ()
             (worker-loop queue)))
      (iter (for i below (nthreads queue))
        (push-back (make-thread #'worker-body)
                   (workers queue))))
    queue))

(defmacro async ((&optional (job-queue '*job-queue*))
                 &body body)
  "Enqueue BODY as an asynchronous `job' into JOB-QUEUE.

Within BODY, the `await' and `yield' macros are available to delay computation."
  `(let ((job (make-instance 'job
                             :body (lambda ()
                                     (with-call/cc
                                       ,@body)))))
     (add-job job ,job-queue)
     job))

(defmacro await (job)
  "Block the current job on JOB, causing the current job to not be executed again until JOB finishes, and returning JOB's return values.

May only occur within the lexical and dynamic extent of an `async' block."
  `(let/cc callback
     (error 'await-condition
            :upon ,job
            :callback callback)))

(defmacro yield ()
  "Pause the current job and place it at the end of the job queue.

May only occur within the lexical and dynamic extent of an `async' block."
  '(progn
    (let/cc callback
      (error 'yield-condition
             :callback callback))
    (values)))
