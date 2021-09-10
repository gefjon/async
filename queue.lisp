(uiop:define-package :async/queue
  (:mix :cl :iterate)
  (:import-from :async/monitor
                #:monitor #:with-monitor #:monitor-wait-until)
  (:import-from :gefjon-utils
                #:define-class #:with-slot-accessors
                #:typedec #:func #:void)
  (:import-from :alexandria
                #:with-gensyms #:once-only)
  (:export
   #:queue
   #:do-queue
   #:list-queue
   #:push-back #:pop-front #:queue-make-empty #:queue-replace-contents))
(in-package :async/queue)

(define-class queue
    ((head list :initform nil)
     (tail list :initform nil :initarg nil))
  :superclasses (monitor)
  :documentation "A synchronized ordered collection which supports `push-back' and `pop-front'")

(defmethod initialize-instance :after ((queue queue) &key &allow-other-keys)
  (when (head queue)
    (setf (tail queue)
          (last (head queue))))
  (values))

(typedec #'queue-make-empty (func (queue) void))
(defun queue-make-empty (queue)
  (with-monitor queue
    (setf (head queue) nil
          (tail queue) nil))
  (values))

(typedec #'unsynchronized-queue-empty-p (func (queue) boolean))
(defun unsynchronized-queue-empty-p (queue)
  (null (head queue)))

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

(typedec #'list-queue (func (list) queue))
(defun list-queue (list)
  (make-instance 'queue
                 :head list))

(typedec #'unsynchronized-push-back (func (t queue) void))
(defun unsynchronized-push-back (new-element queue)
  (with-slot-accessors (head tail) queue
    (if tail
        (let* ((new-tail (list new-element)))
          (assert (null (cdr tail)))
          (setf (cdr tail) new-tail
                tail new-tail))
        (progn (assert (null head))
               (setf head (list new-element)
                     tail head))))
  (values))

(typedec #'push-back (func (t queue) void))
(defun push-back (new-element queue)
  (with-monitor queue
    (unsynchronized-push-back new-element queue))
  (values))

(typedec #'unsynchronized-pop-front (func (queue) (values t boolean)))
(defun unsynchronized-pop-front (queue)
  (with-slot-accessors (head tail) queue
    (if head
        (progn (let* ((popped (pop head)))
                 (assert tail)
                 (when (not head)
                   (setf tail nil))
                 (values popped t)))
        (values nil nil))))

(typedec #'pop-front (func (queue) t))
(defun pop-front (queue)
  "Remove and return the next element from QUEUE. Returns VALUE-FOUND-P as a secondary value.

If a value was removed, returns it as a primary value and T as a secondary value.

If QUEUE was empty, returns (`values' nil nil)."
  (values
   (monitor-wait-until queue (not (unsynchronized-queue-empty-p queue))
     (unsynchronized-pop-front queue))))
