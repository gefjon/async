(uiop:define-package async/monitor
  (:mix cl iterate)
  (:import-from bordeaux-threads
                lock make-lock with-lock-held
                make-condition-variable condition-wait condition-notify)
  (:import-from alexandria
                once-only with-gensyms)
  (:import-from gefjon-utils
                define-class
                with-slot-accessors)
  (:export
   monitor name
   with-monitor monitor-wait-until
   read-monitored-slots write-monitored-slots))
(in-package async/monitor)

(define-class monitor
    ((lock lock
           :initform (make-lock)
           :initarg nil)
     (cond-var t
               :initform (make-condition-variable)
               :initarg nil)
     (name symbol
           :may-init-unbound t
           :initarg nil))
  :documentation "A mixin for an object to which access must be synchronized.

Accesses to a monitor object O should be wrapped in (`with-monitor' O `&body' BODY)")

(defmethod initialize-instance :after ((instance monitor) &key &allow-other-keys)
  "Add a `name' for debugging purposes"
  (setf (name instance)
        (gensym (format nil "~a-" (class-name (class-of instance))))))

(defmacro with-monitor (monitor &body body)
  "Execute BODY with synchronized-safe access to MONITOR.

MONITOR must be a CLOS object which mixes in `monitor'."
  (once-only (monitor)
    (with-gensyms (return-values)
      `(progn
         (assert (typep ,monitor 'monitor) ()
                 "Attempt to `with-monitor' on non-monitor object ~a" ,monitor)
         (with-lock-held ((lock ,monitor))
           (let* ((,return-values (multiple-value-list (progn ,@body))))
             (condition-notify (cond-var ,monitor))
             (values-list ,return-values)))))))

(defmacro monitor-wait-until (monitor condition &body body)
  (once-only (monitor)
    (with-gensyms (return-values)
      `(with-monitor ,monitor
         (iter
           (for condition-p = ,condition)
           (until condition-p)
           (condition-wait (cond-var ,monitor) (lock ,monitor))
           (finally (let* ((,return-values (multiple-value-list (progn ,@body))))
                      (condition-notify (cond-var ,monitor))
                      (return (values-list ,return-values)))))))))

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
