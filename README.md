# async compute in SBCL

This library is a prototype implementation of an async interface and executor for
multithreaded computation in Common Lisp on SBCL. It makes heavy use of SBCL's atomics
interface in its lock-free executor, and as such is not portable to other Common Lisp
implementations.

Unlike many async frameworks in the world, this library is not suited to async I/O,
essentially because I haven't written a wrapper around `kqueue` / `epoll` / whatever
Windows has. It is intended for CPU compute-intensive parallelism of the variety I
encounter in my planning work.

## Examples (taken straight from the tests)

### Using `asynchronously`, sleep for a second, then do some addition

```
(asynchronously (2) ; creates an executor with two worker threads
   (let* ((one (async ()
                 (sleep 1) ; this blocks the thread, preventing other jobs from running
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
     (+ (await twelve) (await fifteen))))
```

### Using `job-seq`, pipe the result of a job into the next

`job-seq` looks a lot like Haskell's `do`

```
(with-executor (1) ; creates an executor with one worker thread
  (wait-for (job-seq (async () 1)
                     (lambda (one) (+ one 1))
                     (lambda (two) (* two two))
                     (lambda (four) (- four 1)))))
```

### Using `with-executor`, spawn an ungodly amount of jobs to do inefficient recursive exponentiation

```
(with-executor (4) ; four worker threads
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
        (finish-output *test-dribble*)))))
```
