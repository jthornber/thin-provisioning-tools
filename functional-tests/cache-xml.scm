(library
  (cache-xml)
  (export generate-xml)
  (import (rnrs)
          (list-utils)
          (loops)
          (generators)
          (xml)
          (fmt fmt)
          (srfi s27 random-bits)
          (only (srfi s1 lists) iota))

  (define (make-enum-vector count)
    (let ((v (make-vector count)))
     (upto (n count)
           (vector-set! v n n))
     v))

  (define (vector-swap! v n m)
    (let ((tmp (vector-ref v n)))
     (vector-set! v n (vector-ref v m))
     (vector-set! v m tmp)))

  (define (rnd b e)
    (+ b (random-integer (- e b))))

  (define (vector-partial-shuffle! v count)
    (let ((len (vector-length v)))
     (upto (n count)
           (vector-swap! v n (rnd n len)))))

  ;;---------------------------------------------------------

  (define (generate-xml block-size nr-origin-blocks nr-cache-blocks)
    (tag 'superblock `((uuid . "")
                       (block-size . ,block-size)
                       (nr-cache-blocks . ,nr-cache-blocks)
                       (policy . "smq")
                       (hint-width . 4))
         ;; FIXME: what a waste of memory
         (let ((v (make-enum-vector nr-origin-blocks)))
           (vector-partial-shuffle! v nr-cache-blocks)
           (vcat (map (lambda (cblock)
                        (tag 'mapping `((cache-block . ,cblock)
                                        (origin-block . ,(vector-ref v cblock))
                                        (dirty . "true"))))
                      (iota nr-cache-blocks)))))))
