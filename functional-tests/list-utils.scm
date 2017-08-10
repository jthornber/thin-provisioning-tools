(library
  (list-utils)
  (export tails intersperse iterate accumulate)
  (import (rnrs))

  (define (tails xs)
    (if (null? xs)
        '()
        (cons xs (tails (cdr xs)))))

  (define (intersperse sep xs)
    (cond
      ((null? xs) '())
      ((null? (cdr xs)) xs)
      (else (cons (car xs)
                  (cons sep
                        (intersperse sep (cdr xs)))))))

  (define (iterate fn count)
    (let loop ((count count))
     (if (zero? count)
        '()
        (cons (fn) (loop (- count 1))))))

  ;; calculates a running total for a list.  Returns a list.
  (define (accumulate xs)
    (let loop ((xs xs) (total 0))
     (if (null? xs)
         '()
         (cons total (loop (cdr xs) (+ total (car xs))))))))
