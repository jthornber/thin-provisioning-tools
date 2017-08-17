(library
  (list-utils)

  (export tails
          intersperse
          iterate
          comparing
          list-group-by
          accumulate)

  (import (rnrs)
          (srfi s8 receive))

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

  (define (comparing cmp key)
    (lambda (x y)
      (cmp (key x) (key y))))

  ;; Assumes the list is already sorted
  (define (list-group-by pred xs)
    (if (null? xs)
        '()
        (let ((fst (car xs)))
         (receive (g1 gs) (partition (lambda (x) (pred fst x)) (cdr xs))
                  (cons (cons fst g1)
                        (list-group-by pred gs))))))

  ;; calculates a running total for a list.  Returns a list.
  (define (accumulate xs)
    (let loop ((xs xs) (total 0))
     (if (null? xs)
         '()
         (cons total (loop (cdr xs) (+ total (car xs))))))))
