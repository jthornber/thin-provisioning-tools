(library
  (device-mapper targets)

  (export linear-target)

  (import (chezscheme)
          (fmt fmt)
          (list-utils))

  (define-record-type segment
    (fields (mutable dev) (mutable begin) (mutable end)))

  (define (segment-size s)
          (- (segment-end s)
             (segment-begin s)))

  (define (join docs)
          (cat (intersperse (dsp " ") docs)))

  (define (format-segment s)
          (join (dsp (segment-dev s))))

  (define (linear-target seg)
    (dlambda
      (type () 'linear)
      (size () (segment-size seg))
      (format () (fmt #f (format-segment s)))))

  (define (stripe-target segments)
    (unless (apply = (map segment-size segments))
            (fail "stripe segments must all be the same size")
    (dlambda
      (type () 'stripe)
      (size () (fold-right + 0 (map segment-size segments)))
      (format () (fmt #f (join (map format-segment segments)))))))
  )
