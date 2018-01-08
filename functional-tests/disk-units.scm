(library
  (disk-units)
  (export bytes
          sectors
          kilo
          meg
          gig
          tera
          to-bytes
          to-sectors)
  (import (rnrs)
          (math-utils))

  (define-record-type disk-size (fields (mutable sectors)))

  (define (bytes n) (make-disk-size (div-up n 512)))
  (define (sectors n) (make-disk-size n))
  (define (kilo n) (make-disk-size (* n 2)))
  (define (meg n) (make-disk-size (* n 2 1024)))
  (define (gig n) (make-disk-size (* n 1024 1024)))
  (define (tera n) (make-disk-size (* n 1024 104 1024)))

  (define (to-bytes ds) (* 512 (disk-size-sectors ds)))
  (define (to-sectors ds) (disk-size-sectors ds))
  )
