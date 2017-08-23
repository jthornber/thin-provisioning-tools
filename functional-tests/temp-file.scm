(library
  (temp-file)

  (export
    temp-file
    temp-file-containing)

  (import (rnrs)
          (fmt fmt))

  (define temp-file
    (let ((counter 0))
     (lambda ()
       (let loop ()
        (let ((path (fmt #f (cat (dsp "/tmp/thinp-functional-tests-")
                                 (pad-char #\0 (pad/left 4 (num counter)))))))
          (set! counter (+ counter 1))
          (if (file-exists? path) (loop) path))))))

  ;; Creates a temporary file with the specified contents.
  (define (temp-file-containing contents)
    (let ((path (temp-file)))
     (with-output-to-file path (lambda () (put-string (current-output-port) contents)))
     path))

  )
