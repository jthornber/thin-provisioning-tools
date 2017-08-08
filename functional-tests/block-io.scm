(library
  (block-io)
  (export metadata-block-size
          open-metadata
          with-metadata
          read-block)
  (import (rnrs)
          (fmt fmt))

  ;;;---------------------------------------------------
  ;;; TODO:
  ;;; - implement a little block cache.
  ;;; - writes
  ;;; - zero blocks
  ;;; - prefetching
  ;;;---------------------------------------------------

  (define metadata-block-size 4096)

  (define (open-metadata path)
    (open-file-input-port path (file-options) (buffer-mode none)))

  (define-syntax with-metadata
    (syntax-rules ()
      ((_ (port path) body ...) (let ((port (open-metadata path)))
                                 (dynamic-wind
                                   (lambda () #f)
                                   (lambda () body ...)
                                   (lambda () (close-port port)))))))

  ;; FIXME: return our own condition?
  (define (io-error msg)
    (raise (condition
             (make-error)
             (make-message-condition msg))))

  ;;; Returns a boolean indicating success
  (define (read-exact! port offset len bv start)
    (set-port-position! port offset)
    (let ((nr (get-bytevector-n! port bv start len)))
     (and (not (eof-object? nr))
          (= len nr))))

  ;;; Returns a 4k bytevector or #f
  (define (read-exact port offset len)
    (let ((bv (make-bytevector len)))
     (if (read-exact! port offset len bv 0) bv #f)))

  (define (read-block port b)
    (or (read-exact port (* b metadata-block-size) metadata-block-size)
        (io-error (fmt #f (dsp "Unable to read metadata block: ") (num b))))))


