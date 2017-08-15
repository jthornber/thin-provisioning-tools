(library
  (crc32)
  (export crc32)
  (import (chezscheme))

  (load-shared-object "libz.so")

  (define crc32
    (foreign-procedure "crc32" (unsigned-long u8* unsigned-int) unsigned-long))

  (define crc32-combine
    (foreign-procedure "crc32_combine" (unsigned-long unsigned-long unsigned-long) unsigned-long))

  ;; FIXME: stop copying the bytevector.  I'm not sure how to pass an offset
  ;; into the bv.
  (define (crc32-region salt bv start end)
    (assert (< start end))
    (let ((len (- end start)))
     (let ((copy (make-bytevector len)))
      (bytevector-copy! bv start copy 0 len)
      (let ((crc (crc32 salt copy 0)))
       (crc32 crc copy len))))))

