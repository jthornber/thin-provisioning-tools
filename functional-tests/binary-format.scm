(library
  (binary-format)
  (export binary-format)
  (import (rnrs))

  (define-syntax unpack-type
    (syntax-rules (le32 le64 bytes)
      ((_ bv offset le32)
       (bytevector-u32-ref bv offset (endianness little)))

      ((_ bv offset le64)
       (bytevector-u64-ref bv offset (endianness little)))

      ((_ bv offset (bytes count))
       (let ((copy (make-bytevector count)))
        (bytevector-copy! bv offset copy 0 count)
        copy))))

  (define (size-type t)
    (syntax-case t (le32 le64 bytes)
                 (le32 #'4)
                 (le64 #'8)
                 ((bytes count) #'count)))

  ;;; FIXME: (bytes <count>) has to use a literal rather than a symbol.
  (define-syntax binary-format
    (lambda (x)
      (syntax-case x ()
                   ((_ (name pack-name unpack-name) (field type) ...)
                    (with-syntax ((((t o) ...)
                                   (let f ((acc 0) (types #'(type ...)))
                                    (if (null? types)
                                        '()
                                        (cons (list (car types) acc)
                                              (f (+ (syntax->datum (size-type (car types))) acc) (cdr types)))))))
                                 #`(begin
                                     (define-record-type name (fields field ...))

                                     (define (unpack-name bv offset)
                                       ((record-constructor (record-type-descriptor name))
                                        (unpack-type bv (+ offset o) t) ...)))))))))
