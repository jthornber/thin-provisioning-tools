(library
  (binary-format)
  (export size-type binary-format le32 le64 bytes)
  (import (rnrs)
          (list-utils))

  (define-syntax size-type
    (syntax-rules (le32 le64 bytes)
      ((_ le32) 4)
      ((_ le64) 8)
      ((_ (bytes count)) count)))

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

  (define-syntax binary-format
    (syntax-rules ()
      ((_ (name pack-name unpack-name) (field type) ...)
       (begin
         (define-record-type name (fields field ...))

       (define (unpack-name bv offset)
         (let ((offset offset))

          (define (inc-offset n v)
            (set! offset (+ offset n))
            v)

          ((record-constructor (record-constructor-descriptor name))
           (inc-offset (size-type type) (unpack-type bv offset type)) ...)))))))

  ;;; since le32, le64 and bytes are used as auxiliary keywords, we must export
  ;;; definitions of them as well.
  ;;; FIXME: use a macro to remove duplication
  (define-syntax le32
    (lambda (x)
      (syntax-violation 'le32 "misplaced auxiliary keyword" x)))

  (define-syntax le64
    (lambda (x)
      (syntax-violation 'le64 "misplaced auxiliary keyword" x)))

  (define-syntax bytes
    (lambda (x)
      (syntax-violation 'bytes "misplaced auxiliary keyword" x))))

