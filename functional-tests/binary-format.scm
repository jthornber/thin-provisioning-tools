(library
  (binary-format)

  (export unpack-type
          size-type
          binary-format
          binary-format-names
          le32
          le64
          bytes)

  (import (chezscheme)
          (fmt fmt)
          (list-utils))

  ;;;-----------------------------------------

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

#|
(define-syntax ordered-funcall
  (lambda (form)
    (let ((form^ (cdr (syntax->list form))))
     (let ((gens (map (lambda (_) (datum->syntax #'* (gensym "t"))) form^)))
      #`(let* #,(map list gens form^)
         #,gens)))))
|#

  (define-syntax ordered-funcall
    (lambda (x)
      (syntax-case x ()
        ((k f v ...)
         (with-syntax
           ([(t ...) (map (lambda (_)
                            (datum->syntax #'k (gensym)))
                          #'(v ...))])
           #'(let* ([t v] ...)
               (f t ...)))))))

  (define-syntax binary-format-names
    (syntax-rules ()
      ((_ (name pack-name unpack-name size-name) (field type) ...)
       (begin
         (define-record-type name (fields field ...))

         (define size-name
           (+ (size-type type) ...))

         (define (unpack-name bv offset)
           (let ((offset offset))

            (define (inc-offset n v)
              (set! offset (+ offset n))
              v)

            (ordered-funcall
              (record-constructor (record-constructor-descriptor name))
              (inc-offset (size-type type) (unpack-type bv offset type)) ...)))))))

  (define-syntax binary-format
    (lambda (x)
      ;;; FIXME: we don't need multiple args
      (define (gen-id template-id . args)
        (datum->syntax template-id
          (string->symbol
            (apply string-append
                   (map (lambda (x)
                          (if (string? x)
                              x
                              (symbol->string (syntax->datum x))))
                        args)))))
      (syntax-case x ()
                   ((_ name field ...)
                    (with-syntax ((pack-name (gen-id #'name #'name "-pack"))
                                  (unpack-name (gen-id #'name #'name "-unpack"))
                                  (size-name (gen-id #'name #'name "-size")))
                                 #'(binary-format-names (name pack-name unpack-name size-name) field ...))))))

  ;;; Since le32, le64 and bytes are used as auxiliary keywords, we must export
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

