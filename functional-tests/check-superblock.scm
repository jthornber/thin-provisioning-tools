(import
  (fmt fmt)
  (matchable))

;;;;---------------------------------------------------
;;;; Constants
;;;;---------------------------------------------------

;; FIXME: duplicate with main.scm
(define (current-metadata)
  "./metadata.bin")

(define metadata-block-size 4096)
(define superblock-magic 27022010)
(define superblock-salt 160774)
(define uuid-size 16)
(define space-map-root-size 128)

;;;;---------------------------------------------------
;;;; Metadata IO
;;;;---------------------------------------------------

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
      (io-error (fmt #f (dsp "Unable to read metadata block: ") (num b)))))

;;; FIXME: implement a little block cache.


;;;;---------------------------------------------------
;;;; CRC32
;;;;---------------------------------------------------

;; FIXME: move to own library
(load-shared-object "libz.so")
(define crc32
  (foreign-procedure "crc32" (unsigned-long u8* unsigned-int) unsigned-long))

(define crc32-combine
  (foreign-procedure "crc32_combine" (unsigned-long unsigned-long unsigned-long) unsigned-long))

;; FIXME: stop copying the bytevector.  I'm not sure how to pass an offset into
;; the bv.
(define (crc32-region salt bv start end)
  (assert (< start end))
  (let ((len (- end start)))
   (let ((copy (make-bytevector len)))
    (bytevector-copy! bv start copy 0 len)
    (let ((crc (crc32 salt copy 0)))
     (crc32 crc copy len)))))

;;;;---------------------------------------------------
;;;; Decoding
;;;;---------------------------------------------------

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
                                      (unpack-type bv (+ offset o) t) ...))))))))

(binary-format (superblock pack-superblock unpack-superblock)
  (csum le32)
  (flags le32)
  (block-nr le64)
  (uuid (bytes 16))
  (magic le64)
  (version le32)
  (time le32)
  (trans-id le64)
  (metadata-snap le64)
  (data-space-map-root (bytes 128))
  (metadata-space-map-root (bytes 128))
  (data-mapping-root le64)
  (device-details-root le64)
  (data-block-size le32)
  (metadata-block-size le32)
  (metadata-nr-blocks le64)
  (compat-flags le32)
  (compat-ro-flags le32)
  (incompat-flags le32))

;;;;---------------------------------------------------
;;;; Top level
;;;;---------------------------------------------------

(define (check-magic sb)
  ((let ((m (bytevector-u32-ref sb 32 (endianness little))))
    (fmt #t (dsp "on disk magic: ") (num m) nl)
    )))

(define (read-superblock)
  (with-metadata (md (current-metadata))
                 (unpack-superblock (read-block md 0) 0)))

(define (check-superblock)
  (with-metadata (md (current-metadata))
                 (let ((superblock (read-block md 0)))
                  (fmt #t (dsp "checksum on disk: ") (dsp (bytevector-u32-ref superblock 0 (endianness little))) nl)
                  (fmt #t (dsp "calculated checksum: ") (dsp (crc32-region superblock-salt superblock 4 4092)) nl)
                  (check-magic superblock))))
