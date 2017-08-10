(import
  (binary-format)
  (block-io)
  (fmt fmt)
  (matchable)
  (rnrs))

;;;;---------------------------------------------------
;;;; Constants
;;;;---------------------------------------------------

;; FIXME: duplicate with main.scm
(define (current-metadata) "./metadata.bin")

(define $superblock-magic 27022010)
(define $superblock-salt 160774)
(define $uuid-size 16)
(define $space-map-root-size 128)

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
                  ;(fmt #t (dsp "calculated checksum: ") (dsp (crc32-region $superblock-salt superblock 4 4092)) nl)
                  (check-magic superblock))))
