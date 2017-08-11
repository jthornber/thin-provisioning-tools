(import
  (binary-format)
  (block-io)
  (btree)
  (fmt fmt)
  (matchable)
  (mapping-tree)
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

(binary-format superblock
  (csum le32)
  (flags le32)
  (block-nr le64)
  (uuid (bytes $uuid-size))
  (magic le64)
  (version le32)
  (time le32)
  (trans-id le64)
  (metadata-snap le64)
  (data-space-map-root (bytes $space-map-root-size))
  (metadata-space-map-root (bytes $space-map-root-size))
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
                 (superblock-unpack (read-block md 0) 0)))

(define (dump-dev-tree)
  (with-metadata (md (current-metadata))
                 (let ((sb (superblock-unpack (read-block md 0) 0)))
                  (btree-each (btree-open le64-type md (superblock-data-mapping-root sb))
                              (lambda (k v)
                                (fmt #t (dsp "dev-id: ") (num k)
                                     (dsp ", mapping root: ") (num v) nl))))))

(define (dump-mappings root)
  (with-metadata (md (current-metadata))
                 (btree-each (btree-open le64-type md root)
                             (lambda (k v)
                               (fmt #t (dsp "vblock: ") (num k)
                                    (dsp ", pblock: ") (num v) nl)))))

(define (dump-all-mappings)
  (with-metadata (md (current-metadata))
                 (let ((sb (superblock-unpack (read-block md 0) 0)))
                  (let ((mappings (mapping-tree-open md (superblock-data-mapping-root sb))))
                   (mapping-tree-each mappings
                                      (lambda (dev-id vblock pblock)
                                        (fmt #t
                                             (dsp "thin dev ") (num dev-id)
                                             (dsp ", vblock ") (num vblock)
                                             (dsp ", pblock ") (num pblock)
                                             nl)))))))

(define (check-superblock)
  (with-metadata (md (current-metadata))
                 (let ((superblock (read-block md 0)))
                  (fmt #t (dsp "checksum on disk: ") (dsp (bytevector-u32-ref superblock 0 (endianness little))) nl)
                  ;(fmt #t (dsp "calculated checksum: ") (dsp (crc32-region $superblock-salt superblock 4 4092)) nl)
                  (check-magic superblock))))
