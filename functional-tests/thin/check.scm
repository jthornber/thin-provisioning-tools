(import
  (bcache block-manager)
  (btree)
  (fmt fmt)
  (matchable)
  (mapping-tree)
  (chezscheme))

;;;;---------------------------------------------------
;;;; Constants
;;;;---------------------------------------------------

;; FIXME: duplicate with main.scm
(define (current-metadata) "./metadata.bin")

(define $superblock-magic 27022010)
(define $superblock-salt 160774)
(define $uuid-size 16)
(define $space-map-root-size 128)

(define-ftype Superblock
  (packed
    (endian little
      (struct
        (csum unsigned-32)
        (flags unsigned-32)
        (block-nr unsigned-64)
        (uuid (bytes $uuid-size))
        (magic unsigned-32)
        (version unsigned-32)
        (time unsigned-32)
        (trans-id unsigned-64)
        (metadata-snap unsigned-64)
        (data-space-map-root (bytes $space-map-root-size))
        (metadata-space-map-root (bytes $space-map-root-size))
        (data-mapping-root unsigned-64)
        (device-details-root unsigned-64)
        (data-block-size unsigned-32)
        (metadata-block-size unsigned-32)
        (metadata-nr-blocks unsigned-64)
        (compat-flags unsigned-32)
        (compat-ro-flags unsigned-32)
        (incompat-flags unsigned-32)))))

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
                                      (lambda (dev-id vblock pblock time)
                                        (fmt #t
                                             (dsp "thin dev ") (num dev-id)
                                             (dsp ", vblock ") (num vblock)
                                             (dsp ", pblock ") (num pblock)
                                             (dsp ", time ") (num time)
                                             nl)))))))

(define (check-superblock)
  (with-metadata (md (current-metadata))
                 (let ((superblock (read-block md 0)))
                  (fmt #t (dsp "checksum on disk: ") (dsp (bytevector-u32-ref superblock 0 (endianness little))) nl)
                  ;(fmt #t (dsp "calculated checksum: ") (dsp (crc32-region $superblock-salt superblock 4 4092)) nl)
                  (check-magic superblock))))
