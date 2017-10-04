(import (chezscheme)
        (bcache block-manager)
        (fmt fmt)
        (loops)
        (persistent-data btree)
        (process)
        (srfi s8 receive)
        (thin metadata)
        (utils))

;;;;---------------------------------------------------------------------
;;;; Constraints
;;;; 1) No thin device has a data block mapped in more than once.
;;;; 2) Only root nodes may have fewer than max_entries/3
;;;; 3) tl-leaf values are all in-metadata-bounds?
;;;; 4) bl-leaf values are all in-data-bounds?
;;;; 5) internal values are all in-metadata-bounds? (true of every internal
;;;     node though, can't be used to differentiate).
(define (thin-check metadata root)
  (let ((exit-code (system
                     (fmt #f "../bin/thin_check --skip-mappings --override-mapping-root "
                          root " " metadata " > /dev/null 2>&1"))))
       (fmt #t "exit-code: " (wrt exit-code) ", ")
       (zero? exit-code)))

(define metadata "../customer-metadata-full.bin")

(define (read-superblock cache)
  (with-block (b cache 0 (get-flags))
    (block->superblock b)))

(define nr-metadata-blocks 8192)

(define (in-metadata-bounds? i)
  (< i nr-metadata-blocks))

(define nr-data-blocks (* 183 32 1024))

(define (in-data-bounds? i)
  (< i nr-data-blocks))

(define superblock-salt 160774)

(define (checksum-superblock cache)
  (with-block (b cache 0 (get-flags))
    (checksum-block b (ftype-sizeof unsigned-32) superblock-salt)))

(define (mk-conser v)
  (lambda (xs)
    (cons v xs)))

(define (add-rmap! rmap parent child)
  (hashtable-update! rmap child (mk-conser parent) '()))

(define (get-nr-entries hdr)
  (ftype-ref BTreeNodeHeader (nr-entries) hdr))

(define (all-values? pred b hdr vt)
  (let ((vals (block->values b vt)))
   (all? (lambda (i) (pred (value-at vt vals i)))
         (iota (get-nr-entries hdr)))))

(define (unpack-block bt)
  (receive (block time) (unpack-block-time bt)
    block))

(define (internal? b hdr)
  (internal-node? hdr))

(define (top-level-leaf? b hdr)
  (and (leaf-node? hdr)
       (= (ftype-sizeof unsigned-64) (ftype-ref BTreeNodeHeader (value-size) hdr))
       (all-values? in-metadata-bounds? b hdr le64-vt)))

(define (bottom-level-leaf? b hdr)
  (and (leaf-node? hdr)
       (= (ftype-sizeof unsigned-64) (ftype-ref BTreeNodeHeader (value-size) hdr))
       (let ((vals (block->values b le64-vt)))
        (all? in-data-bounds?
              (map (lambda (i)
                     (unpack-block (value-at le64-vt vals i)))
                   (iota (get-nr-entries hdr)))))))

(define (top-level-root? b hdr)
  (and (leaf-node? hdr)
       (< (ftype-ref BTreeNodeHeader (nr-entries) hdr)
          (/ (ftype-ref BTreeNodeHeader (max-entries) hdr) 3))))

(define (classify-node b hdr)
  (fold-left append '()
    (map (lambda (pair)
           (if ((car pair) b hdr)
               (cdr pair)
               '()))
         `((,internal? internal)
           (,bottom-level-leaf? bottom-level-leaf)
           (,top-level-leaf? top-level-leaf)))))

(define (checksum-btree-node b)
  (checksum-block b (ftype-sizeof unsigned-32) btree-node-salt))

(define (classify-nodes cache)
  (map (lambda (n)
         (with-block (b cache n (get-flags))
           (let ((hdr (block->header b))
                 (csum (checksum-btree-node b)))
                (cons n
                      (if (= csum (ftype-ref BTreeNodeHeader (csum) hdr))
                          (classify-node b hdr)
                          '())))))
       (iota (get-nr-blocks cache))))

;;; The rmap depends on which class we're assuming

(define-syntax detail-ref
  (syntax-rules ()
    ((_ ptr field)
     (ftype-ref ThinDeviceDetails (field) ptr))))

(define (dump-device-details cache)
  (let ((sb (read-superblock cache)))
   (device-tree-each cache (ftype-ref ThinSuperblock (device-details-root) sb)
     (lambda (dev-id dd)
       (fmt #t "dev-id: " dev-id "\n"
            "mapped-blocks: " (detail-ref dd mapped-blocks) "\n"
            "transaction-id: " (detail-ref dd transaction-id) "\n"
            "creation-time: " (detail-ref dd creation-time) "\n"
            "snappshotted-time: " (detail-ref dd snapshotted-time) "\n")))))

(define (dump-rmap rmap)
  (receive (keys values) (hashtable-entries rmap)
    (vector-for-each
      (lambda (k v)
        (fmt #t k ": " (dsp v) "\n"))
      keys
      values)))

;; An interesting node has more than one class.
(define (filter-interesting-blocks classes)
  (filter (lambda (xs)
            (> (length xs) 1))
          classes))

(with-bcache (cache metadata (* 16 1024))
  (dump-device-details cache)
  (let ((classes (classify-nodes cache))
        (rmap (make-eq-hashtable)))
   (fmt #t (filter-interesting-blocks classes))))

#|
(with-bcache (cache metadata (* 16 1024))
  (let ((sb (read-superblock cache)))
   (let ((actual-cs (checksum-superblock cache))
         (disk-cs (ftype-ref ThinSuperblock (csum) sb)))
        (fmt #t "actual checksum: " actual-cs ", disk checksum: " disk-cs "\n"))))
|#

#|
(let loop ((i 0)
           (successes '()))
 (if (> i 8192)
     (fmt #t "successes: " (wrt successes) "\n")
     ;; add --ignore-non-fatal-errors flag
     (if (thin-check "../customer-metadata-full.bin" i)
         (begin
           (fmt #t "success: " i "\n")
           (loop (+ i 1) (cons i successes)))
         (begin
           (fmt #t "fail: " i "\n")
           (loop (+ i 1) successes)))))
|# 
