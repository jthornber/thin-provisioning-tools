(library
  (btree)

  (export btree-value-type
          btree-dev
          btree-root
          btree-open
          btree-lookup
          btree-each
          le64-type)

  (import (block-io)
          (chezscheme)
          (binary-format)
          (list-utils))

  ;;; Unlike the kernel or c++ versions, I'm going to leave it to the hiogher
  ;;; levels to handle multi level btrees.
  (binary-format node-header
    (csum le32)
    (flags le32)
    (blocknr le64)
    (nr-entries le32)
    (max-entries le32)
    (value-size le32)
    (padding le32))

  ;;; (unpacker bv offset)
  (define-record-type value-type (fields size unpacker))

  (define (max-entries vt)
    (/ (- metadata-block-size node-header-size)
       (+ (size-type le64)
          (value-type-size vt))))

  (define (key-offset index)
    (+ node-header-size (* (size-type le64) index)))

  (define (value-base header)
    (+ node-header-size
       (* (node-header-max-entries header)
          (size-type le64))))

  (define (value-offset header vt index)
    (+ (value-base header)
       (* (value-type-size vt) index)))

  (define-record-type btree
                      (fields value-type dev root))

  (define (btree-open vt dev root)
    (make-btree vt dev root))

  (define le64-type
    (make-value-type (size-type le64)
                     (lambda (bv offset)
                       (unpack-type bv offset le64))))

  (define (internal-node? header)
    (bitwise-bit-set? (node-header-flags header) 0))

  (define (leaf-node? header)
    (bitwise-bit-set? (node-header-flags header) 1))

  (define (key-at node index)
    (unpack-type node (key-offset index) le64))

  (define (value-at header node index vt)
    ((value-type-unpacker vt) node (value-offset header vt index)))

  ;;; Performs a binary search looking for the key and returns the index of the
  ;;; lower bound.
  (define (lower-bound node header key)
    (let loop ((lo 0) (hi (node-header-nr-entries header)))
      (if (<= (- hi lo) 1)
          lo
          (let* ((mid (+ lo (/ (- hi lo) 2)))
                 (k (key-at node mid)))
            (cond
              ((= key k) mid)
              ((< k key) (loop mid hi))
              (else (loop lo mid)))))))

  ;;;;----------------------------------------------
  ;;;; Lookup
  ;;;;----------------------------------------------

  (define (btree-lookup tree key default)
    (let ((dev (btree-dev tree))
          (vt (btree-value-type tree)))

      (let loop ((root (btree-root tree)))
       (let* ((node (read-block dev root))
              (header (node-header-unpack node 0))
              (index (lower-bound node header key)))
         (if (internal-node? header)
             (loop (value-at header node index le64-type))
             (if (= key (key-at node index))
                 (value-at header node index vt)
                 default))))))

  ;;;;----------------------------------------------
  ;;;; Walking the btree
  ;;;;----------------------------------------------

  ;;; Calls (fn key value) on every entry of the btree.
  (define (btree-each tree fn)
    (let ((vt (btree-value-type tree)))

     (define (visit-leaf node header)
       (let loop ((index 0))
        (when (< index (node-header-nr-entries header))
          (fn (key-at node index) (value-at header node index vt))
          (loop (+ 1 index)))))

     (define (visit-internal node header)
       (let loop ((index 0))
        (when (< index (node-header-nr-entries header))
          (visit-node (value-at header node index le64-type))
          (loop (+ 1 index)))))

     (define (visit-node root)
       (let* ((node (read-block (btree-dev tree) root))
              (header (node-header-unpack node 0)))
         ((if (internal-node? header) visit-internal visit-leaf) node header)))

     (visit-node (btree-root tree)))))

