(library
  (btree)

  (export btree-value-type
          btree-bcache
          btree-root
          btree-open
          btree-lookup
          btree-each
          le64-vt

          BTreeNodeHeader)

  (import (bcache block-manager)
          (chezscheme)
          (list-utils)
          (utils))

  (define-ftype BTreeNodeHeader
    (packed
      (endian little
              (struct
                (csum unsigned-32)
                (flags unsigned-32)
                (blocknr unsigned-64)
                (nr-entries unsigned-32)
                (max-entries unsigned-32)
                (value-size unsigned-32)
                (padding unsigned-32)))))

  (define-ftype LittleEndian64
    (endian little unsigned-64))

  ;; The metadata block is made up of:
  ;; | node header | keys | values |
  (define (block->header b)
    (make-ftype-pointer BTreeNodeHeader b))

  (define (block->keys b)
    (make-ftype-pointer LittleEndian64
                        (+ b (ftype-sizeof BTreeNodeHeader))))

  ;;; Value-types are dlambdas with these methods:
  ;;; (vt 'mk-ptr <raw-ptr>)
  ;;; (vt 'ref <vt-ptr> index)
  ;;; (vt 'set <vt-ptr> index val)
  ;;; (vt 'size)
  (define le64-vt
    (dlambda
      (mk-ptr (p) (make-ftype-pointer LittleEndian64 p))
      (ref (fp index) (ftype-ref LittleEndian64 () fp index))
      (set (fp index val) (ftype-set! LittleEndian64 () fp index val))
      (size () (ftype-sizeof LittleEndian64))))

  (define (block->values b vt)
    (vt
      (+ b (ftype-sizeof BTreeNodeHeader)
        (* (ftype-ref BTreeNodeHeader (max-entries) (block->header b))
           (ftype-sizeof LittleEndian64)))))

  (define (key-at keys index)
    (ftype-ref LittleEndian64 () keys index))

  (define (value-at vt vals index)
    (vt 'ref vals index))

  #|
  (define (max-entries vt)
    (/ (- metadata-block-size (ftype-sizeof BTreeNodeHeader))
       (+ (ftype-sizeof LittleEndian64)
          (vt 'size))))
  |#

  (define-record-type btree
    (fields value-type bcache root))

  (define (btree-open vt bcache root)
    (make-btree vt bcache root))

  ;;; (ftype-pointer BTreeNodeHeader) -> bool
  (define (internal-node? header)
    (bitwise-bit-set? (ftype-ref BTreeNodeHeader (flags) header) 0))

  ;;; (ftype-pointer BTreeNodeHeader) -> bool
  (define (leaf-node? header)
    (bitwise-bit-set? (ftype-ref BTreeNodeHeader (flags) header) 1))

  ;;; void* BTreeNodeHeader u64 -> integer
  ;;; Performs a binary search looking for the key and returns the index of the
  ;;; lower bound.
  (define (lower-bound b header key)
    (let ((keys (block->keys b)))
     (let loop ((lo 0)
                (hi (ftype-ref BTreeNodeHeader (nr-entries) header)))
      (if (<= (- hi lo) 1)
          lo
          (let* ((mid (+ lo (/ (- hi lo) 2)))
                 (k (key-at b mid)))
                (cond
                  ((= key k) mid)
                  ((< k key) (loop mid hi))
                  (else (loop lo mid))))))))

  ;;;;----------------------------------------------
  ;;;; Lookup
  ;;;;----------------------------------------------

  ;; FIXME: this holds more blocks than we need as we recurse,  use a fixed
  ;; size block queue.
  (define (btree-lookup tree key default)
    (let ((cache (btree-bcache tree))
          (vt (btree-value-type tree)))

      (let loop ((root (btree-root tree)))
           (with-block (b cache root (get-flags))
             (let* ((header (block->header b))
                    (keys (block->keys b))
                    (vals (block->values b vt))
                    (index (lower-bound b header key)))
                   (if (internal-node? header)
                       (loop (value-at le64-vt vals index))
                       (if (= key (key-at keys index))
                           (value-at vt vals index)
                           default)))))))

  ;;;;----------------------------------------------
  ;;;; Walking the btree
  ;;;;----------------------------------------------

  ;;; Calls (fn key value) on every entry of the btree.
  (define (btree-each tree fn)
    (let ((vt (btree-value-type tree))
          (cache (btree-bcache tree)))

         (define (visit-leaf nr-entries keys vals)
                 (let loop ((index 0))
                  (when (< index nr-entries)
                        (fn (key-at keys index) (value-at vt vals index))
                        (loop (+ 1 index)))))

         (define (visit-internal nr-entries keys vals)
                 (let loop ((index 0))
                  (when (< index nr-entries)
                        (visit-node (value-at le64-vt vals index))
                        (loop (+ 1 index)))))

         (define (visit-node root)
                 (with-block (b cache root (get-flags))
                   (let* ((header (block->header b))
                          (nr-entries (ftype-ref BTreeNodeHeader (nr-entries) header))
                          (keys (block->keys b))
                          (vals (block->values b vt)))
                         ((if (internal-node? header)
                              visit-internal
                              visit-leaf) nr-entries keys vals))))

         (visit-node (btree-root tree)))))

