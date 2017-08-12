(library
  (mapping-tree)

  (export mapping-tree-open
          mapping-tree-lookup
          mapping-tree-each)

  (import (btree)
          (chezscheme)
          (binary-format)
          (srfi s8 receive))

  (define-record-type mapping-tree (fields dev-tree))

  (define (mapping-tree-open dev root)
    (make-mapping-tree (btree-open le64-type dev root)))

  (define (mapping-tree-lookup mtree dev-id vblock default)
    (let* ((unique (gensym))
           (dev-tree (mapping-tree-dev-tree mtree))
           (root2 (btree-lookup dev-tree dev-id unique)))
     (if (eq? unique root2)
         default
         (btree-lookup (btree-open le64-type (btree-dev dev-tree) root2) vblock default))))

  ;; (values <block> <time>)
  (define time-mask (- (fxsll 1 24) 1))

  (define (unpack-block-time bt)
    (values (fxsrl bt 24) (fxlogand bt time-mask)))

  ;;; Visits every entry in the mapping tree calling (fn dev-id vblock pblock time).
  (define (mapping-tree-each mtree fn)
    (let ((dev-tree (mapping-tree-dev-tree mtree)))

     (define (visit-dev dev-id mapping-root)
       (btree-each (btree-open le64-type (btree-dev dev-tree) mapping-root)
                   (lambda (vblock mapping)
                     (receive (block time) (unpack-block-time mapping)
                              (fn dev-id vblock block time)))))

     (btree-each dev-tree visit-dev))))
