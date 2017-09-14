(library
  (thin mapping-tree)

  (export mapping-tree-lookup
          mapping-tree-each)

  (import (persistent-data btree)
          (bcache block-manager)
          (chezscheme)
          (srfi s8 receive))

  ;; (values <block> <time>)
  (define time-mask (- (fxsll 1 24) 1))

  (define (unpack-block-time bt)
    (values (fxsrl bt 24) (fxlogand bt time-mask)))

  ;; FIXME: unpack the block time
  (define (mapping-tree-lookup cache root dev-id vblock default)
    (with-spine (sp cache 1)
      (let* ((unique (gensym))
             (dev-tree (btree-open le64-vt root))
             (root2 (btree-lookup dev-tree sp dev-id unique)))
            (if (eq? unique root2)
                default
                (btree-lookup (btree-open le64-vt root2) sp vblock default)))))

  ;;; Visits every entry in the mapping tree calling (fn dev-id vblock pblock time).
  (define (mapping-tree-each cache root fn)
          (with-spine (sp cache 1)
            (let ((dev-tree (btree-open le64-vt root)))

             (define (visit-dev dev-id mapping-root)
                     (btree-each (btree-open le64-vt mapping-root)
                                 (lambda (vblock mapping)
                                   (receive (block time) (unpack-block-time mapping)
                                     (fn dev-id vblock block time)))))

             (btree-each dev-tree sp visit-dev))))
  )
