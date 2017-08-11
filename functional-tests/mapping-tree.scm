(library
  (mapping-tree)

  (export mapping-tree-open
          mapping-tree-lookup
          mapping-tree-each)

  (import (btree)
          (chezscheme)
          (binary-format))

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

  ;;; Visits every entry in the mapping tree calling (fn dev-id vblock mapping).
  (define (mapping-tree-each mtree fn)
    (let ((dev-tree (mapping-tree-dev-tree mtree)))

     (define (visit-dev dev-id mapping-root)
       (btree-each (btree-open le64-type (btree-dev dev-tree) mapping-root)
                   (lambda (vblock mapping)
                     (fn dev-id vblock mapping))))

     (btree-each dev-tree visit-dev))))
