(library
  (temp-file)

  (export
    working-directory
    with-dir-thunk
    with-dir
    with-temp-file-thunk
    with-temp-file-containing-thunk
    with-temp-file-sized-thunk
    with-temp-file
    with-temp-file-containing
    with-temp-file-sized
    disable-unlink)

  (import (chezscheme)
          (fmt fmt))

  ;; FIXME: global var!  Not thread safe.
  (define working-dir "/tmp")

  (define (working-directory) working-dir)

  (define (mkdir-p)
    (system (string-append "mkdir -p " working-dir)))

  (define (with-dir-thunk path thunk)
    (fluid-let ((working-dir (string-append working-dir "/" path)))
               (mkdir-p)
               (thunk)))

  (define-syntax with-dir
    (syntax-rules ()
      ((_ path b1 b2 ...)
       (with-dir-thunk path (lambda () b1 b2 ...)))))

  (define (with-temp-dir-thunk path thunk)
    (with-dir-thunk path
                    (lambda ()
                      (auto-unlink-file path
                                   (thunk)))))

   (define temp-filename
     (let ((counter 0))
      (lambda ()
        (let loop ()
         (let ((path (fmt #f (cat (dsp working-dir) (dsp "/tmp-")
                                  (pad-char #\0 (pad/left 4 (num counter)))))))
           (set! counter (+ counter 1))
           (if (file-exists? path)
               (loop)
               path))))))

   ;; fn takes the path
   (define (with-temp-file-thunk fn)
     (let ((path (temp-filename)))
      (auto-unlink-file path
        (lambda () (fn path)))))

   (define-syntax with-temp-file
     (syntax-rules ()
       ((_ (v) b1 b2 ...)
        (with-temp-file-thunk
          (lambda (v)
            b1 b2 ...)))

       ((_ (v1 v2 ...) b1 b2 ...)
        (with-temp-file-thunk
          (lambda (v1)
            (with-temp-file (v2 ...) b1 b2 ...))))))

   ;; Creates a temporary file with the specified contents.
   (define (with-temp-file-containing-thunk contents fn)
     (with-temp-file-thunk
       (lambda (path)
         (with-output-to-file path (lambda ()
                                     (put-string (current-output-port) contents)))
         (fn path))))

   (define-syntax with-temp-file-containing
     (syntax-rules ()
       ((_ ((v txt)) b1 b2 ...)
        (with-temp-file-containing-thunk
          txt (lambda (v) b1 b2 ...)))

       ((_ ((v txt) rest ...) b1 b2 ...)
        (with-temp-file-containing-thunk
          txt (lambda (v txt)
                (with-temp-file-containing (rest ...)
                                           b1 b2 ...))))))

   (define (with-temp-file-sized-thunk size fn)
     (with-temp-file-thunk
       (lambda (path)
         (let ((cmd (fmt #f (dsp "fallocate -l ") (wrt size) (dsp " ") (dsp path))))
          (system cmd)
          (fn path)))))

   (define-syntax with-temp-file-sized
     (syntax-rules ()
       ((_ ((v size)) b1 b2 ...)
        (with-temp-file-sized-thunk
          size
          (lambda (v)
            b1 b2 ...)))

       ((_ ((v size) rest ...) b1 b2 ...)
        (with-temp-file-sized-thunk
          size (lambda (v)
                 (with-temp-file-sized (rest ...) b1 b2 ...))))))

   ;;-------------------------

   (define should-unlink #t)

   (define (disable-unlink-thunk fn)
     (fluid-let ((should-unlink #f))
                (fn)))

   (define-syntax disable-unlink
     (syntax-rules ()
       ((_ b1 b2 ...)
        (disable-unlink-thunk
          (lambda () b1 b2 ...)))))

   ;; FIXME: use 'run' so we get logging
   (define (unlink-file path)
     (when should-unlink
       (system (string-append "rm -f " path))))

   (define (unlink-dir path)
     (when should-unlink
       (system (string-append "rmdir -f " path))))

   (define (auto-unlink-file path thunk)
     (dynamic-wind (lambda () #t)
                   thunk
                   (lambda () (unlink-file path))))

   (define (auto-unlink-dir path thunk)
     (dynamic-wind (lambda () #t)
                   thunk
                   (lambda () (unlink-dir path))))
   )
