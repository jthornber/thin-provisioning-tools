(library
  (crc32c checksum)

  (export crc32c)

  (import (chezscheme))

  (define __ (load-shared-object "../lib/libft.so"))

  (define crc32c
          (foreign-procedure "crc32c" (unsigned-32 (* unsigned-8) unsigned) unsigned-32)))
