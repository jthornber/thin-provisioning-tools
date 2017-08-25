(library
  (scenario-string-constants)

  (export thin-check-help
          thin-restore-outfile-too-small-text
          thin-restore-help
          thin-rmap-help

          cache-check-help
          )

  (import (rnrs))

  ;; These long string constants really confuse vim and mess up Paredit mode.
  ;; So moving into a separate file.
  (define thin-check-help
    "Usage: thin_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--clear-needs-check-flag}
  {--ignore-non-fatal-errors}
  {--skip-mappings}
  {--super-block-only}")

  (define thin-restore-outfile-too-small-text
    "Output file too small.

The output file should either be a block device,
or an existing file.  The file needs to be large
enough to hold the metadata.")

  (define thin-restore-help
    "Usage: thin_restore [options]
Options:
  {-h|--help}
  {-i|--input} <input xml file>
  {-o|--output} <output device or file>
  {-q|--quiet}
  {-V|--version}")

  (define thin-rmap-help
    "Usage: thin_rmap [options] {device|file}
Options:
  {-h|--help}
  {-V|--version}
  {--region <block range>}*
Where:
  <block range> is of the form <begin>..<one-past-the-end>
  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45")

  (define cache-check-help
    "Usage: cache_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--clear-needs-check-flag}
  {--super-block-only}
  {--skip-mappings}
  {--skip-hints}
  {--skip-discards}")

)
