(library
  (scenario-string-constants)

  (export thin-check-help
          thin-restore-outfile-too-small-text
          thin-restore-help
          thin-rmap-help
          thin-delta-help

          cache-check-help
          cache-restore-help
          cache-restore-outfile-too-small-text
          cache-dump-help
          cache-metadata-size-help

          era-check-help
          era-restore-help
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
  {--override-mapping-root}
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

  (define thin-delta-help
    "Usage: thin_delta [options] <device or file>
Options:
  {--thin1, --snap1}
  {--thin2, --snap2}
  {-m, --metadata-snap} [block#]
  {--verbose}
  {-h|--help}
  {-V|--version}")

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

  (define cache-restore-help
    "Usage: cache_restore [options]
Options:
  {-h|--help}
  {-i|--input} <input xml file>
  {-o|--output} <output device or file>
  {-q|--quiet}
  {--metadata-version} <1 or 2>
  {-V|--version}

  {--debug-override-metadata-version} <integer>
  {--omit-clean-shutdown}")

  (define cache-restore-outfile-too-small-text
    "Output file too small.

The output file should either be a block device,
or an existing file.  The file needs to be large
enough to hold the metadata.")

  (define cache-dump-help
    "Usage: cache_dump [options] {device|file}
Options:
  {-h|--help}
  {-o <xml file>}
  {-V|--version}
  {--repair}")

  (define cache-metadata-size-help
          "Usage: cache_metadata_size [options]
Options:
  {-h|--help}
  {-V|--version}
  {--block-size <sectors>}
  {--device-size <sectors>}
  {--nr-blocks <natural>}
  {--max-hint-width <nr bytes>}

These all relate to the size of the fast device (eg, SSD), rather
than the whole cached device.")

  (define era-check-help
    "Usage: era_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--super-block-only}")

  (define era-restore-help
    "Usage: era_restore [options]
Options:
  {-h|--help}
  {-i|--input} <input xml file>
  {-o|--output} <output device or file>
  {-q|--quiet}
  {-V|--version}")
)
