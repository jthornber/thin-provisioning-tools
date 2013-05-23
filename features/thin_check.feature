Feature: thin_check
  Scenario: print version (-V flag)
    When I run `thin_check -V`
    Then it should pass with: 

    """
    0.1.5
    """

  Scenario: print version (--version flag)
    When I run `thin_check --version`
    Then it should pass with: 

    """
    0.1.5
    """

  Scenario: print help
    When I run `thin_check --help`
    Then it should pass with:

    """
    Usage: thin_check [options] {device|file}
    Options:
      {-q|--quiet}
      {-h|--help}
      {-V|--version}
    """

  Scenario: print help
    When I run `thin_check -h`
    Then it should pass with:

    """
    Usage: thin_check [options] {device|file}
    Options:
      {-q|--quiet}
      {-h|--help}
      {-V|--version}
      {--super-block-only}
      {--skip-mappings}
      {--ignore-non-fatal-errors}
    """

  Scenario: Unrecognised option should cause failure
    When I run `thin_check --hedeghogs-only`
    Then it should fail

  Scenario: --super-block-only check passes on valid metadata
    Given valid metadata
    When I run thin_check with --super-block-only
    Then it should pass

  Scenario: --super-block-only check fails with corrupt superblock
    Given a corrupt superblock
    When I run thin_check with --super-block-only
    Then it should fail with:
    """
    examining superblock
      superblock is corrupt
        bad checksum in superblock
    """

  Scenario: --skip-mappings check passes on valid metadata
    Given valid metadata
    When I run thin_check with --skip-mappings
    Then it should pass

  Scenario: --ignore-non-fatal-errors check passes on valid metadata
    Given valid metadata
    When I run thin_check with --ignore-non-fatal-errors
    Then it should pass