Feature: thin_check
  Scenario: print version (-V flag)
    When I run `thin_check -V`
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run `thin_check --version`
    Then it should pass with version

  Scenario: print help
    When I run `thin_check --help`
    Then it should pass with:

    """
    Usage: thin_check [options] {device|file}
    Options:
      {-q|--quiet}
      {-h|--help}
      {-V|--version}
      {--clear-needs-check-flag}
      {--ignore-non-fatal-errors}
      {--skip-mappings}
      {--super-block-only}
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
      {--clear-needs-check-flag}
      {--ignore-non-fatal-errors}
      {--skip-mappings}
      {--super-block-only}
    """

  Scenario: Unrecognised option should cause failure
    When I run `thin_check --hedeghogs-only`
    Then it should fail

  Scenario: --super-block-only check passes on valid metadata
    Given valid thin metadata
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
    Given valid thin metadata
    When I run thin_check with --skip-mappings
    Then it should pass

  Scenario: --ignore-non-fatal-errors check passes on valid metadata
    Given valid thin metadata
    When I run thin_check with --ignore-non-fatal-errors
    Then it should pass

  Scenario: -q should give no output
    Given a corrupt superblock
    When I run thin_check with --quiet
    Then it should fail
    And it should give no output

  Scenario: --quiet should give no output
    Given a corrupt superblock
    When I run thin_check with --quiet
    Then it should fail
    And it should give no output

  Scenario: Accepts --clear-needs-check-flag
    Given valid thin metadata
    When I run thin_check with --clear-needs-check-flag
    Then it should pass
