Feature: thin_restore
  Scenario: print version (-V flag)
    When I run thin_restore with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run thin_restore with --version
    Then it should pass with version

  Scenario: print help (-h)
    When I run thin_restore with -h
    Then it should pass with:

    """
    Usage: thin_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-q|--quiet}
      {-V|--version}
    """

  Scenario: print help (--help)
    When I run thin_restore with -h
    Then it should pass with:

    """
    Usage: thin_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-q|--quiet}
      {-V|--version}
    """

  Scenario: missing input file
    Given the dev file metadata.bin
    When I run thin_restore with -o metadata.bin
    Then it should fail with:
    """
    No input file provided.
    """

  Scenario: input file not found
    Given the dev file metadata.bin
    When I run thin_restore with -i foo.xml -o metadata.bin
    Then it should fail

  Scenario: missing output file
    When I run thin_restore with -i metadata.xml
    Then it should fail with:
    """
    No output file provided.
    """

  Scenario: --quiet is accepted
    Given valid thin metadata
    When I run thin_restore with -i metadata.xml -o metadata.bin --quiet
    Then it should pass
    And the output should contain exactly:
    """
    """

  Scenario: -q is accepted
    Given valid thin metadata
    When I run thin_restore with -i metadata.xml -o metadata.bin -q
    Then it should pass
    And the output should contain exactly:
    """
    """

  Scenario: dump/restore is a noop
    Given valid thin metadata
    When I dump
    And I restore
    And I dump
    Then dumps 1 and 2 should be identical

  Scenario: dump matches original metadata
    Given valid thin metadata
    When I dump
    Then dumps 0 and 1 should be identical

  Scenario: dump matches original metadata (small)
    Given small thin metadata
    When I dump
    Then dumps 0 and 1 should be identical
