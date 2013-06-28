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
      {-V|--version}
    """

  Scenario: missing input file
    When I run thin_restore with -o metadata.bin
    Then it should fail with:
    """
    No input file provided.
    """

  Scenario: dump/restore is a noop
    Given valid metadata
    When I dump
    And I restore
    And I dump
    Then dumps 0 and 1 should be identical