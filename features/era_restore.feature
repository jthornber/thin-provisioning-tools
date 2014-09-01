Feature: era_restore
  Scenario: print version (-V flag)
    When I run era_restore with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run era_restore with --version
    Then it should pass with version

  Scenario: print help (-h)
    When I run era_restore with -h
    Then it should pass
    And the output should contain exactly:

    """
    Usage: era_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-q|--quiet}
      {-V|--version}

    """

  Scenario: print help (--help)
    When I run era_restore with -h
    Then it should pass
    And the output should contain exactly:

    """
    Usage: era_restore [options]
    Options:
      {-h|--help}
      {-i|--input} <input xml file>
      {-o|--output} <output device or file>
      {-q|--quiet}
      {-V|--version}

    """

  Scenario: missing input file
    Given the dev file metadata.bin
    When I run era_restore with -o metadata.bin
    Then it should fail with:
    """
    No input file provided.
    """

  Scenario: input file not found
    Given the dev file metadata.bin
    When I run era_restore with -i foo.xml -o metadata.bin
    Then it should fail

  Scenario: missing output file
    When I run era_restore with -i metadata.xml
    Then it should fail with:
    """
    No output file provided.
    """

  Scenario: successfully restores a valid xml file
    Given a small era xml file
    And an empty dev file
    When I run era_restore with -i metadata.xml -o metadata.bin
    Then it should pass
    And the metadata should be valid
    
  Scenario: --quiet is accepted
    Given valid era metadata
    When I run era_restore with -i metadata.xml -o metadata.bin --quiet
    Then it should pass
    And the output should contain exactly:
    """
    """

  Scenario: -q is accepted
    Given valid era metadata
    When I run era_restore with -i metadata.xml -o metadata.bin -q
    Then it should pass
    And the output should contain exactly:
    """
    """

  Scenario: dump/restore is a noop
    Given valid era metadata
    When I era dump
    And I era restore
    And I era dump
    Then dumps 1 and 2 should be identical

  Scenario: dump matches original metadata
    Given valid era metadata
    When I era dump
    Then dumps 0 and 1 should be identical
