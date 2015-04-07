Feature: thin_rmap
  Scenario: print version (-V flag)
    When I run `thin_rmap -V`
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run `thin_rmap --version`
    Then it should pass with version

  Scenario: print help
    When I run `thin_rmap --help`
    Then it should pass with:

    """
    Usage: thin_rmap [options] {device|file}
    Options:
      {-h|--help}
      {-V|--version}
      {--region <block range>}*
    Where:
      <block range> is of the form <begin>..<one-past-the-end>
      for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45
    """

  Scenario: print help
    When I run `thin_rmap -h`
    Then it should pass with:
    """
    Usage: thin_rmap [options] {device|file}
    Options:
      {-h|--help}
      {-V|--version}
      {--region <block range>}*
    Where:
      <block range> is of the form <begin>..<one-past-the-end>
      for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45
    """

  Scenario: Unrecognised option should cause failure
    When I run `thin_rmap --unleash-the-hedeghogs`
    Then it should fail

  @announce
  Scenario: Valid region format should pass
    Given valid thin metadata
    When I run thin_rmap with --region 23..7890
    Then it should pass

  Scenario: Invalid region format should fail (comma instean of dots)
    Given valid thin metadata
    When I run thin_rmap with --region 23,7890
    Then it should fail

  Scenario: Invalid region format should fail (second number a word)
    Given valid thin metadata
    When I run thin_rmap with --region 23..six
    Then it should fail

  Scenario: Invalid region format should fail (first number a word)
    Given valid thin metadata
    When I run thin_rmap with --region four..7890
    Then it should fail

  Scenario: Invalid region format should fail (end is lower than begin)
    Given valid thin metadata
    When I run thin_rmap with --region 89..88
    Then it should fail

  Scenario: Invalid region format should fail (end is equal to begin)
    Given valid thin metadata
    When I run thin_rmap with --region 89..89
    Then it should fail

  Scenario: Invalid region format should fail (no begin)
    Given valid thin metadata
    When I run thin_rmap with --region ..89
    Then it should fail

  Scenario: Invalid region format should fail (no end)
    Given valid thin metadata
    When I run thin_rmap with --region 89..
    Then it should fail

  Scenario: Invalid region format should fail (no region at all)
    Given valid thin metadata
    When I run thin_rmap with --region
    Then it should fail

  Scenario: Invalid region format should fail (three dots)
    Given valid thin metadata
    When I run thin_rmap with --region 89...99
    Then it should fail

  Scenario: Multiple regions should pass
    Given valid thin metadata
    When I run thin_rmap with --region 1..23 --region 45..78
    Then it should pass
