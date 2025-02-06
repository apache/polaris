# Catalog Serialization Tests

This directory contains test cases for validating the serialization and deserialization of Catalog objects in the Polaris management model.

## Test Coverage

### Basic Functionality
- `testCatalogSerialization`: Verifies proper serialization of a Catalog object to JSON
- `testCatalogDeserialization`: Ensures correct deserialization from JSON to Catalog object

### Edge Cases
- `testCatalogWithNullFields`: Validates handling of null field values
- `testCatalogWithEmptyFields`: Tests behavior with empty string values
- `testCatalogWithEmptyStrings`: Verifies consistent handling of empty strings
- `testLongCatalogName`: Ensures support for very long catalog names
- `testWhitespaceHandling`: Validates preservation of whitespace in strings

### Error Handling
- `testInvalidJsonDeserialization`: Verifies proper error handling for invalid JSON
- `testInvalidEnumValue`: Tests rejection of invalid enum values
- `testMalformedJson`: Ensures proper handling of malformed JSON input

### Special Cases
- `testSpecialCharacters`: Validates handling of special characters in strings
- `testUnicodeCharacters`: Ensures proper support for Unicode characters
- `testRoleArnValidation`: Verifies validation of AWS Role ARN formats

## Setup

The tests use Jackson ObjectMapper with the following configuration:
- Fails silently on unknown properties
- Uses strict enum parsing

## Test Constants
- Test Location: `s3://test/`
- Test Catalog Name: `test-catalog`
- Test Role ARN: `arn:aws:iam::123456789012:role/test-role`
