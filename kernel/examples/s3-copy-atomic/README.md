# S3 Copy Atomic Example

Integration test for `copy_atomic` functionality with AWS S3.

## Prerequisites

Create a private S3 bucket and get AWS credentials:

1. Go to https://console.aws.amazon.com/s3/
2. Create a bucket (or use existing)
3. Go to https://console.aws.amazon.com/iam/
4. Create access key: Your username → Security credentials → Create access key → CLI
5. Copy Access Key ID and Secret Access Key

## Running the Test

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key

# Navigate to example directory
cd examples/s3-copy-atomic

# Run the test
cargo run -- --bucket your-bucket-name --region us-east-1
```

Replace:

- `your-bucket-name` with your S3 bucket name
- `us-east-1` with your bucket's region

Delta-kernel-rs automatically uses the **header strategy** (`If-None-Match: *`) for atomic copy on S3. This works on AWS
S3 (August 2024+).

## What Gets Tested

1. Source file creation on S3
2. Atomic copy to destination using `copy_atomic`
3. Content verification (destination matches source)
4. Duplicate prevention (second copy fails with FileAlreadyExists)
5. Missing source error handling

All test files are automatically cleaned up.

## Expected Output

```
=== S3 copy_atomic Test ===
Bucket: your-bucket
Region: us-east-1

Test files:
  Source: copy-atomic-test/source-1234567890.txt
  Dest:   copy-atomic-test/dest-1234567890.txt

Test 1: Creating source file...
[OK] Source file created

Test 2: Copying to destination (should succeed)...
[OK] Copy succeeded

Test 3: Verifying destination content...
[OK] Destination content matches source

Test 4: Attempting duplicate copy (should fail)...
[OK] Correctly failed with FileAlreadyExists

Test 5: Copying from non-existent source (should fail)...
[OK] Correctly failed when source doesn't exist

Cleanup: Deleting test files...
[OK] Test files deleted

=== All Tests Passed! ===

Summary:
  [OK] Source file creation
  [OK] Atomic copy to destination
  [OK] Content verification
  [OK] Duplicate copy prevention (atomic if-not-exists)
  [OK] Missing source error handling

The copy_atomic implementation works correctly on S3!
```
