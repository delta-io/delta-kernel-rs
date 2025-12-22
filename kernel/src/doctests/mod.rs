// Rustdoc's documentation tests can do some things that regular unit tests can't. Here we are
// using doctests to test macros. Specifically, we are testing for failed macro invocations due
// to invalid input, not the macro output when the macro invocation is successful (which can/should be
// done in unit tests). This module is not exclusively for macro tests only so other doctests can also be added.
// https://doc.rust-lang.org/rustdoc/write-documentation/documentation-tests.html#include-items-only-when-collecting-doctests

mod to_schema;

