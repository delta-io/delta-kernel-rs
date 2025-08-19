#[macro_export]
macro_rules! sort_lines {
    ($lines: expr) => {{
        // sort except for header + footer
        let num_lines = $lines.len();
        if num_lines > 3 {
            $lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }
    }};
}

// NB: expected_lines_sorted MUST be pre-sorted (via sort_lines!())
#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($expected_lines_sorted: expr, $CHUNKS: expr) => {
        let formatted = delta_kernel::arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();
        // fix for windows: \r\n -->
        let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();
        sort_lines!(actual_lines);
        assert_eq!(
            $expected_lines_sorted, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            $expected_lines_sorted, actual_lines
        );
    };
}
