//! Error types + shared helpers.
//! 错误类型和共享辅助函数。

use thiserror::Error;

/// Top-level error categories for the probe.
/// 探针的顶层错误类别。
///
/// 注:实际代码多用 `anyhow::Result` 短路,本类型用于需要分类处理的场合
/// (日志、metrics、退出码)。
/// Most code uses `anyhow::Result`; these categories support classified logs, metrics, and exit codes.
#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Round a float to 2 decimal places for stable JSON output.
/// 将浮点数四舍五入到两位小数，使 JSON 输出稳定。
pub fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/** Bound text by the receiver's UTF-16 length. / 按接收端的 UTF-16 长度限制文本。 */
pub fn bounded_text(value: &str, limit: usize) -> String {
    let mut length = 0;
    value.chars().take_while(|character| { length += character.len_utf16(); length <= limit }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round2_keeps_two_dp() {
        assert_eq!(round2(23.456), 23.46);
        assert_eq!(round2(0.001), 0.0);
        assert_eq!(round2(99.999), 100.0);
    }
}
