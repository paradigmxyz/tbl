use colored::Colorize;

/// format bytes
pub fn format_bytes(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut size = bytes as f64;
    let mut unit = 0;

    while size >= 1024.0 && unit < units.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    format!("{:.2} {}", size, units[unit])
}

/// format number with commas
pub fn format_with_commas(number: u64) -> String {
    let num_str = number.to_string();
    let mut result = String::new();
    let mut count = 0;

    for c in num_str.chars().rev() {
        if count == 3 {
            result.push(',');
            count = 0;
        }
        result.push(c);
        count += 1;
    }

    result.chars().rev().collect()
}

const TITLE_R: u8 = 0;
const TITLE_G: u8 = 225;
const TITLE_B: u8 = 0;
const ERROR_R: u8 = 225;
const ERROR_G: u8 = 0;
const ERROR_B: u8 = 0;

/// print header
pub fn print_header<A: AsRef<str>>(header: A) {
    let header_str = header.as_ref().white().bold();
    let underline = "─"
        .repeat(header_str.len())
        .truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}", header_str);
    println!("{}", underline);
}

/// print header error
pub fn print_header_error<A: AsRef<str>>(header: A) {
    let header_str = header.as_ref().white().bold();
    let underline = "─"
        .repeat(header_str.len())
        .truecolor(ERROR_R, ERROR_G, ERROR_B);
    println!("{}", header_str);
    println!("{}", underline);
}

/// print bullet as `- key`
pub fn print_bullet_key<A: AsRef<str>>(key: A) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    println!("{}{}", bullet_str, key_str);
}

/// print bullet as `- key: value`
pub fn print_bullet<A: AsRef<str>, B: AsRef<str>>(key: A, value: B) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    let value_str = value.as_ref().truecolor(170, 170, 170);
    let colon_str = ": ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!("{}{}{}{}", bullet_str, key_str, colon_str, value_str);
}

/// print bullet as `- key (value)`
pub fn print_bullet_parenthetical<A: AsRef<str>, B: AsRef<str>>(key: A, value: B) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    let value_str = value.as_ref().truecolor(170, 170, 170);
    println!("{}{} ({})", bullet_str, key_str, value_str);
}

/// print bullet as `    - key: value`
pub fn print_bullet_indent<A: AsRef<str>, B: AsRef<str>>(key: A, value: B, indent: usize) {
    let bullet_str = "- ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    let key_str = key.as_ref().white().bold();
    let value_str = value.as_ref().truecolor(170, 170, 170);
    let colon_str = ": ".truecolor(TITLE_R, TITLE_G, TITLE_B);
    println!(
        "{}{}{}{}{}",
        " ".repeat(indent),
        bullet_str,
        key_str,
        colon_str,
        value_str
    );
}
