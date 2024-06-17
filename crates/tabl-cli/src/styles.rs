use toolstr::Colorize;
use toolstr_colored::ColoredString;

pub(crate) trait FontStyle {
    fn colorize_background(self) -> ColoredString;
    fn colorize_title(self) -> ColoredString;
    fn colorize_comment(self) -> ColoredString;
    fn colorize_string(self) -> ColoredString;
    fn colorize_constant(self) -> ColoredString;
    fn colorize_function(self) -> ColoredString;
    fn colorize_variable(self) -> ColoredString;
}

impl FontStyle for &str {
    fn colorize_background(self) -> ColoredString {
        self.truecolor(40, 42, 54)
    }

    fn colorize_title(self) -> ColoredString {
        self.truecolor(206, 147, 249).bold()
    }

    fn colorize_comment(self) -> ColoredString {
        self.truecolor(98, 114, 164)
    }

    fn colorize_string(self) -> ColoredString {
        self.truecolor(241, 250, 140)
    }

    fn colorize_constant(self) -> ColoredString {
        self.truecolor(185, 242, 159)
    }

    fn colorize_function(self) -> ColoredString {
        self.truecolor(139, 233, 253)
    }

    fn colorize_variable(self) -> ColoredString {
        self.truecolor(100, 170, 170)
    }
}
