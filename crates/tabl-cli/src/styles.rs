use toolstr::Colorize;
use toolstr_colored::ColoredString;

pub(crate) fn get_styles() -> clap::builder::Styles {
    let white = anstyle::Color::Rgb(anstyle::RgbColor(255, 255, 255));
    let green = anstyle::Color::Rgb(anstyle::RgbColor(0, 225, 0));
    let grey = anstyle::Color::Rgb(anstyle::RgbColor(170, 170, 170));
    let title = anstyle::Style::new().bold().fg_color(Some(green));
    let arg = anstyle::Style::new().bold().fg_color(Some(white));
    let comment = anstyle::Style::new().fg_color(Some(grey));
    clap::builder::Styles::styled()
        .header(title)
        .error(comment)
        .usage(title)
        .literal(arg)
        .placeholder(comment)
        .valid(title)
        .invalid(comment)
}

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

use inquire::ui::{Attributes, Color, IndexPrefix, RenderConfig, StyleSheet, Styled};

pub(crate) fn get_render_config() -> RenderConfig<'static> {
    let highlight_color = Color::DarkGreen;

    let mut render_config = RenderConfig::default();
    render_config.prompt = StyleSheet::new().with_attr(Attributes::BOLD);
    render_config.prompt_prefix = Styled::new("").with_fg(Color::LightRed);
    render_config.answered_prompt_prefix = Styled::new("").with_fg(Color::LightRed);
    render_config.placeholder = StyleSheet::new().with_fg(Color::LightRed);
    render_config.selected_option = Some(StyleSheet::new().with_fg(highlight_color));
    render_config.highlighted_option_prefix = Styled::new("→").with_fg(highlight_color);
    render_config.selected_checkbox = Styled::new("☑").with_fg(highlight_color);
    render_config.scroll_up_prefix = Styled::new("⇞");
    render_config.scroll_down_prefix = Styled::new("⇟");
    render_config.unselected_checkbox = Styled::new("☐");
    render_config.option_index_prefix = IndexPrefix::Simple;
    render_config.error_message = render_config
        .error_message
        .with_prefix(Styled::new("❌").with_fg(Color::LightRed));
    render_config.answer = StyleSheet::new()
        .with_attr(Attributes::BOLD)
        .with_fg(highlight_color);
    let grey = Color::Rgb {
        r: 100,
        g: 100,
        b: 100,
    };
    render_config.help_message = StyleSheet::new()
        .with_fg(grey)
        .with_attr(Attributes::ITALIC);

    render_config
}
