#![allow(dead_code, unused)]
mod sys;
mod e1;
mod e2;
mod e3;
mod e4;
mod e6;
mod util;
mod helpers;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 2 {
        eprintln!("usage: hsm <e1|e2|e3|e4|e6|h-*> ...");
        std::process::exit(2);
    }
    let rest: Vec<&str> = a[2..].iter().map(|s| s.as_str()).collect();
    match a[1].as_str() {
        "e1" => e1::run(&rest),
        "e2" => e2::run(&rest),
        "e3" => e3::run(&rest),
        "e4" => e4::run(&rest),
        "e6" => e6::run(&rest),
        s if s.starts_with("h-") => helpers::run(s, &rest),
        _ => {
            eprintln!("unknown subcommand");
            std::process::exit(2);
        }
    }
}
