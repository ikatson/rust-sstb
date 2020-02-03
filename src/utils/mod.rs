mod sorted_bytes_iterator;

pub use sorted_bytes_iterator::SortedBytesIterator;

pub fn get_current_pid_rss() -> usize {
    let pid = format!("{}", std::process::id());
    let out = std::process::Command::new("ps")
        .args(&["-p", &pid, "-o", "rss"])
        .output()
        .unwrap();
    let out = String::from_utf8(out.stdout).unwrap();
    let pid_line = out.lines().nth(1).unwrap();
    pid_line.trim().parse::<usize>().unwrap()
}
