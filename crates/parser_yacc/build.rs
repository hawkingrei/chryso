fn main() {
    lrlex::process_root().expect("lrlex");
    lrpar::process_root().expect("lrpar");
}
