#[cfg(feature = "velox")]
fn main() {
    use chryso::VeloxAdapter;
    use chryso::adapter::ExecutorAdapter;
    use chryso_planner::PhysicalPlan;
    use std::fs;

    let adapter = VeloxAdapter::try_new().expect("adapter");
    let plan = PhysicalPlan::TableScan {
        table: "demo_table".to_string(),
    };
    let result = adapter.execute(&plan).expect("execute");
    println!("{:?}", result.columns);
    println!("{:?}", result.rows);

    let arrow = adapter.execute_arrow(&plan).expect("execute_arrow");
    fs::write("velox_demo.arrow", &arrow).expect("write arrow");
    println!("arrow_bytes={}", arrow.len());
}

#[cfg(not(feature = "velox"))]
fn main() {
    eprintln!("velox feature not enabled");
}
