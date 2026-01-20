#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ordering {
    Any,
    Sorted(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Distribution {
    Any,
    Single,
    Hash(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct PhysicalProperties {
    pub ordering: Ordering,
    pub distribution: Distribution,
}

impl Default for PhysicalProperties {
    fn default() -> Self {
        Self {
            ordering: Ordering::Any,
            distribution: Distribution::Any,
        }
    }
}
