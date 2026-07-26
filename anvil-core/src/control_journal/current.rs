use super::*;

#[derive(Debug, Clone)]
pub struct CurrentAppPage {
    pub apps: Vec<App>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct CurrentRegionPage {
    pub regions: Vec<String>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct CurrentTenantPage {
    pub tenants: Vec<Tenant>,
    pub next_tuple_key: Option<Vec<u8>>,
}
