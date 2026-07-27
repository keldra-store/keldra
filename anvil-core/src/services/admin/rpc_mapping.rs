use crate::system_realm::SystemAdminRelation;

pub fn admin_rpc_relation_mapping() -> &'static [(&'static str, SystemAdminRelation)] {
    &[
        ("CreateTenant", SystemAdminRelation::ManageTenants),
        ("CreateApplication", SystemAdminRelation::ManageApps),
        ("RotateApplicationSecret", SystemAdminRelation::ManageApps),
        (
            "GrantApplicationPolicy",
            SystemAdminRelation::ManagePolicies,
        ),
        (
            "RevokeApplicationPolicy",
            SystemAdminRelation::ManagePolicies,
        ),
        (
            "GrantApplicationPolicies",
            SystemAdminRelation::ManagePolicies,
        ),
        (
            "RevokeApplicationPolicies",
            SystemAdminRelation::ManagePolicies,
        ),
        (
            "RotateSecretEncryptionKey",
            SystemAdminRelation::ManageSecretEncryptionKeys,
        ),
        (
            "ImportPersonalDbSigningKey",
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        ),
        (
            "ListPersonalDbSigningKeys",
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        ),
        (
            "SetPersonalDbSigningKeyStatus",
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        ),
        ("CreateBucketAdmin", SystemAdminRelation::ManageBuckets),
        (
            "SetBucketPublicAccessAdmin",
            SystemAdminRelation::ManageBuckets,
        ),
        ("CreateHostAlias", SystemAdminRelation::ManageHostAliases),
        ("ActivateHostAlias", SystemAdminRelation::ManageHostAliases),
        ("SuspendHostAlias", SystemAdminRelation::ManageHostAliases),
        ("DeleteHostAlias", SystemAdminRelation::ManageHostAliases),
        ("ReadHostAlias", SystemAdminRelation::ManageHostAliases),
        ("ListHostAliases", SystemAdminRelation::ManageHostAliases),
        ("CreateRegion", SystemAdminRelation::ManageRegions),
        ("ActivateRegion", SystemAdminRelation::ManageRegions),
        ("SetRegionReadOnly", SystemAdminRelation::ManageRegions),
        ("DrainRegion", SystemAdminRelation::ManageRegions),
        ("RemoveRegion", SystemAdminRelation::ManageRegions),
        ("ListRegions", SystemAdminRelation::ManageRegions),
        ("RegisterCell", SystemAdminRelation::ManageRegions),
        ("ActivateCell", SystemAdminRelation::ManageRegions),
        ("DrainCell", SystemAdminRelation::ManageRegions),
        ("RemoveCell", SystemAdminRelation::ManageRegions),
        ("ListCells", SystemAdminRelation::ManageRegions),
        ("RegisterNode", SystemAdminRelation::ManageNodes),
        (
            "ReplaceClusterNodeIncarnation",
            SystemAdminRelation::ManageNodes,
        ),
        ("GetLocalNodeDescriptor", SystemAdminRelation::ManageNodes),
        ("ActivateNode", SystemAdminRelation::ManageNodes),
        ("DrainNode", SystemAdminRelation::ManageNodes),
        ("ForceOfflineNode", SystemAdminRelation::ManageNodes),
        ("RemoveNode", SystemAdminRelation::ManageNodes),
        ("ListNodes", SystemAdminRelation::ManageNodes),
        ("ListRoutingRecords", SystemAdminRelation::ManageRouting),
        ("RepairRoutingRecord", SystemAdminRelation::ManageRouting),
        ("RunRepair", SystemAdminRelation::RunRepair),
        ("ListDiagnostics", SystemAdminRelation::ViewDiagnostics),
        ("ListAuditEvents", SystemAdminRelation::ViewAuditLog),
        ("ListStorageClasses", SystemAdminRelation::ViewSystem),
        ("GetStorageClass", SystemAdminRelation::ViewSystem),
    ]
}
