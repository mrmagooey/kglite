"""Regression tests from AD_Miner Cypher queries.

AD_Miner (https://github.com/AD-Security/AD_Miner) is a popular BloodHound data
analysis tool. These tests verify that kglite can parse and execute the Cypher
queries it uses. Queries extracted from:
https://github.com/AD-Security/AD_Miner/blob/main/ad_miner/sources/modules/requests.json

Queries are organized by execution phase:
  Phase 0: Cleanup/setup (DETACH DELETE, REMOVE)
  Phase 1: SET queries that create temporary properties (dependency-ordered)
  Phase 2: Read queries that consume those properties
  GDS: Graph Data Science queries (skipped, not supported by kglite)
"""

import pytest

import kglite as rg


# ---------------------------------------------------------------------------
# AD_Miner template variable substitution
# ---------------------------------------------------------------------------
# AD_Miner replaces these placeholders at runtime before sending queries to
# Neo4j.  We substitute sensible defaults so kglite can parse and execute them.


def substitute_adminer_templates(query: str) -> str:
    """Replace AD_Miner runtime template placeholders with concrete values.

    Template variables ($var$):
      - $extract_date$: Unix timestamp for "now" (used in date arithmetic)
      - $password_renewal$: days threshold for password renewal checks
      - $properties$: relationship type(s) for variable-length path traversal
      - $path_to_group_operators_props$: relationship types for operator paths
      - $recursive_level$: max depth for variable-length path traversal

    Pagination placeholders:
      - SKIP PARAM1 / LIMIT PARAM2: pagination with integer defaults
    """
    replacements = {
        "$extract_date$": "1711929600",  # 2024-04-01 Unix epoch
        "$password_renewal$": "90",  # 90-day threshold
        "$properties$": "MemberOf|AdminTo|GenericAll|GenericWrite|Owns|WriteDacl|WriteOwner",
        "$path_to_group_operators_props$": "MemberOf|AdminTo|GenericAll|GenericWrite|Owns|WriteDacl|WriteOwner",
        "$recursive_level$": "5",  # max traversal depth
    }
    for template, value in replacements.items():
        query = query.replace(template, value)
    # Handle SKIP/LIMIT pagination placeholders
    query = query.replace("SKIP PARAM1", "SKIP 0")
    query = query.replace("LIMIT PARAM2", "LIMIT 100")
    return query


# ---------------------------------------------------------------------------
# All AD_Miner queries with metadata
# ---------------------------------------------------------------------------

ADMINER_QUERIES = {
    "check_if_GDS_installed": {
        "name": "Checking if Graph Data Science neo4j plugin is installed",
        "request": "SHOW PROCEDURES YIELD name RETURN 'gds.graph.project' IN COLLECT(name) AND 'gds.shortestPath.dijkstra.stream' IN COLLECT(name) as gds_installed",
        "is_write": False,
        "is_gds": False,
        "unsupported_features": ["SHOW PROCEDURES", "GDS (Graph Data Science)"],
    },
    "delete_orphans": {
        "name": "Delete orphan objects that have no labels",
        "request": 'MATCH (n) WHERE labels(n) = ["Base"] OR labels(n) = ["AZBase"] OR labels(n) = ["Base", "AZBase"] DETACH DELETE n',
        "is_write": True,
        "is_gds": False,
    },
    "preparation_request_nodes": {
        "name": "Clean AD Miner custom attributes",
        "request": "MATCH (n) REMOVE n.is_server,n.is_dc,n.is_da,n.is_dag,n.can_dcsync,n.path_candidate,n.ou_candidate,n.contains_da_dc,n.is_da_dc,n.ghost_computer,n.has_path_to_da,n.is_admin,n.is_group_operator,n.members_count,n.has_members,n.user_members_count,n.is_operator_member,n.is_group_account_operator,n.is_group_backup_operator,n.is_group_server_operator,n.is_group_print_operator,n.is_account_operator,n.is_backup_operator,n.is_server_operator,n.is_print_operator,n.gpolinks_count,n.has_links,n.dangerous_inbound, n.is_adminsdholder,n.is_dnsadmin,n.da_types,n.vulnerable_ou,n.can_abuse_adcs,n.dac,n.dac_types,n.is_adcs,n.target_kud,n.is_gag,n.is_msol,n.is_rbcd_target,n.is_dcg,n.esc7",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "azure_set_gag",
            "graph_rbcd",
            "objects_to_domain_admin",
            "set_containsda",
            "set_containsdc",
            "set_da",
            "set_da_types",
            "set_dac_types",
            "set_dag",
            "set_dag_types",
            "set_dagg",
            "set_dagg_types",
            "set_daggg",
            "set_dc",
            "set_dcg",
            "set_dcsync1",
            "set_dcsync2",
            "set_ghost_computer",
            "set_gpo_links_count",
            "set_gpos_has_links",
            "set_groups_direct_admin",
            "set_groups_has_members",
            "set_groups_indirect_admin_1",
            "set_groups_indirect_admin_2",
            "set_groups_indirect_admin_3",
            "set_groups_indirect_admin_4",
            "set_groups_members_count",
            "set_groups_members_count_computers",
            "set_is_adcs",
            "set_is_adminsdholder",
            "set_is_da_dc",
            "set_is_dnsadmin",
            "set_is_group_operator",
            "set_is_not_da_dc",
            "set_is_operator_member",
            "set_msol",
            "set_non_server",
            "set_nonda",
            "set_nondag",
            "set_nondc",
            "set_nondcg",
            "set_ou_candidate",
            "set_path_candidate",
            "set_server",
            "set_target_kud",
            "set_user_indirect_admin",
            "set_users_direct_admin",
        ],
    },
    "delete_unresolved": {
        "name": "Delete objects for which SID could not resolved",
        "request": "MATCH (n) WHERE ((n.domain IS NULL AND NOT (n:Domain)) OR n.name IS NULL) AND n.tenantid IS NULL DETACH DELETE n",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "check_relation_types": {
        "name": "Checking relation types",
        "request": "MATCH ()-[r]->() RETURN DISTINCT type(r) as relationType",
        "is_write": False,
        "is_gds": False,
    },
    "set_upper_domain_name": {
        "name": "Set domain names to upper case when not the case",
        "request": "MATCH (g) where g.domain <> toUpper(g.domain) SET g.domain=toUpper(g.domain)",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["check_if_all_group_objects_have_domain_attribute", "set_domain_attributes_to_domains"],
    },
    "set_domain_attributes_to_domains": {
        "name": "Set domain attributes to domain objects when not the case",
        "request": "MATCH (d:Domain) where d.domain IS NULL SET d.domain = toUpper(d.name)",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["check_if_all_group_objects_have_domain_attribute", "set_upper_domain_name"],
    },
    "check_if_all_domain_objects_exist": {
        "name": "Check for unexisting domain objects",
        "request": "MATCH (d:Domain) WITH DISTINCT d.domain AS domain WITH COLLECT(domain) AS domains MATCH (o) WHERE NOT o.domain IN domains RETURN count(o)",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "check_if_all_group_objects_have_domain_attribute": {
        "name": "Check for Group objects without domain attribute",
        "request": 'MATCH (g:Group) WHERE g.domain <> split(g.name, "@")[-1] SET g.domain=split(g.name, "@")[-1]',
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "set_domain_attributes_to_domains", "set_upper_domain_name"],
    },
    "preparation_request_relations": {
        "name": "Clean AD Miner custom relations",
        "request": "MATCH (g:Group)-[r:CanExtractDCSecrets|CanLoadCode|CanLogOnLocallyOnDC]->(c:Computer) DELETE r",
        "is_write": True,
        "is_gds": False,
    },
    "set_server": {
        "name": "Set is_server=TRUE to computers for which operatingsystem contains Server)",
        "request": 'MATCH (c:Computer)  WHERE toUpper(c.operatingsystem) CONTAINS "SERVER" SET c.is_server=TRUE',
        "is_write": True,
        "is_gds": False,
    },
    "set_non_server": {
        "name": "Set is_server=FALSE to other computers )",
        "request": "MATCH (c:Computer) WHERE c.is_server IS NULL  SET c.is_server=FALSE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_server"],
    },
    "set_dc": {
        "name": "Set dc=TRUE to computers that are domain controllers)",
        "request": 'MATCH (c:Computer)-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-516" OR g.objectid ENDS WITH "-521" SET c.is_dc=TRUE',
        "is_write": True,
        "is_gds": False,
    },
    "set_nondc": {
        "name": "Set dc=FALSE to computers that are not domain controllers)",
        "request": "MATCH (c:Computer) WHERE c.is_dc IS NULL SET c.is_dc=FALSE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_dc"],
    },
    "set_dcg": {
        "name": "Set is_dcg=TRUE to domain controllers groups",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-516" OR g.objectid ENDS WITH "-521" SET g.is_dcg=TRUE',
        "is_write": True,
        "is_gds": False,
    },
    "set_nondcg": {
        "name": "Set is_dcg=TRUE to domain controllers groups",
        "request": "MATCH (g:Group) WHERE g.is_dcg IS NULL SET g.is_dcg=FALSE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_dcg"],
    },
    "set_isacl_adcs": {
        "name": "Set isacl to TRUE for ADCS privilege escalation paths (ADCSESCxx)",
        "request": "MATCH (u)-[r]->(g) WHERE r.isacl IS NULL AND type(r) CONTAINS 'ADCSESC' SET r.isacl=TRUE",
        "is_write": True,
        "is_gds": False,
    },
    "onpremid_ompremsesecurityidentifier": {
        "name": "Setting onpremid in case of old collector",
        "request": "MATCH (a)  WHERE NOT a.onpremisesecurityidentifier IS NULL set a.onpremid=a.onpremisesecurityidentifier",
        "is_write": True,
        "is_gds": False,
    },
    "set_can_extract_dc_secrets": {
        "name": "ADD CanExtractDCSecrets relation from BACKUP OPERATORS OR SERVER OPERATORS groups to DCs of same domain",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-551" OR g.objectid ENDS WITH "-549" MATCH (c:Computer{is_dc:true}) WHERE g.domain = c.domain MERGE (g)-[:CanExtractDCSecrets]->(c)',
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "set_is_adminsdholder": {
        "name": "Set is_adminsdholder to Container with AdminSDHOLDER in name",
        "request": 'MATCH (c:Container) WHERE c.name STARTS WITH "ADMINSDHOLDER@" SET c.is_adminsdholder=true',
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_is_dnsadmin": {
        "name": "Set is_dnsadmin to Group with DNSAdmins in name",
        "request": 'MATCH (g:Group) WHERE g.name STARTS WITH "DNSADMINS@" SET g.is_dnsadmin=true',
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_can_load_code": {
        "name": "ADD CanLoadCode relation from PRINT OPERATORS groups to DCs of same domain",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-550" MATCH (c:Computer{is_dc:true}) WHERE g.domain = c.domain MERGE (g)-[:CanLoadCode]->(c)',
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "set_can_logon_dc": {
        "name": "ADD CanLogOnLocallyOnDC relation from ACCOUNT OPERATORS groups to DCs of same domain",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-548" MATCH (c:Computer{is_dc:true}) WHERE g.domain = c.domain MERGE (g)-[:CanLogOnLocallyOnDC]->(c)',
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "set_da": {
        "name": "Set da=TRUE to users that are domain admins or administrators or enterprise admin",
        "request": 'MATCH (c:User)-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" SET c.is_da=TRUE, c.da_types=[]',
        "is_write": True,
        "is_gds": False,
    },
    "set_msol": {
        "name": "Set is_da=TRUE and is_msol=TRUE to accounts associated with Microsoft Online Services",
        "request": "MATCH (c:User) where c.name STARTS WITH 'MSOL_' SET c.is_da=TRUE, c.is_msol=true",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_da_types": {
        "name": "Set the da type (domain, enterprise, key or builtin)",
        "request": 'MATCH (c:User)-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-525" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" WITH c,g, CASE WHEN g.objectid ENDS WITH "-512" THEN "Domain Admin" WHEN g.objectid ENDS WITH "-518" THEN "Schema Admin" WHEN g.objectid ENDS WITH "-519" THEN "Enterprise Admin" WHEN g.objectid ENDS WITH "-525" THEN "Protected Users" WHEN g.objectid ENDS WITH "-526" THEN "_ Key Admin" WHEN g.objectid ENDS WITH "-527" THEN "Enterprise Key Admin" WHEN g.objectid ENDS WITH "-544" THEN "Builtin Administrator" ELSE null END AS da_type SET c.da_types = c.da_types + da_type',
        "is_write": True,
        "is_gds": False,
    },
    "set_dag": {
        "name": "Set da=TRUE to groups that are domain admins or administrators or enterprise admin",
        "request": 'MATCH (c:Group)-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" SET c.is_da=TRUE',
        "is_write": True,
        "is_gds": False,
    },
    "set_dag_types": {
        "name": "Set the da type (domain, enterprise, key or builtin)",
        "request": 'MATCH (c:Group)-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-525" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" WITH c,g, CASE WHEN g.objectid ENDS WITH "-512" THEN "Domain Admin" WHEN g.objectid ENDS WITH "-518" THEN "Schema Admin" WHEN g.objectid ENDS WITH "-519" THEN "Enterprise Admin" WHEN g.objectid ENDS WITH "-525" THEN "Protected Users" WHEN g.objectid ENDS WITH "-526" THEN "_ Key Admin" WHEN g.objectid ENDS WITH "-527" THEN "Enterprise Key Admin" WHEN g.objectid ENDS WITH "-544" THEN "Builtin Administrator" ELSE null END AS da_type SET c.da_types = c.da_types + da_type',
        "is_write": True,
        "is_gds": False,
    },
    "set_dagg": {
        "name": "Set da=TRUE to groups that are domain admins or administrators or enterprise admin",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" SET g.is_da=TRUE',
        "is_write": True,
        "is_gds": False,
    },
    "set_dagg_types": {
        "name": "Set the da type (domain, enterprise, key or builtin)",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-525" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" WITH g, CASE WHEN g.objectid ENDS WITH "-512" THEN "Domain Admin" WHEN g.objectid ENDS WITH "-518" THEN "Schema Admin" WHEN g.objectid ENDS WITH "-519" THEN "Enterprise Admin" WHEN g.objectid ENDS WITH "-525" THEN "Protected Users" WHEN g.objectid ENDS WITH "-526" THEN "_ Key Admin" WHEN g.objectid ENDS WITH "-527" THEN "Enterprise Key Admin" WHEN g.objectid ENDS WITH "-544" THEN "Builtin Administrator" ELSE null END AS da_type SET g.da_types = g.da_types + da_type',
        "is_write": True,
        "is_gds": False,
    },
    "set_daggg": {
        "name": "Set dag=TRUE to the exact domain admin group (end with 512)",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-512"  SET g.is_dag=TRUE',
        "is_write": True,
        "is_gds": False,
    },
    "set_dac": {
        "name": "Set dac=TRUE to computers that are domain admins or administrators or enterprise admin and not DC computer",
        "request": 'MATCH (c:Computer{is_dc:False})-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" SET c.is_dac=TRUE, c.dac_types=[]',
        "is_write": True,
        "is_gds": False,
    },
    "set_dac_types": {
        "name": "Set the dac types (domain, enterprise, key or builtin)",
        "request": 'MATCH (c:Computer)-[:MemberOf*1..3]->(g:Group) WHERE g.objectid ENDS WITH "-512" OR g.objectid ENDS WITH "-518" OR g.objectid ENDS WITH "-519" OR g.objectid ENDS WITH "-525" OR g.objectid ENDS WITH "-526" OR g.objectid ENDS WITH "-527" OR g.objectid ENDS WITH "-544" WITH c,g, CASE WHEN g.objectid ENDS WITH "-512" THEN "Domain Admin" WHEN g.objectid ENDS WITH "-518" THEN "Schema Admin" WHEN g.objectid ENDS WITH "-519" THEN "Enterprise Admin" WHEN g.objectid ENDS WITH "-525" THEN "Protected Users" WHEN g.objectid ENDS WITH "-526" THEN "_ Key Admin" WHEN g.objectid ENDS WITH "-527" THEN "Enterprise Key Admin" WHEN g.objectid ENDS WITH "-544" THEN "Builtin Administrator" ELSE null END AS da_type SET c.da_types = c.da_types + da_type',
        "is_write": True,
        "is_gds": False,
    },
    "set_nonda": {
        "name": "Set is_da=FALSE to all objects that do not have is_da=TRUE",
        "request": "MATCH (c) WHERE c.is_da IS NULL SET c.is_da=FALSE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_da", "set_dag", "set_dagg", "set_msol"],
    },
    "set_nondag": {
        "name": "Set is_dag=FALSE to all objects that do not have is_da=TRUE",
        "request": "MATCH (g) WHERE g.is_dag IS NULL SET g.is_dag=FALSE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_daggg"],
    },
    "set_is_group_operator": {
        "name": "Set is_group_operator to Operator Groups (cf: ACCOUNT OPERATORS, SERVER OPERATORS, BACKUP OPERATORS, PRINT OPERATORS)",
        "request": 'MATCH (g:Group) WHERE g.objectid ENDS WITH "-551" OR g.objectid ENDS WITH "-549" OR g.objectid ENDS WITH "-548" OR g.objectid ENDS WITH "-550" SET g.is_group_operator=True SET g.is_group_account_operator = CASE WHEN g.objectid ENDS WITH "-548" THEN true END, g.is_group_backup_operator = CASE WHEN g.objectid ENDS WITH "-551" THEN true END, g.is_group_server_operator = CASE WHEN g.objectid ENDS WITH "-549" THEN true END, g.is_group_print_operator = CASE WHEN g.objectid ENDS WITH "-550" THEN true END',
        "is_write": True,
        "is_gds": False,
    },
    "set_is_operator_member": {
        "name": "Set is_operator_member to objects member of Operator Groups (cf: ACCOUNT OPERATORS, SERVER OPERATORS, BACKUP OPERATORS, PRINT OPERATORS)",
        "request": 'MATCH (o:User)-[r:MemberOf*1..5]->(g:Group{is_group_operator:True}) WHERE o.is_da=false OR o.domain <> g.domain SET o.is_operator_member=true SET o.is_account_operator = CASE WHEN g.objectid ENDS WITH "-548" THEN true ELSE o.is_account_operator END, o.is_type_operator = CASE WHEN g.objectid ENDS WITH "-548" THEN "ACCOUNT OPERATOR" ELSE o.is_type_operator END, o.is_backup_operator = CASE WHEN g.objectid ENDS WITH "-551" THEN true ELSE o.is_backup_operator END, o.is_type_operator = CASE WHEN g.objectid ENDS WITH "-548" THEN "BACKUP OPERATOR" ELSE o.is_type_operator END, o.is_server_operator = CASE WHEN g.objectid ENDS WITH "-549" THEN true ELSE o.is_server_operator END, o.is_type_operator = CASE WHEN g.objectid ENDS WITH "-548" THEN "SERVER OPERATOR" ELSE o.is_type_operator END, o.is_print_operator = CASE WHEN g.objectid ENDS WITH "-550" THEN true ELSE o.is_print_operator END, o.is_type_operator = CASE WHEN g.objectid ENDS WITH "-548" THEN "PRINT OPERATOR" ELSE o.is_type_operator END',
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "set_dcsync1": {
        "name": "Set dcsync=TRUE to nodes that can DCSync (GetChanges/GetChangesAll)",
        "request": "MATCH (n1) WITH n1 ORDER BY ID(n1) SKIP PARAM1 LIMIT PARAM2 MATCH p=allShortestPaths((n1)-[:MemberOf|GetChanges*1..5]->(u:Domain)) WHERE n1 <> u WITH n1 MATCH p2=(n1)-[:MemberOf|GetChangesAll*1..5]->(u:Domain) WHERE n1 <> u AND NOT n1.name IS NULL AND (((n1.is_da IS NULL OR n1.is_da=FALSE) AND (n1.is_dc IS NULL OR n1.is_dc=FALSE)) OR (NOT u.domain CONTAINS '.' + n1.domain AND n1.domain <> u.domain)) SET n1.can_dcsync=TRUE RETURN DISTINCT p2 as p",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_dc",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_nondc",
            "set_upper_domain_name",
        ],
    },
    "set_dcsync2": {
        "name": "Set dcsync=TRUE to nodes that can DCSync (GenericAll/AllExtendedRights)",
        "request": "MATCH (n2) WITH n2 ORDER BY ID(n2) SKIP PARAM1 LIMIT PARAM2 MATCH p3=allShortestPaths((n2)-[:MemberOf|GenericAll|AllExtendedRights*1..5]->(u:Domain)) WHERE n2 <> u AND NOT n2.name IS NULL AND (((n2.is_da IS NULL OR n2.is_da=FALSE) AND (n2.is_dc IS NULL OR n2.is_dc=FALSE)) OR (NOT u.domain CONTAINS '.' + n2.domain AND n2.domain <> u.domain)) SET n2.can_dcsync=TRUE RETURN DISTINCT p3 as p",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_dc",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_nondc",
            "set_upper_domain_name",
        ],
    },
    "dcsync_list": {
        "name": "Get list of objects that can DCsync (and should probably not be to)",
        "request": "MATCH (n{can_dcsync:true}) RETURN n.domain as domain, n.name as name",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "set_ou_candidate": {
        "name": "Set ou_candidate=TRUE to candidates eligible to shortestou to DA",
        "request": "MATCH (m) WHERE NOT m.name IS NULL AND ((m:Computer AND m.enabled AND (m.is_dc=false OR m.is_dc IS NULL)) OR (m:User AND m.enabled AND (m.is_da=false OR m.is_da IS NULL))) SET m.ou_candidate=TRUE",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_dc",
            "set_msol",
            "set_nonda",
            "set_nondc",
        ],
    },
    "set_containsda": {
        "name": "Set contains_da_dc=TRUE to all objects that contains a domain administrator",
        "request": "MATCH p=(o:OU)-[r:Contains*1..]->(x{is_da:true}) SET o.contains_da_dc=true RETURN p",
        "is_write": True,
        "is_gds": False,
    },
    "set_containsdc": {
        "name": "Set contains_da_dc=TRUE to all objects that contains a domain controller",
        "request": "MATCH p=(o:OU)-[r:Contains*1..]->(x{is_dc:true}) SET o.contains_da_dc=true RETURN p",
        "is_write": True,
        "is_gds": False,
    },
    "set_is_da_dc": {
        "name": "Set is_da_dc=TRUE to all objects that are domain controller or domain admins",
        "request": "MATCH (u) WHERE (u.is_da=true OR u.is_dc=true) SET u.is_da_dc=true",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_da", "set_dag", "set_dagg", "set_dc", "set_msol", "set_nonda", "set_nondc"],
    },
    "set_is_not_da_dc": {
        "name": "Set is_da_dc=FALSE to objects without is_da_dc = TRUE",
        "request": "MATCH (o:Base) WHERE o.is_da_dc IS NULL SET o.is_da_dc = FALSE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_is_da_dc"],
    },
    "set_is_adcs": {
        "name": "Set is_adcs to ADCS servers",
        "request": "MATCH (g:Group) WHERE g.objectid ENDS WITH '-517' MATCH (c:Computer)-[r:MemberOf*1..4]->(g) SET c.is_adcs=TRUE RETURN c.domain AS domain, c.name AS name",
        "is_write": True,
        "is_gds": False,
    },
    "set_path_candidate": {
        "name": "Set path_candidate=TRUE to candidates eligible to shortestPath to DA",
        "request": "MATCH (o{is_da_dc:false}) WHERE NOT o:Domain AND ((o.enabled=True AND o:User) OR NOT o:User) AND (NOT o.is_adcs OR o.is_adcs is null) SET o.path_candidate=TRUE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_is_adcs"],
    },
    "set_groups_members_count": {
        "name": "Set members_count to groups counting users (recursivity = 5)",
        "request": "MATCH  (g:Group) WITH g ORDER BY g.name SKIP PARAM1 LIMIT PARAM2 MATCH (u:User)-[:MemberOf*1..5]->(g) WHERE NOT u.name IS NULL AND NOT g.name IS NULL WITH g AS g1, count(u) AS memberscount SET g1.members_count=memberscount",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_groups_members_count_computers": {
        "name": "Set members_count to groups counting computers (recursivity = 5)",
        "request": "MATCH (g:Group) WITH g ORDER BY g.name SKIP PARAM1 LIMIT PARAM2 MATCH (u:Computer)-[:MemberOf*1..5]->(g) WHERE NOT u.name IS NULL AND NOT g.name IS NULL WITH g AS g1, count(u) AS memberscount SET g1.members_count= COALESCE(g1.members_count, 0) + memberscount",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_groups_has_members": {
        "name": "Set has_member=True to groups with member, else false ",
        "request": "MATCH (g:Group) SET g.has_members=(CASE WHEN g.members_count>0 THEN TRUE ELSE FALSE END)",
        "is_write": True,
        "is_gds": False,
    },
    "set_gpo_links_count": {
        "name": "Set the count of links/object where the GPO is applied",
        "request": "MATCH p=(g:GPO)-[:GPLink]->(o) WITH g.name as gponame, count(p) AS gpolinkscount MATCH (g1:GPO) WHERE g1.name=gponame AND gpolinkscount IS NOT NULL SET g1.gpolinks_count=gpolinkscount",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_gpos_has_links": {
        "name": "Set has_links=True to GPOs with links, else false ",
        "request": "MATCH (g:GPO) SET g.has_links=(CASE WHEN g.gpolinks_count>0 THEN TRUE ELSE FALSE END)",
        "is_write": True,
        "is_gds": False,
    },
    "set_groups_direct_admin": {
        "name": "Set groups which are direct admins of computers",
        "request": "MATCH (g:Group)-[r:AdminTo]->(c:Computer) SET g.is_admin=true RETURN DISTINCT g",
        "is_write": True,
        "is_gds": False,
    },
    "set_groups_indirect_admin_1": {
        "name": "1 - Set groups which are indirect admins of computers, ie. admins of admin groups (see precedent request)",
        "request": "MATCH (g:Group)-[r:MemberOf]->(gg:Group{is_admin:true}) SET g.is_admin=true RETURN DISTINCT g",
        "is_write": True,
        "is_gds": False,
    },
    "set_groups_indirect_admin_2": {
        "name": "2 - Set groups which are indirect admins of computers, ie. admins of admin groups (see precedent request)",
        "request": "MATCH (g:Group)-[r:MemberOf]->(gg:Group{is_admin:true}) WHERE g.is_admin IS NULL SET g.is_admin=true RETURN DISTINCT g",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "set_groups_direct_admin",
            "set_groups_indirect_admin_1",
            "set_groups_indirect_admin_3",
            "set_groups_indirect_admin_4",
            "set_user_indirect_admin",
            "set_users_direct_admin",
        ],
    },
    "set_groups_indirect_admin_3": {
        "name": "3 - Set groups which are indirect admins of computers, ie. admins of admin groups (see precedent request)",
        "request": "MATCH (g:Group)-[r:MemberOf]->(gg:Group{is_admin:true}) WHERE g.is_admin IS NULL SET g.is_admin=true RETURN DISTINCT g",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "set_groups_direct_admin",
            "set_groups_indirect_admin_1",
            "set_groups_indirect_admin_2",
            "set_groups_indirect_admin_4",
            "set_user_indirect_admin",
            "set_users_direct_admin",
        ],
    },
    "set_groups_indirect_admin_4": {
        "name": "4 - Set groups which are indirect admins of computers, ie. admins of admin groups (see precedent request)",
        "request": "MATCH (g:Group)-[r:MemberOf]->(gg:Group{is_admin:true}) WHERE g.is_admin IS NULL SET g.is_admin=true RETURN DISTINCT g",
        "is_write": True,
        "is_gds": False,
        "depends_on": [
            "set_groups_direct_admin",
            "set_groups_indirect_admin_1",
            "set_groups_indirect_admin_2",
            "set_groups_indirect_admin_3",
            "set_user_indirect_admin",
            "set_users_direct_admin",
        ],
    },
    "set_user_indirect_admin": {
        "name": "Set is_admin=True to users members of groups with is_admin=True",
        "request": "MATCH (u:User)-[:MemberOf]->(:Group{is_admin:true}) SET u.is_admin=true",
        "is_write": True,
        "is_gds": False,
    },
    "set_users_direct_admin": {
        "name": "Set is_admin=True to users with ",
        "request": "MATCH (u:User)-[:AdminTo]->() SET u.is_admin=true",
        "is_write": True,
        "is_gds": False,
    },
    "set_target_kud": {
        "name": "Set target_kud attribute on nodes that are configured for KUD",
        "request": "MATCH (o{unconstraineddelegation:true}) WHERE ((o:User AND o.enabled=true) OR (o:Computer AND o.is_dc=false)) SET o.target_kud=TRUE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_dc", "set_nondc"],
    },
    "set_az_privileged": {
        "name": "Find all Azure objects that are privileged and set is_priv is true",
        "request": "MATCH (n:AZBase) WHERE 'admin_tier_0' IN split(n.system_tags, ' ') AND n.name =~ '(?i)Global Administrator.*|User Administrator.*|Cloud Application Administrator.*|Authentication Policy Administrator.*|Exchange Administrator.*|Helpdesk Administrator.*|Privileged Authentication Administrator.*'  SET n.is_priv=true",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "set_az_not_privileged": {
        "name": "Find all Azure objects that are not privileged and set is_priv is false",
        "request": "MATCH (n:AZBase) WHERE n.is_priv IS NULL SET n.is_priv=false",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["set_az_privileged"],
    },
    "azure_set_apps_name": {
        "name": "Set Azure applications names",
        "request": "MATCH (a:AZApp) WHERE a.name IS NULL AND a.displayname IS NOT NULL SET a.name = a.displayname",
        "is_write": True,
        "is_gds": False,
    },
    "nb_domain_collected": {
        "name": "Count number of domains collected",
        "request": "MATCH (m:Domain{collected:true}) RETURN m.name",
        "is_write": False,
        "is_gds": False,
    },
    "set_ghost_computer": {
        "name": "Set ghost_computer=TRUE to computers that did not login for more than 90 days",
        "request": "MATCH (n:Computer{enabled:true}) WHERE toInteger(($extract_date$ - n.lastlogontimestamp)/86400)>$password_renewal$ SET   n.ghost_computer=TRUE",
        "is_write": True,
        "is_gds": False,
    },
    "set_default_exploitability_rating": {
        "name": "Set default exploitability rating (r.cost=100) to all relations",
        "request": "MATCH ()-[r]->() WITH r SKIP PARAM1 LIMIT PARAM2 MATCH ()-[r]->() SET r.cost=100",
        "is_write": False,
        "is_gds": False,
    },
    "get_all_nodes": {
        "name": "Retrieving all nodes for fastGDS mode",
        "request": "MATCH (o) WITH o ORDER BY ID(o) SKIP PARAM1 LIMIT PARAM2 MATCH (o) RETURN ID(o) AS id, LABELS(o) AS labels, o.name AS name, o.domain AS domain, o.tenant_id AS tenant_id",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "check_unknown_relations": {
        "name": "Checking for unknown relations",
        "request": "MATCH ()-[r]->() RETURN DISTINCT type(r) as relationType",
        "is_write": False,
        "is_gds": False,
    },
    "domains": {
        "name": "List of domains",
        "request": "MATCH (m:Domain) RETURN DISTINCT(m.name) AS domain ORDER BY m.name",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "nb_domain_controllers": {
        "name": "Number of domain controllers",
        "request": "MATCH (c1:Computer{is_dc:TRUE}) RETURN DISTINCT(c1.domain) AS domain, c1.name AS name, COALESCE(c1.operatingsystem, 'Unknown') AS os, COALESCE(c1.ghost_computer, False) AS ghost, toInteger(($extract_date$ - c1.lastlogontimestamp)/86400) as lastLogon",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_ghost_computer",
            "set_upper_domain_name",
        ],
    },
    "domain_OUs": {
        "name": "Domain Organisational Units",
        "request": "MATCH (o:OU)-[:Contains]->(c) RETURN o.name AS OU, c.name AS name",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "users_shadow_credentials": {
        "name": "Non privileged users that can impersonate privileged users",
        "request": "MATCH (u:User{enabled:true,is_da:false}) WITH u ORDER BY ID(u) SKIP PARAM1 LIMIT PARAM2 MATCH p=(u)-[:MemberOf*0..3]->()-[r:AddKeyCredentialLink|WriteProperty|GenericAll|GenericWrite|Owns|WriteDacl]->(m:User{is_da:true,enabled:true}) RETURN p",
        "is_write": False,
        "is_gds": False,
    },
    "users_shadow_credentials_to_non_admins": {
        "name": "Non privileged users that can be impersonated by non privileged users",
        "request": "MATCH (s) WHERE (s:User AND s.enabled AND NOT s.is_da) OR (s:Group AND NOT s.is_dag AND NOT s.is_da) WITH s ORDER BY ID(s) SKIP PARAM1 LIMIT PARAM2 MATCH p=shortestPath((s)-[r:AddKeyCredentialLink|WriteProperty|GenericAll|GenericWrite|Owns|WriteDacl*1..3]->(t:User{enabled:true})) WHERE s <> t AND s.is_group_account_operator IS NULL RETURN p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target:User{enabled:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_users_shadow_credentials_to_non_admins', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path WITH nodes(path)[-1] AS starting_node, path WHERE starting_node <> target AND starting_node.is_group_account_operator IS NULL AND starting_node.is_account_operator IS NULL AND ((starting_node:User AND starting_node.enabled AND NOT starting_node.is_da) OR (starting_node:Group AND NOT starting_node.is_dag AND NOT starting_node.is_da)) RETURN path as p",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_users_shadow_credentials_to_non_admins', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:AddKeyCredentialLink|WriteProperty|GenericAll|GenericWrite|Owns|WriteDacl]->(m) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": [
            "set_da",
            "set_dag",
            "set_dagg",
            "set_daggg",
            "set_is_group_operator",
            "set_msol",
            "set_nonda",
            "set_nondag",
        ],
    },
    "nb_enabled_accounts": {
        "name": "Number of domain accounts enabled",
        "request": "MATCH p=(u:User{enabled:true} ) RETURN DISTINCT(u.domain) AS domain, u.name AS name, toInteger(($extract_date$ - u.lastlogontimestamp)/86400) AS logon ORDER BY u.domain",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "nb_disabled_accounts": {
        "name": "Number of domain accounts disabled",
        "request": "MATCH p=(u:User{enabled:false} ) RETURN DISTINCT(u.domain) AS domain, u.name AS name, toInteger(($extract_date$ - u.lastlogontimestamp)/86400) AS logon ORDER BY u.domain",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "nb_groups": {
        "name": "Number of groups",
        "request": "MATCH p=(g:Group) WHERE NOT g.name IS NULL AND NOT g.domain IS NULL RETURN DISTINCT(g.domain) AS domain, g.name AS name, g.is_da AS da ORDER BY g.domain",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "nb_computers": {
        "name": "Number of computers",
        "request": "MATCH (c:Computer) WHERE NOT c.name IS NULL RETURN DISTINCT(c.domain) AS domain, c.name AS name, c.operatingsystem AS os, c.ghost_computer AS ghost, c.enabled as enabled ORDER BY c.domain",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_ghost_computer",
            "set_upper_domain_name",
        ],
    },
    "computers_not_connected_since": {
        "name": "Computers not connected since",
        "request": "MATCH (c:Computer) WHERE NOT c.lastlogontimestamp IS NULL AND c.name IS NOT NULL AND c.enabled RETURN c.name AS name, toInteger(($extract_date$ - c.lastlogontimestamp)/86400) as days, toInteger(($extract_date$ - c.pwdlastset)/86400) as pwdlastset, c.enabled as enabled ORDER BY days DESC",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "nb_domain_admins": {
        "name": "Number of domain admin accounts",
        "request": "MATCH (n{enabled:true}) WHERE n.is_msol IS NULL AND n.is_da = TRUE RETURN n.domain AS domain, n.name AS name, n.da_types AS `admin type`, n.admincount AS `admincount`",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_da_types",
            "set_dac_types",
            "set_dag",
            "set_dag_types",
            "set_dagg",
            "set_dagg_types",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "os": {
        "name": "Number of OS",
        "request": "MATCH (c:Computer{enabled:true}) WHERE  NOT c.enabled IS NULL AND NOT c.operatingsystem IS NULL RETURN DISTINCT(c.operatingsystem) AS os, toInteger(($extract_date$ - c.lastlogontimestamp)/86400) as lastLogon, c.name AS name, c.domain AS domain ORDER BY c.operatingsystem",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "krb_pwd_last_change": {
        "name": "Kerberos password last change in days",
        "request": 'MATCH(u:User) WHERE u.name STARTS WITH "KRBTGT@" RETURN u.domain as domain, u.name as name, toInteger(($extract_date$ - u.pwdlastset)/86400) as pass_last_change, toInteger(($extract_date$ - u.whencreated)/86400) AS accountCreationDate',
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "nb_kerberoastable_accounts": {
        "name": "Number of Kerberoastable accounts",
        "request": "MATCH (u:User{hasspn:true,enabled:true}) WHERE u.gmsa IS NULL AND u.name IS NOT NULL RETURN u.domain AS domain, u.name AS name, toInteger(($extract_date$ - u.pwdlastset)/86400) AS pass_last_change, u.is_da AS is_Domain_Admin, u.serviceprincipalnames AS SPN, toInteger(($extract_date$ - u.whencreated)/86400) AS accountCreationDate ORDER BY pass_last_change DESC",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "nb_as-rep_roastable_accounts": {
        "name": "Number of AS-REP Roastable accounts",
        "request": "MATCH (u:User{enabled:true,dontreqpreauth: true}) RETURN u.domain AS domain,u.name AS name, u.is_da AS is_Domain_Admin",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "nb_computer_unconstrained_delegations": {
        "name": "Number of machines with unconstrained delegations",
        "request": "MATCH (c2:Computer{unconstraineddelegation:true,is_dc:FALSE}) RETURN DISTINCT(c2.domain) AS domain,c2.name AS name",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "nb_users_unconstrained_delegations": {
        "name": "Number of users with unconstrained delegations",
        "request": "MATCH (c2:User{enabled:true,unconstraineddelegation:true,is_da:FALSE}) RETURN DISTINCT(c2.domain) AS domain,c2.name AS name",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "users_constrained_delegations": {
        "name": "Number of users with constrained delegations",
        "request": "MATCH (u:User)-[:AllowedToDelegate]->(c:Computer) WHERE u.name IS NOT NULL AND c.name IS NOT NULL RETURN u.name AS name, c.name AS computer,c.is_dc as to_DC ORDER BY name",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "set_dc", "set_nondc"],
    },
    "dormant_accounts": {
        "name": "Dormant accounts",
        "request": "MATCH (n:User{enabled:true}) WHERE toInteger(($extract_date$ - n.lastlogontimestamp)/86400)>$password_renewal$ RETURN n.domain as domain, n.name as name, n.displayname as displayname, toInteger(($extract_date$ - n.lastlogontimestamp)/86400) AS days, toInteger(($extract_date$ - n.whencreated)/86400) AS accountCreationDate, n.distinguishedname as distinguishedname ORDER BY days DESC",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "password_last_change": {
        "name": "Password last change in days",
        "request": "MATCH (c:User {enabled:TRUE}) RETURN DISTINCT(c.name) AS user,toInteger(($extract_date$ - c.pwdlastset )/ 86400) AS days, toInteger(($extract_date$ - c.whencreated)/86400) AS accountCreationDate ORDER BY days DESC",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "nb_user_password_cleartext": {
        "name": "Number of accounts where password cleartext password is populated",
        "request": 'MATCH (u:User) WHERE NOT u.userpassword IS null RETURN u.name AS user,"[redacted for security purposes]" AS password, u.is_da as `is Domain Admin`',
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "set_da", "set_dag", "set_dagg", "set_msol", "set_nonda"],
    },
    "get_users_password_not_required": {
        "name": "Number of accounts where password is not required",
        "request": "MATCH (u:User{enabled:true,passwordnotreqd:true}) RETURN DISTINCT (u.domain) as domain, (u.name) AS user,toInteger(($extract_date$ - u.pwdlastset )/ 86400) AS pwdlastset,toInteger(($extract_date$ - u.lastlogontimestamp)/86400) AS lastlogon",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "objects_admincount": {
        "name": "N objects have AdminSDHolder",
        "request": "MATCH (n{enabled:True, admincount:True}) RETURN n.domain as domain, labels(n) as type, n.name as name",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "user_password_never_expires": {
        "name": "Password never expired",
        "request": "MATCH (u:User{enabled:true})WHERE u.pwdneverexpires = true RETURN DISTINCT(u.domain) AS domain, u.name AS name, toInteger(($extract_date$ - u.lastlogontimestamp)/86400) AS LastLogin, toInteger(($extract_date$ - u.pwdlastset )/ 86400) AS LastPasswChange,toInteger(($extract_date$ - u.whencreated)/86400) AS accountCreationDate",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "computers_members_high_privilege": {
        "name": "High privilege group computer member",
        "request": "MATCH(c:Computer{is_dc:false})-[r:MemberOf*1..4]->(g:Group{is_da:true}) WHERE NOT c.name IS NULL RETURN distinct(c.name) AS computer, g.name AS group, g.domain AS domain",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "objects_to_domain_admin": {
        "name": "Objects with path to DA",
        "request": "MATCH (m{path_candidate:true}) WHERE NOT m.name IS NULL WITH m ORDER BY ID(m) SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((m)-[r:$properties$*1..$recursive_level$]->(g:Group{is_dag:true})) WHERE m<>g SET m.has_path_to_da=true RETURN DISTINCT(p) as p",
        "is_write": True,
        "is_gds": True,
        "gds_request": "MATCH (target:Group {is_dag: true}) CALL gds.allShortestPaths.dijkstra.stream('graph_objects_to_domain_admin', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE starting_node.path_candidate = TRUE SET starting_node.has_path_to_da=true RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_objects_to_domain_admin', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:$properties$]->(m) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": ["azure_set_apps_name"],
    },
    "objects_to_adcs": {
        "name": "Objects with path to ADCS servers",
        "request": "MATCH (o{path_candidate:true}) WHERE NOT o:Group AND NOT o.name IS NULL WITH o ORDER BY o.name SKIP PARAM1 LIMIT PARAM2 MATCH p=(o)-[rrr:MemberOf*0..4]->()-[rr:AdminTo]->(c{is_adcs:true}) RETURN DISTINCT(p) as p",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "users_admin_on_computers": {
        "name": "Users admin on machines",
        "request": "MATCH (u:User{enabled:true}) WITH u ORDER BY ID(u) SKIP PARAM1 LIMIT PARAM2 MATCH p=(u)-[:MemberOf*0..3]->()-[r:AdminTo]->(c:Computer) RETURN u.name AS user, u.displayname as displayname, c.name AS computer, c.has_path_to_da AS has_path_to_da, ID(u) as user_id, u.distinguishedname AS distinguishedname, p",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "objects_to_domain_admin"],
    },
    "users_admin_on_servers_1": {
        "name": "Users admin on servers n°1",
        "request": "MATCH (n:User{enabled:true,is_da:false}) WHERE NOT n.name IS NULL WITH n ORDER BY ID(n) SKIP PARAM1 LIMIT PARAM2 MATCH p=(n)-[r:MemberOf*1..2]->(g:Group)-[r1:$properties$]->(u:Computer) WITH LENGTH(p) as pathLength, p, n, u WHERE NONE (x in NODES(p)[1..(pathLength-1)] WHERE x.objectid = u.objectid) AND NOT n.objectid = u.objectid RETURN n.name AS user, u.name AS computer, u.has_path_to_da as has_path_to_da",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "objects_to_domain_admin"],
    },
    "users_admin_on_servers_2": {
        "name": "Users admin on servers n°2",
        "request": "MATCH (n:User{enabled:true,is_da:false}) WHERE NOT n.name IS NULL WITH n ORDER BY ID(n) SKIP PARAM1 LIMIT PARAM2 MATCH p=(n)-[r1:$properties$]->(u:Computer) WITH LENGTH(p) as pathLength, p, n, u WHERE NONE (x in NODES(p)[1..(pathLength-1)] WHERE x.objectid = u.objectid) AND NOT n.objectid = u.objectid RETURN n.name AS user, u.name AS computer, u.has_path_to_da as has_path_to_da",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "objects_to_domain_admin"],
    },
    "computers_admin_on_computers": {
        "name": "Number of computers admin of computers",
        "request": "MATCH (c1:Computer)-[:MemberOf*0..]->()-[:AdminTo]->(c2:Computer) WHERE c1 <> c2 RETURN DISTINCT c1.name AS source_computer, c2.name AS target_computer, c2.has_path_to_da AS has_path_to_da, c2.smbsigning AS smbsigning",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "objects_to_domain_admin"],
    },
    "domain_map_trust": {
        "name": "Domain map trust",
        "request": "MATCH p=shortestpath((d:Domain)-[:TrustedBy|AbuseTGTDelegation|SameForestTrust|SpoofSIDHistory|CrossForestTrust]->(m:Domain)) WHERE d<>m RETURN DISTINCT(p)",
        "is_write": False,
        "is_gds": False,
    },
    "kud": {
        "name": "Shortest paths to objects configured for KUD",
        "request": "MATCH (n) WHERE (n:Computer OR (n:User AND n.enabled=true))  AND (n.is_da IS NULL OR n.is_da=FALSE) AND (n.is_dc IS NULL OR n.is_dc=FALSE) WITH n ORDER BY n.name SKIP PARAM1 LIMIT PARAM2 MATCH p=shortestPath((n)-[:$properties$*1..$recursive_level$]->(m{target_kud:true})) WHERE NOT n=m AND (((n.is_da IS NULL OR n.is_da=FALSE) AND (n.is_dc IS NULL OR n.is_dc=FALSE)) OR (NOT m.domain CONTAINS '.' + n.domain AND n.domain <> m.domain)) RETURN DISTINCT(p)",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target{target_kud:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_kud', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE ((starting_node:Computer OR (starting_node:User AND starting_node.enabled=true))  AND (starting_node.is_da IS NULL OR starting_node.is_da=FALSE) AND (starting_node.is_dc IS NULL OR starting_node.is_dc=FALSE)) AND (target <> starting_node AND (((starting_node.is_da IS NULL OR starting_node.is_da=FALSE) AND (starting_node.is_dc IS NULL OR starting_node.is_dc=FALSE)) OR (NOT target.domain CONTAINS '.' + starting_node.domain AND starting_node.domain <> target.domain))) RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_kud', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:$properties$]->(m) WHERE NOT (m:Domain OR n:Domain) AND NOT (n.is_dag=true or m.is_dag=true) AND NOT (n.is_da=true or m.is_da=true) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_dc",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_nondc",
            "set_upper_domain_name",
        ],
    },
    "nb_computers_laps": {
        "name": "Number of computers with laps",
        "request": "MATCH (c:Computer) WHERE NOT c.name is NULL and NOT c.haslaps IS NULL AND toUpper(c.operatingsystem) CONTAINS 'WINDOWS' RETURN DISTINCT(c.domain) AS domain, toInteger(($extract_date$ - c.lastlogontimestamp)/86400) as lastLogon, c.name AS name, toString(c.haslaps) AS LAPS",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "can_read_laps": {
        "name": "Objects allowed to read LAPS",
        "request": "MATCH (n{path_candidate:true}) WHERE n:User OR n:Group OR n:Computer WITH n ORDER BY ID(n) SKIP PARAM1 LIMIT PARAM2 MATCH p = (n)-[r1:MemberOf*0..3]->()-[r2:GenericAll|ReadLAPSPassword|AllExtendedRights|SyncLAPSPassword]->(t:Computer{haslaps:true}) WHERE NOT (n)-[:MemberOf*0..3]->()-[:AdminTo]->(t) RETURN DISTINCT n.domain AS source_domain, n.name AS source_name, labels(n) as source_labels, t.domain as target_domain, t.name as target_name",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "objects_to_dcsync": {
        "name": "Objects to dcsync",
        "request": "MATCH (n{path_candidate:true}) WHERE n.can_dcsync IS NULL AND NOT n.name IS NULL WITH n ORDER BY n.name SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((n)-[r:$properties$*1..$recursive_level$]->(target{can_dcsync:TRUE})) WHERE n<>target RETURN distinct(p) AS p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target{can_dcsync:TRUE}) CALL gds.allShortestPaths.dijkstra.stream('graph_objects_to_dcsync', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE target <> starting_node AND starting_node.path_candidate = TRUE AND starting_node:User RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_objects_to_dcsync', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:$properties$]->(m) WHERE NOT (m:Domain OR n:Domain) AND NOT (n.is_dag=true or m.is_dag=true) AND NOT (n.is_da=true or m.is_da=true) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": ["azure_set_apps_name", "set_dcsync1", "set_dcsync2"],
    },
    "dom_admin_on_non_dc": {
        "name": "Domain admin with session on non DC computers",
        "request": "MATCH p=(c:Computer{path_candidate:true})-[r:HasSession]->(u:User{enabled:true, is_da:true}) WHERE NOT c.name IS NULL and NOT u.name IS NULL and NOT c.is_dc=True RETURN distinct(p) AS p",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name", "set_dc", "set_nondc"],
    },
    "unpriv_to_dnsadmins": {
        "name": "Unprivileged users with path to DNSAdmins",
        "request": "MATCH (u:User{path_candidate:true}) WITH u ORDER BY u.name SKIP PARAM1 LIMIT PARAM2 MATCH p=(u)-[r:MemberOf*1..$recursive_level$]->(g:Group{is_dnsadmin:true}) RETURN distinct(p) AS p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target:Group{is_dnsadmin:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_unpriv_to_dnsadmins', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE target <> starting_node AND starting_node.path_candidate = TRUE AND starting_node:User RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_unpriv_to_dnsadmins', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:MemberOf]->(m) WHERE NOT (m:Domain OR n:Domain) AND NOT (n.is_dag=true or m.is_dag=true) AND NOT (n.is_da=true or m.is_da=true) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": ["azure_set_apps_name"],
    },
    "rdp_access": {
        "name": "Users with RDP-access to Computers ",
        "request": "MATCH (u:User{enabled:true,is_da:false}) WITH u ORDER BY ID(u) SKIP PARAM1 LIMIT PARAM2 MATCH p=(u)-[r1:MemberOf*0..5]->()-[r2:CanRDP]->(c:Computer) RETURN u.name as user, c.name as computer",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "dc_impersonation": {
        "name": "Non-domain admins that can directly or indirectly impersonate a Domain Controller ",
        "request": "MATCH (u{ou_candidate:true}) WITH u ORDER BY ID(u) SKIP PARAM1 LIMIT PARAM2 MATCH p=(u)-[r:MemberOf*0..3]->()-[r3:AddKeyCredentialLink|WriteProperty|GenericAll|GenericWrite|Owns|WriteDacl]->(m:Computer{is_dc:true}) RETURN DISTINCT p",
        "is_write": False,
        "is_gds": False,
    },
    "graph_rbcd": {
        "name": "Builds RBCD attack path graph and sets is_rbcd_target attribute ",
        "request": "MATCH (m:Computer{is_server:true}) WITH m SKIP PARAM1 LIMIT PARAM2 MATCH p=(u:User{path_candidate:true})-[rr:MemberOf|AddMember*0..5]->()-[r:GenericAll|GenericWrite|WriteDACL|AllExtendedRights|Owns]->(m) SET m.is_rbcd_target=TRUE RETURN p",
        "is_write": True,
        "is_gds": False,
    },
    "graph_rbcd_to_da": {
        "name": "Builds RBCD targets to DA paths",
        "request": "MATCH (m:Computer{is_rbcd_target:true}) WHERE NOT m.name IS NULL WITH m ORDER BY m.name SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((m)-[r:$properties$*1..$recursive_level$]->(g:Group{is_dag:true})) WHERE m<>g RETURN DISTINCT(p) as p",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "compromise_paths_of_OUs": {
        "name": "Compromisable OUs",
        "request": "MATCH (o:OU) WITH o ORDER BY ID(o) SKIP PARAM1 LIMIT PARAM2 MATCH p=shortestPath((u{ou_candidate:true})-[:MemberOf|GenericAll|GenericWrite|Owns|WriteOwner|WriteDacl|WriteGPLink*1..8]->(o:OU)) SET o.vulnerable_OU = TRUE RETURN p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target:OU) CALL gds.allShortestPaths.dijkstra.stream('graph_compromise_paths_of_OUs', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE starting_node.ou_candidate = TRUE SET starting_node.vulnerable_OU=true RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_compromise_paths_of_OUs', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:MemberOf|GenericAll|GenericWrite|Owns|WriteOwner|WriteDacl|WriteGPLink]->(m) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
    },
    "vulnerable_OU_impact": {
        "name": "Impact of compromisable OUs",
        "request": "MATCH (o:OU{vulnerable_OU:true}) WITH o ORDER BY o.name SKIP PARAM1 LIMIT PARAM2 MATCH p=shortestPath((o)-[:Contains|MemberOf*1..]->(e)) WHERE o <> e AND (e:User OR e:Computer) RETURN p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (source:OU{vulnerable_OU:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_vulnerable_OU_impact', {sourceNode: source, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS target_node, path, costs WHERE target_node:User OR target_node:Computer RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_vulnerable_OU_impact', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:Contains|MemberOf]->(m) RETURN id(n) as source, id(m) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": ["azure_set_apps_name"],
    },
    "vuln_functional_level": {
        "name": "Insufficient forest and domains functional levels. According to ANSSI (on a scale from 1 to 5, 5 being the better): the security level is at 1 if functional level (FL) <= Windows 2008 R2, at 3 if FL <= Windows 2012R2, at 4 if FL <= Windows 2016 / 2019 / 2022.",
        "request": 'MATCH (o:Domain) WHERE NOT(o.functionallevel IS NULL OR SIZE(o.functionallevel) < 1) RETURN CASE WHEN toUpper(o.functionallevel) CONTAINS "2000" OR toUpper(o.functionallevel) CONTAINS "2003" OR toUpper(o.functionallevel) CONTAINS "2008" OR toUpper(o.functionallevel) CONTAINS "2008 R2" THEN 1 WHEN toUpper(o.functionallevel) CONTAINS "2012" THEN 2 WHEN toUpper(o.functionallevel) CONTAINS "2016" OR toUpper(o.functionallevel) CONTAINS "2018" OR toUpper(o.functionallevel) CONTAINS "2020" OR toUpper(o.functionallevel) CONTAINS "2022" THEN 5 END as `Level maturity`, o.distinguishedname as `Full name`, o.functionallevel as `Functional level`',
        "is_write": False,
        "is_gds": False,
    },
    "vuln_sidhistory_dangerous": {
        "name": "Accounts or groups with unexpected SID history",
        "request": "MATCH(o1)-[r:HasSIDHistory]->(o2{is_da:true}) RETURN o1.domain as parent_domain, o1.name as name, o1.sidhistory as sidhistory",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "can_read_gmsapassword_of_adm": {
        "name": "Objects allowed to read the GMSA of objects with admincount=True",
        "request": "MATCH (o{path_candidate:true}) WITH o ORDER BY ID(o) SKIP PARAM1 LIMIT PARAM2 MATCH p=((o)-[:MemberOf*0..5]->()-[:ReadGMSAPassword]->(u:User{is_admin:true})) WHERE o.name<>u.name RETURN DISTINCT(p)",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "objects_to_operators_member": {
        "name": "Unprivileged users with path to an Operator Member",
        "request": "MATCH (m:User{path_candidate:true}) WITH m ORDER BY m.name SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((m)-[r:$path_to_group_operators_props$*1..$recursive_level$]->(o:User{is_operator_member:true})) WHERE m<>o AND ((o.is_da=true AND o.domain<>m.domain) OR (o.is_da=false)) RETURN DISTINCT(p) as p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target:User{is_operator_member:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_objects_to_operators_member', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE starting_node:User AND target <> starting_node AND starting_node.path_candidate = TRUE AND ((target.is_da=true AND target.domain<>starting_node.domain) OR (target.is_da=false)) RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_objects_to_operators_member', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:$path_to_group_operators_props$]->(m) WHERE NOT (m:Domain OR n:Domain) AND NOT (n.is_dag=true or m.is_dag=true) AND NOT (n.is_da=true or m.is_da=true) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "objects_to_operators_groups": {
        "name": "Operator Member path to Operators Groups",
        "request": "MATCH (m:User{is_operator_member:true}) WITH m ORDER BY ID(m) SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((m)-[r:MemberOf*1..$recursive_level$]->(o:Group{is_group_operator:true})) WHERE (m.is_da=true AND o.domain<>m.domain) OR (m.is_da=false) RETURN DISTINCT(p) as p",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "vuln_permissions_adminsdholder": {
        "name": "Dangerous permissions on the adminSDHolder object",
        "request": 'MATCH (n:User{path_candidate:true}) WITH n ORDER BY n.name SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((n)-[r:$properties$*1..4]->(target1{is_adminsdholder:true})) WHERE n<>target1 AND NOT ANY(no in nodes(p) WHERE (no.is_da=true AND (no.domain=target1.domain OR target1.domain CONTAINS "." + no.domain))) RETURN distinct(p) AS p',
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target{is_adminsdholder:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_vuln_permissions_adminsdholder', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE starting_node:User AND target <> starting_node AND starting_node.path_candidate = TRUE AND NOT ANY(no in nodes(path) WHERE (no.is_da=true AND (no.domain=target.domain OR target.domain CONTAINS \".\" + no.domain))) RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_vuln_permissions_adminsdholder', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:$properties$]->(m) WHERE NOT (m:Domain OR n:Domain) AND NOT (n.is_dag=true or m.is_dag=true) AND NOT (n.is_da=true or m.is_da=true) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "da_to_da": {
        "name": "Paths between two domain admins belonging to different domains",
        "request": "MATCH p=shortestPath((g:Group{is_dag:true})-[r:$properties$*1..$recursive_level$]->(gg:Group{is_dag:true})) WHERE g<>gg AND g.domain <> gg.domain RETURN p",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "anomaly_acl_1": {
        "name": "anomaly_acl_1",
        "request": "MATCH (gg) WHERE NOT gg:Group AND ((gg:User AND gg.enabled) OR (gg:Computer AND gg.enabled) OR (NOT (gg:User OR gg:Computer))) WITH gg as g MATCH (g)-[r2{isacl:true}]->(n) WHERE ((g.is_da IS NULL OR g.is_da=FALSE) AND (g.is_dc IS NULL OR g.is_dc=FALSE) AND (NOT g.is_adcs OR g.is_adcs IS NULL)) OR (NOT n.domain CONTAINS '.' + g.domain AND n.domain <> g.domain) RETURN n.name,g.name,type(r2),LABELS(g),labels(n),ID(n)",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_dc",
            "set_domain_attributes_to_domains",
            "set_is_adcs",
            "set_msol",
            "set_nonda",
            "set_nondc",
            "set_upper_domain_name",
        ],
    },
    "anomaly_acl_2": {
        "name": "anomaly_acl_2",
        "request": "MATCH (gg:Group) WHERE gg.members_count IS NOT NULL with gg as g order by gg.members_count DESC MATCH (g)-[r2{isacl:true}]->(n) WHERE ((g.is_da IS NULL OR g.is_da=FALSE) AND (g.is_dcg IS NULL OR g.is_dcg=FALSE) AND (NOT g.is_adcs OR g.is_adcs IS NULL)) OR (NOT n.domain CONTAINS '.' + g.domain AND n.domain <> g.domain) RETURN g.members_count,n.name,g.name,type(r2),LABELS(g),labels(n),ID(n) order by g.members_count DESC",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_dcg",
            "set_domain_attributes_to_domains",
            "set_groups_members_count",
            "set_groups_members_count_computers",
            "set_is_adcs",
            "set_msol",
            "set_nonda",
            "set_nondcg",
            "set_upper_domain_name",
        ],
    },
    "get_empty_groups": {
        "name": "Returns empty groups",
        "request": "MATCH (g:Group) WHERE NOT EXISTS(()-[:MemberOf]->(g)) AND NOT g.distinguishedname CONTAINS 'CN=BUILTIN' RETURN g.name AS `Empty group`, COALESCE(g.distinguishedname, '-') AS `Full Reference`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "get_empty_ous": {
        "name": "Returns empty ous",
        "request": "MATCH (o:OU) WHERE NOT ()<-[:Contains]-(o) RETURN o.name AS `Empty Organizational Unit`, COALESCE(o.distinguishedname, '-') AS `Full Reference`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "has_sid_history": {
        "name": "Objects that have a SID History",
        "request": "MATCH (a)-[r:HasSIDHistory]->(b) RETURN a.name AS `Has SID History`, LABELS(a) AS `Type_a`, b.name AS `Target`, LABELS(b) AS `Type_b`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "unpriv_users_to_GPO_init": {
        "name": "Initialization request for GPOs [WARNING: If this query is too slow, you can use --gpo_low]",
        "request": "MATCH (n:User{path_candidate:true}) WITH n ORDER BY n.name SKIP PARAM1 LIMIT PARAM2 MATCH p = shortestPath((n)-[r:MemberOf|AddSelf|WriteSPN|AddKeyCredentialLink|AddMember|AllExtendedRights|ForceChangePassword|GenericAll|GenericWrite|WriteDacl|WriteOwner|Owns*1..]->(g:GPO)) WHERE NOT n=g AND NOT g.name IS NULL RETURN p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target:GPO) CALL gds.allShortestPaths.dijkstra.stream('graph_unpriv_users_to_GPO_init', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path, costs WITH nodes(path)[-1] AS starting_node, path, costs WHERE starting_node:User AND target <> starting_node AND starting_node.path_candidate = TRUE RETURN [n in nodes(path) | ID(n)] AS nodeIds, costs",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_unpriv_users_to_GPO_init', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:MemberOf|AddSelf|WriteSPN|AddKeyCredentialLink|AddMember|AllExtendedRights|ForceChangePassword|GenericAll|GenericWrite|WriteDacl|WriteOwner|Owns]->(m) WHERE NOT (m:Domain OR n:Domain) AND NOT (n.is_dag=true or m.is_dag=true) AND NOT (n.is_da=true or m.is_da=true) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": ["azure_set_apps_name"],
    },
    "unpriv_users_to_GPO_user_enforced": {
        "name": "Compromisable GPOs to users (enforced)",
        "request": "MATCH (n:User{enabled:true}) WHERE n.name IS NOT NULL WITH n ORDER BY ID(n) SKIP PARAM1 LIMIT PARAM2 MATCH p = (g:GPO{dangerous_inbound:true})-[r1:GPLink {enforced:true}]->(container2)-[r2:Contains*1..]->(n) RETURN p",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "unpriv_users_to_GPO_user_not_enforced": {
        "name": "Compromisable GPOs to users (not enforced)",
        "request": "MATCH (n:User{enabled:true}) WHERE n.name IS NOT NULL WITH n ORDER BY ID(n) SKIP PARAM1 LIMIT PARAM2 MATCH p = (g:GPO{dangerous_inbound:true})-[r1:GPLink{enforced:false}]->(container1)-[r2:Contains*1..]->(n) WHERE NONE(x in NODES(p) WHERE x.blocksinheritance = true AND (x:OU)) RETURN p",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "unpriv_users_to_GPO_computer_enforced": {
        "name": "Compromisable GPOs to computers (enforced)",
        "request": "MATCH (n:Computer) WITH n ORDER BY ID(n) WITH n SKIP PARAM1 LIMIT PARAM2 MATCH p = (g:GPO{dangerous_inbound:true})-[r1:GPLink {enforced:true}]->(container2)-[r2:Contains*1..]->(n) RETURN p",
        "is_write": False,
        "is_gds": False,
    },
    "unpriv_users_to_GPO_computer_not_enforced": {
        "name": "Compromisable GPOs to computers (not enforced)",
        "request": "MATCH (n:Computer) WITH n ORDER BY ID(n) WITH n SKIP PARAM1 LIMIT PARAM2 MATCH p = (g:GPO{dangerous_inbound:true})-[r1:GPLink{enforced:false}]->(container1)-[r2:Contains*1..]->(n) WHERE NONE(x in NODES(p) WHERE x.blocksinheritance = true AND (x:OU)) RETURN p",
        "is_write": False,
        "is_gds": False,
    },
    "unpriv_users_to_GPO": {
        "name": "Non privileged users to GPO",
        "request": "MATCH (g:GPO) WITH g ORDER BY ID(g) SKIP PARAM1 LIMIT PARAM2 OPTIONAL MATCH (g)-[r1:GPLink {enforced:false}]->(container1) WITH g,container1 OPTIONAL MATCH (g)-[r2:GPLink {enforced:true}]->(container2) WITH g,container1,container2 OPTIONAL MATCH p = (g)-[r1:GPLink]->(container1)-[r2:Contains*1..8]->(n1:Computer) WHERE NONE(x in NODES(p) WHERE x.blocksinheritance = true AND (x:OU)) WITH g,p,container2,n1 OPTIONAL MATCH p2 = (g)-[r1:GPLink]->(container2)-[r2:Contains*1..8]->(n2:Computer) RETURN p",
        "is_write": False,
        "is_gds": False,
    },
    "cross_domain_local_admins": {
        "name": "Users that are local admins cross-domain",
        "request": "MATCH p=(u{enabled:true})-[r:MemberOf*0..4]->()-[rr:AdminTo]->(c:Computer) WHERE c.ghost_computer IS NULL AND u.domain <> c.domain AND NOT c.domain CONTAINS u.domain RETURN DISTINCT p",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_ghost_computer",
            "set_upper_domain_name",
        ],
    },
    "cross_domain_domain_admins": {
        "name": "Users that are domain admins cross-domain",
        "request": "MATCH p=(u{enabled:true})-[r:MemberOf*1..4]->(g:Group{is_da:true}) WHERE u.domain <> g.domain AND NOT g.domain CONTAINS u.domain return p",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "primaryGroupID_lower_than_1000": {
        "name": "User with PrimaryGroupID lower than 1000",
        "request": 'MATCH (n) WHERE (n:Group OR n:User) AND toInteger(split(n.objectid, "-")[-1]) < 1000 AND (n.enabled = true or n:Group) return toInteger(split(n.objectid, "-")[-1]) as sid, n.name, n.domain, n.is_da',
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "pre_windows_2000_compatible_access_group": {
        "name": "Pre-Windows 2000 Compatible Access contains unauthenticated users",
        "request": 'MATCH (n:Group) WHERE n.name STARTS WITH "PRE-WINDOWS 2000 COMPATIBLE ACCESS@" MATCH (m)-[r:MemberOf]->(n) WHERE NOT m.objectid ENDS WITH "-S-1-5-11" return m.domain, m.name, m.objectid, labels(m) as type',
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "guest_accounts": {
        "name": "Guest accounts enabled",
        "request": 'MATCH (n:User) WHERE n.objectid ENDS WITH "-501" RETURN n.name, n.domain, n.enabled',
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "unpriviledged_users_with_admincount": {
        "name": "Unpriviledged users with admincount=1",
        "request": "MATCH (u:User{enabled:true}) WHERE u.is_da=false AND u.admincount=true RETURN u.name, u.domain, u.da_type",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_da",
            "set_dag",
            "set_dagg",
            "set_domain_attributes_to_domains",
            "set_msol",
            "set_nonda",
            "set_upper_domain_name",
        ],
    },
    "get_fgpp": {
        "name": "FGPP applied to users directly or via groups",
        "request": "MATCH (u:User) WHERE u.fgpp_name IS NOT NULL RETURN u.fgpp_msds_psoappliesto, u.fgpp_name, u.fgpp_msds_minimumpasswordlength, u.fgpp_msds_minimumpasswordage, u.fgpp_msds_maximumpasswordage, u.fgpp_msds_passwordreversibleencryptionenabled, u.fgpp_msds_passwordhistorylength, u.fgpp_msds_passwordcomplexityenabled, u.fgpp_msds_lockoutduration, u.fgpp_msds_lockoutthreshold, u.fgpp_msds_lockoutobservationwindow",
        "is_write": False,
        "is_gds": False,
    },
    "esc15_adcs_privilege_escalation": {
        "name": "Check for potential ESC15 attacks (ADCS privilege escalation) and create associated edge",
        "request": "MATCH p=(x:Base)-[:MemberOf*0..]->()-[:Enroll|AllExtendedRights]->(ct:CertTemplate)-[:PublishedTo]->(:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) WHERE ct.enrolleesuppliessubject = True AND ct.authenticationenabled = False AND ct.requiresmanagerapproval = False AND ct.schemaversion = 1 CREATE (x)-[:ADCSESC15]->(d)",
        "is_write": True,
        "is_gds": False,
    },
    "smb_signing": {
        "name": "SMB signing",
        "request": "MATCH (c:Computer) RETURN c.name AS name, c.domain AS domain, c.smbsigning AS smbsigning, c.is_dc AS dc, c.is_server AS server, toInteger(($extract_date$ - c.lastlogontimestamp)/86400) AS lastlogontimestamp",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_dc",
            "set_domain_attributes_to_domains",
            "set_non_server",
            "set_nondc",
            "set_server",
            "set_upper_domain_name",
        ],
    },
    "ldap_server_configuration": {
        "name": "LDAP and LDAPS configuration",
        "request": "MATCH (c) WHERE c.ldapavailable OR c.ldapsavailable RETURN c.name AS name, c.domain AS domain, c.ldapavailable AS ldap, c.ldapsavailable AS ldaps, c.ldapsigning AS ldapsigning, c.ldapsepa AS ldapsepa",
        "is_write": False,
        "is_gds": False,
        "depends_on": [
            "azure_set_apps_name",
            "check_if_all_group_objects_have_domain_attribute",
            "set_domain_attributes_to_domains",
            "set_upper_domain_name",
        ],
    },
    "azure_set_gag": {
        "name": "Set gag=TRUE to Global admin group",
        "request": "MATCH (a:AZRole) WHERE a.name STARTS WITH 'GLOBAL ADMINISTRATOR@' SET a.is_gag=TRUE",
        "is_write": True,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_user": {
        "name": "Get Azure Users",
        "request": "MATCH (n:AZUser) RETURN n.name AS Name, n.tenantid AS `Tenant ID`, n.onpremisesyncenabled AS onpremisesynced, n.onpremisesecurityidentifier AS SID",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_admin": {
        "name": "Get Azure Admins",
        "request": "MATCH p =(n)-[r:AZGlobalAdmin*1..]->(m) RETURN n.name AS Name, n.tenantid AS `Tenant ID`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_groups": {
        "name": "get Azure Groups",
        "request": "MATCH (n:AZGroup) RETURN n.tenantid AS `Tenant ID`, n.name AS Name, COALESCE(n.description, '-') AS Description",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_vm": {
        "name": "get Azure VM",
        "request": "MATCH (n:AZVM) RETURN n.tenantid AS `Tenant ID`, n.name AS Name, n.operatingsystem AS os",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_apps": {
        "name": "get Azure Apps",
        "request": "MATCH (n:AZApp) WHERE n.name IS NOT NULL AND SIZE(n.name) > 1 RETURN n.tenantid AS `Tenant ID`, n.name AS Name",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_devices": {
        "name": "get Azure Devices",
        "request": "MATCH (n:AZDevice) RETURN n.tenantid AS `Tenant ID`, n.name AS Name, n.operatingsystem AS os",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_users_paths_high_target": {
        "name": "Find all Azure Users with a Path to High Value Targets ",
        "request": "MATCH (n:AZBase{is_priv:false}) WITH n ORDER BY n.name SKIP PARAM1 LIMIT PARAM2 MATCH p=shortestPath((n)-[r:$properties$*1..$recursive_level$]->(m:AZBase{is_priv:true})) WHERE m<>n RETURN p",
        "is_write": False,
        "is_gds": True,
        "gds_request": "MATCH (target:AZBase{is_priv:true}) CALL gds.allShortestPaths.dijkstra.stream('graph_azure_users_paths_high_target', {sourceNode: target, relationshipWeightProperty: 'cost', logProgress: false}) YIELD path WITH nodes(path)[-1] AS starting_node, path WHERE starting_node.is_priv = FALSE AND starting_node:AZBase RETURN path as p",
        "create_gds_graph": "CALL gds.graph.project.cypher('graph_azure_users_paths_high_target', 'MATCH (n) RETURN id(n) AS id', 'MATCH (n)-[r:$properties$]->(m) RETURN id(m) as source, id(n) AS target, r.cost as cost', {validateRelationships: false})",
        "unsupported_features": ["GDS (Graph Data Science)"],
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_ms_graph_controllers": {
        "name": "Return all direct Controllers of MS Graph",
        "request": 'MATCH p = (n)-[r:AZAddOwner|AZAddSecret|AZAppAdmin|AZCloudAppAdmin|AZMGAddOwner|AZMGAddSecret|AZOwns]->(g:AZServicePrincipal {appdisplayname: "Microsoft Graph"}) RETURN p',
        "is_write": False,
        "is_gds": False,
    },
    "azure_aadconnect_users": {
        "name": "Return all Users and Azure Users possibly related to AADConnect",
        "request": "MATCH (u) WHERE (u:User OR u:AZUser) AND (u.name =~ '(?i)^MSOL_|.*AADConnect.*' OR u.userprincipalname =~ '(?i)^sync_.*') OPTIONAL MATCH (u)-[:HasSession]->(s:Session) RETURN u.name AS Name, s AS Session, u.tenantid AS `Tenant ID`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_admin_on_prem": {
        "name": "Return all Azure admins that are also on premise admins",
        "request": "MATCH (u:User{is_da:true})-[:SyncedToEntraUser]->(a:AZUser)-[r:AZGlobalAdmin]->() RETURN u.name as Name",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_role_listing": {
        "name": "List of all Azure roles",
        "request": "MATCH (a:AZRole) return distinct a.name AS Name, a.description AS Description",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_role_paths": {
        "name": "Paths to the Azure roles",
        "request": "MATCH p=(a:AZUser)-[r:AZHasRole]->(x) return distinct p",
        "is_write": False,
        "is_gds": False,
    },
    "azure_reset_passwd": {
        "name": "Azure accounts that can reset passwords",
        "request": "MATCH (m:AZBase) WITH m ORDER BY ID(m) SKIP PARAM1 LIMIT PARAM2 MATCH p=(n)-[r:AZResetPassword]->(m) return distinct p",
        "is_write": False,
        "is_gds": False,
    },
    "azure_last_passwd_change": {
        "name": "Last password change on Azure and on premise",
        "request": "MATCH (u:User {enabled:TRUE}),(a:AZUser) WHERE a.onpremisesecurityidentifier = u.objectid RETURN DISTINCT(u.name) AS Name, toInteger(($extract_date$ - u.pwdlastset )/ 86400) AS `Last password set on premise`, toInteger(($extract_date$ - (datetime('1970-01-01T00:00:00').epochMillis + duration.inSeconds(datetime('1970-01-01T00:00:00'), a.pwdlastset).seconds)) / 86400) AS `Last password set on Azure`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_dormant_accounts": {
        "name": "Azure dormant accounts",
        "request": "MATCH (a:AZUser)-[:SyncedToADUser]->(u:User{enabled:TRUE}) RETURN a.name AS Name, toInteger(($extract_date$ - u.lastlogontimestamp)/86400) AS lastlogon, toInteger(($extract_date$ - u.whencreated)/86400) AS whencreated",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_accounts_disabled_on_prem": {
        "name": "Azure accounts that are disabled on premise",
        "request": "MATCH (a:AZUser{enabled:TRUE})-[:SyncedToADUser]->(u:User{enabled:FALSE}) RETURN a.name AS `Azure name`, a.enabled AS `Enabled on Azure`, u.name AS `On premise name`, u.enabled AS `Enabled on premise` UNION MATCH (a:AZUser{enabled:FALSE})-[:SyncedToADUser]->(u:User{enabled:TRUE}) RETURN a.name AS `Azure name`, a.enabled AS `Enabled on Azure`, u.name AS `On premise name`, u.enabled AS `Enabled on premise`",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_accounts_not_found_on_prem": {
        "name": "Azure accounts that are synced but do not exist on premise",
        "request": "MATCH (azUser:AZUser{onpremisesyncenabled:true}) WHERE NOT EXISTS {MATCH (user:User) WHERE user.objectid = azUser.onpremisesecurityidentifier} return azUser.name AS Name",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_tenants": {
        "name": "Get all Azure tenants",
        "request": "MATCH (t:AZTenant) RETURN t.name AS Name, t.tenantid AS ID",
        "is_write": False,
        "is_gds": False,
        "depends_on": ["azure_set_apps_name"],
    },
    "azure_ga_to_ga": {
        "name": "Paths between two global admins belonging to different tenants",
        "request": "MATCH p=allShortestPaths((g:AZRole{is_gag:TRUE})-[r:$properties$*1..$recursive_level$]->(gg:AZRole{is_gag:TRUE})) WHERE g<>gg AND g.tenantid <> gg.tenantid RETURN p",
        "is_write": False,
        "is_gds": False,
    },
    "azure_cross_ga_da": {
        "name": "Paths between tenants admin and domain admins",
        "request": "MATCH p=allShortestPaths((g:AZRole{is_gag:TRUE})-[r:$properties$*1..$recursive_level$]->(gg:Group{is_dag:TRUE})) RETURN p UNION MATCH p=allShortestPaths((gg:Group{is_dag:TRUE})-[r:$properties$*1..$recursive_level$]->(g:AZRole{is_gag:TRUE})) RETURN p",
        "is_write": False,
        "is_gds": False,
    },
}


# ---------------------------------------------------------------------------
# Known compatibility issues per query
# ---------------------------------------------------------------------------
# Each entry maps a query key to a list of reasons why it fails in kglite.
# These are used to mark tests as xfail with informative messages.

KNOWN_ISSUES = {
    # SHOW PROCEDURES is not a supported Cypher clause in kglite.
    "check_if_GDS_installed": ["SHOW PROCEDURES not supported"],
    # 'Contains' is a reserved Cypher keyword (string comparison operator) and
    # cannot be used as a relationship type name in MATCH patterns.
    "domain_OUs": ["Contains is reserved keyword in kglite parser"],
    "get_empty_ous": ["Contains is reserved keyword in kglite parser"],
    "vulnerable_OU_impact": ["Contains is reserved keyword in kglite parser"],
    # {prop:bool} without space/quotes and MATCH Contains both work fine in
    # kglite. Multi-type relationship patterns ([:A|B]) also parse correctly.
    # These were initially thought to be issues but are actually supported.
    # Variable aliased via WITH AS is not recognized as a node in SET clause.
    "set_groups_members_count": ["Aliased variable not bound to node in SET"],
    "set_groups_members_count_computers": ["Aliased variable not bound to node in SET"],
    "set_default_exploitability_rating": ["Relationship variable not bound to node in SET"],
    # Contains is a reserved keyword — these queries use Contains in path patterns.
    "unpriv_users_to_GPO_user_enforced": ["Contains in path pattern (reserved keyword)"],
    "unpriv_users_to_GPO_user_not_enforced": ["Contains in path pattern (reserved keyword)"],
    "unpriv_users_to_GPO_computer_enforced": ["Contains in path pattern (reserved keyword)"],
    "unpriv_users_to_GPO_computer_not_enforced": ["Contains in path pattern (reserved keyword)"],
    "unpriv_users_to_GPO": ["Contains in path pattern (reserved keyword)"],
    # NOT o:Group label syntax (bare :Label after NOT) not supported.
    "objects_to_adcs": ["NOT node:Label syntax not supported"],
    "can_read_laps": ["NOT node:Label syntax not supported"],
    # Nested pattern syntax in ReadGMSAPassword path.
    "can_read_gmsapassword_of_adm": ["Nested pattern syntax not supported"],
    # datetime() function with duration arithmetic not supported.
    "azure_last_passwd_change": ["datetime() / duration function syntax not supported"],
    # GDS queries that also fail parsing independently (in addition to GDS skip).
    "users_shadow_credentials_to_non_admins": ["GDS + AND keyword parsed in MATCH pattern context"],
    "kud": ["GDS + OR keyword parsed in MATCH pattern context"],
    # Queries with {prop:bool} that also have other issues making them fail
    # on execution (the map syntax itself works, but other parts don't).
    "set_ou_candidate": ["Complex nested boolean with label predicates"],
    "set_path_candidate": ["{is_da_dc:false} with unquoted bool in node match"],
    "set_target_kud": ["{unconstraineddelegation:true} requires is_dc property from prior SET"],
    "anomaly_acl_1": ["AND in relationship type pattern parsed as keyword"],
    "primaryGroupID_lower_than_1000": ["primarygroupid property comparison"],
    "azure_aadconnect_users": ["onpremisesyncenabled property syntax"],
}


def _xfail_if_known(query_key):
    """Return an xfail marker if the query has known issues, else None."""
    if query_key in KNOWN_ISSUES:
        reasons = "; ".join(KNOWN_ISSUES[query_key])
        return pytest.mark.xfail(reason=f"Known issue: {reasons}", strict=False)
    return None


# Dependency-ordered execution phases
PHASE_0_CLEANUP = ["delete_orphans", "preparation_request_nodes", "delete_unresolved", "preparation_request_relations"]

PHASE_1_SET_QUERIES = [
    "azure_set_apps_name",
    "check_if_all_group_objects_have_domain_attribute",
    "set_upper_domain_name",
    "set_domain_attributes_to_domains",
    "check_if_all_group_objects_have_domain_attribute",
    "set_upper_domain_name",
    "set_server",
    "set_non_server",
    "set_dc",
    "set_nondc",
    "set_dcg",
    "set_nondcg",
    "set_isacl_adcs",
    "set_is_adminsdholder",
    "set_is_dnsadmin",
    "set_da",
    "set_msol",
    "set_da_types",
    "set_dag",
    "set_dag_types",
    "set_dagg",
    "set_dagg_types",
    "set_daggg",
    "set_dac",
    "set_dac_types",
    "set_nonda",
    "set_nondag",
    "set_is_group_operator",
    "set_is_operator_member",
    "set_dcsync1",
    "set_dcsync2",
    "set_ou_candidate",
    "set_containsda",
    "set_containsdc",
    "set_is_da_dc",
    "set_is_not_da_dc",
    "set_is_adcs",
    "set_path_candidate",
    "set_groups_members_count",
    "set_groups_members_count_computers",
    "set_groups_has_members",
    "set_gpo_links_count",
    "set_gpos_has_links",
    "set_groups_direct_admin",
    "set_groups_indirect_admin_1",
    "set_groups_indirect_admin_2",
    "set_groups_indirect_admin_3",
    "set_user_indirect_admin",
    "set_users_direct_admin",
    "set_groups_indirect_admin_4",
    "set_groups_indirect_admin_3",
    "set_groups_indirect_admin_2",
    "set_target_kud",
    "set_az_privileged",
    "set_az_not_privileged",
    "set_ghost_computer",
    "set_default_exploitability_rating",
    "objects_to_domain_admin",
    "graph_rbcd",
    "compromise_paths_of_OUs",
    "azure_set_gag",
]

PHASE_2_READ_QUERIES = [
    "check_relation_types",
    "check_if_all_domain_objects_exist",
    "onpremid_ompremsesecurityidentifier",
    "set_can_extract_dc_secrets",
    "set_can_load_code",
    "set_can_logon_dc",
    "dcsync_list",
    "nb_domain_collected",
    "get_all_nodes",
    "check_unknown_relations",
    "domains",
    "nb_domain_controllers",
    "domain_OUs",
    "users_shadow_credentials",
    "nb_enabled_accounts",
    "nb_disabled_accounts",
    "nb_groups",
    "nb_computers",
    "computers_not_connected_since",
    "nb_domain_admins",
    "os",
    "krb_pwd_last_change",
    "nb_kerberoastable_accounts",
    "nb_as-rep_roastable_accounts",
    "nb_computer_unconstrained_delegations",
    "nb_users_unconstrained_delegations",
    "users_constrained_delegations",
    "dormant_accounts",
    "password_last_change",
    "nb_user_password_cleartext",
    "get_users_password_not_required",
    "objects_admincount",
    "user_password_never_expires",
    "computers_members_high_privilege",
    "objects_to_adcs",
    "users_admin_on_computers",
    "users_admin_on_servers_1",
    "users_admin_on_servers_2",
    "computers_admin_on_computers",
    "domain_map_trust",
    "nb_computers_laps",
    "can_read_laps",
    "dom_admin_on_non_dc",
    "rdp_access",
    "dc_impersonation",
    "graph_rbcd_to_da",
    "vuln_functional_level",
    "vuln_sidhistory_dangerous",
    "can_read_gmsapassword_of_adm",
    "objects_to_operators_groups",
    "da_to_da",
    "anomaly_acl_1",
    "anomaly_acl_2",
    "get_empty_groups",
    "get_empty_ous",
    "has_sid_history",
    "unpriv_users_to_GPO_user_enforced",
    "unpriv_users_to_GPO_user_not_enforced",
    "unpriv_users_to_GPO_computer_enforced",
    "unpriv_users_to_GPO_computer_not_enforced",
    "unpriv_users_to_GPO",
    "cross_domain_local_admins",
    "cross_domain_domain_admins",
    "primaryGroupID_lower_than_1000",
    "pre_windows_2000_compatible_access_group",
    "guest_accounts",
    "unpriviledged_users_with_admincount",
    "get_fgpp",
    "esc15_adcs_privilege_escalation",
    "smb_signing",
    "ldap_server_configuration",
    "azure_user",
    "azure_admin",
    "azure_groups",
    "azure_vm",
    "azure_apps",
    "azure_devices",
    "azure_ms_graph_controllers",
    "azure_aadconnect_users",
    "azure_admin_on_prem",
    "azure_role_listing",
    "azure_role_paths",
    "azure_reset_passwd",
    "azure_last_passwd_change",
    "azure_dormant_accounts",
    "azure_accounts_disabled_on_prem",
    "azure_accounts_not_found_on_prem",
    "azure_tenants",
    "azure_ga_to_ga",
    "azure_cross_ga_da",
]

GDS_QUERIES = [
    "users_shadow_credentials_to_non_admins",
    "objects_to_domain_admin",
    "kud",
    "objects_to_dcsync",
    "unpriv_to_dnsadmins",
    "compromise_paths_of_OUs",
    "vulnerable_OU_impact",
    "objects_to_operators_member",
    "vuln_permissions_adminsdholder",
    "unpriv_users_to_GPO_init",
    "azure_users_paths_high_target",
]


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def bloodhound_graph():
    """Minimal BloodHound-like graph for AD_Miner query testing.

    Creates a small AD-like structure with domains, users, groups, computers,
    OUs, GPOs, and standard BloodHound relationships.

    All nodes are created via the DataFrame API so they have proper _id fields,
    which is required for add_connections to work. The 'Contains' relationship
    type is a reserved keyword in kglite's Cypher parser, so it must be created
    via the DataFrame API rather than Cypher CREATE.
    """
    import pandas as pd

    g = rg.KnowledgeGraph()

    # Domains
    domains = pd.DataFrame(
        {
            "objectid": ["S-1-5-21-1234"],
            "name": ["TESTLAB.LOCAL"],
            "domain": ["TESTLAB.LOCAL"],
            "functionallevel": ["2016"],
        }
    )
    g.add_nodes(domains, "Domain", "objectid", "name")

    # Users
    users = pd.DataFrame(
        {
            "objectid": [
                "S-1-5-21-1234-500",
                "S-1-5-21-1234-1001",
                "S-1-5-21-1234-1002",
                "S-1-5-21-1234-1003",
            ],
            "name": [
                "ADMIN@TESTLAB.LOCAL",
                "JDOE@TESTLAB.LOCAL",
                "SVCACCT@TESTLAB.LOCAL",
                "DISABLED@TESTLAB.LOCAL",
            ],
            "domain": ["TESTLAB.LOCAL"] * 4,
            "enabled": [True, True, True, False],
            "admincount": [True, False, False, False],
            "pwdlastset": [18900, 18800, 18000, 17000],
            "lastlogon": [19000, 18500, 19000, 17000],
            "pwdneverexpires": [False, True, False, False],
            "sensitive": [False, False, False, False],
            "hasspn": [False, True, False, False],
            "dontreqpreauth": [False, False, True, False],
            "unconstraineddelegation": [False, False, False, False],
            "passwordnotreqd": [False, False, False, False],
        }
    )
    g.add_nodes(users, "User", "objectid", "name")

    # Groups
    groups = pd.DataFrame(
        {
            "objectid": [
                "S-1-5-21-1234-512",
                "S-1-5-21-1234-516",
                "S-1-5-21-1234-519",
                "S-1-5-21-1234-551",
                "S-1-5-21-1234-549",
                "S-1-5-21-1234-550",
                "S-1-5-21-1234-548",
                "S-1-5-21-1234-1101",
                "S-1-5-21-1234-518",
            ],
            "name": [
                "DOMAIN ADMINS@TESTLAB.LOCAL",
                "DOMAIN CONTROLLERS@TESTLAB.LOCAL",
                "ENTERPRISE ADMINS@TESTLAB.LOCAL",
                "BACKUP OPERATORS@TESTLAB.LOCAL",
                "SERVER OPERATORS@TESTLAB.LOCAL",
                "PRINT OPERATORS@TESTLAB.LOCAL",
                "ACCOUNT OPERATORS@TESTLAB.LOCAL",
                "DNSADMINS@TESTLAB.LOCAL",
                "SCHEMA ADMINS@TESTLAB.LOCAL",
            ],
            "domain": ["TESTLAB.LOCAL"] * 9,
            "admincount": [True, False, False, False, False, False, False, False, False],
        }
    )
    g.add_nodes(groups, "Group", "objectid", "name")

    # Computers
    computers = pd.DataFrame(
        {
            "objectid": ["S-1-5-21-1234-1000", "S-1-5-21-1234-1100"],
            "name": ["DC01.TESTLAB.LOCAL", "WS01.TESTLAB.LOCAL"],
            "domain": ["TESTLAB.LOCAL", "TESTLAB.LOCAL"],
            "operatingsystem": ["Windows Server 2019", "Windows 10 Enterprise"],
            "enabled": [True, True],
            "lastlogontimestamp": [19000, 18900],
            "haslaps": [True, False],
            "unconstraineddelegation": [True, False],
        }
    )
    g.add_nodes(computers, "Computer", "objectid", "name")

    # OUs
    ous = pd.DataFrame(
        {
            "objectid": ["OU-1", "OU-2"],
            "name": ["SERVERS@TESTLAB.LOCAL", "WORKSTATIONS@TESTLAB.LOCAL"],
            "domain": ["TESTLAB.LOCAL", "TESTLAB.LOCAL"],
        }
    )
    g.add_nodes(ous, "OU", "objectid", "name")

    # GPOs
    gpos = pd.DataFrame(
        {
            "objectid": ["GPO-1"],
            "name": ["DEFAULT DOMAIN POLICY@TESTLAB.LOCAL"],
            "domain": ["TESTLAB.LOCAL"],
        }
    )
    g.add_nodes(gpos, "GPO", "objectid", "name")

    # Containers
    containers = pd.DataFrame(
        {
            "objectid": ["CN-1"],
            "name": ["ADMINSDHOLDER@TESTLAB.LOCAL"],
            "domain": ["TESTLAB.LOCAL"],
        }
    )
    g.add_nodes(containers, "Container", "objectid", "name")

    # --- Relationships ---

    # MemberOf (User -> Group)
    memberof_user = pd.DataFrame(
        {
            "src": ["S-1-5-21-1234-500"],
            "tgt": ["S-1-5-21-1234-512"],
        }
    )
    g.add_connections(memberof_user, "MemberOf", "User", "src", "Group", "tgt")

    # MemberOf (Computer -> Group)
    memberof_comp = pd.DataFrame(
        {
            "src": ["S-1-5-21-1234-1000"],
            "tgt": ["S-1-5-21-1234-516"],
        }
    )
    g.add_connections(memberof_comp, "MemberOf", "Computer", "src", "Group", "tgt")

    # AdminTo
    adminto = pd.DataFrame(
        {
            "src": ["S-1-5-21-1234-500", "S-1-5-21-1234-500", "S-1-5-21-1234-1001"],
            "tgt": ["S-1-5-21-1234-1000", "S-1-5-21-1234-1100", "S-1-5-21-1234-1100"],
        }
    )
    g.add_connections(adminto, "AdminTo", "User", "src", "Computer", "tgt")

    # Contains (reserved keyword in Cypher parser — must use DataFrame API)
    contains_ou = pd.DataFrame(
        {
            "src": ["OU-1", "OU-2"],
            "tgt": ["S-1-5-21-1234-1000", "S-1-5-21-1234-1100"],
        }
    )
    g.add_connections(contains_ou, "Contains", "OU", "src", "Computer", "tgt")

    contains_domain = pd.DataFrame(
        {
            "src": ["S-1-5-21-1234"],
            "tgt": ["S-1-5-21-1234-512"],
        }
    )
    g.add_connections(contains_domain, "Contains", "Domain", "src", "Group", "tgt")

    # GpLink
    gplink = pd.DataFrame(
        {
            "src": ["GPO-1"],
            "tgt": ["OU-1"],
        }
    )
    g.add_connections(gplink, "GpLink", "GPO", "src", "OU", "tgt")

    # CanRDP
    canrdp = pd.DataFrame(
        {
            "src": ["S-1-5-21-1234-1001"],
            "tgt": ["S-1-5-21-1234-1100"],
        }
    )
    g.add_connections(canrdp, "CanRDP", "User", "src", "Computer", "tgt")

    # TrustedBy (self-trust for testing)
    trust = pd.DataFrame(
        {
            "src": ["S-1-5-21-1234"],
            "tgt": ["S-1-5-21-1234"],
        }
    )
    g.add_connections(trust, "TrustedBy", "Domain", "src", "Domain", "tgt")

    return g


# ---------------------------------------------------------------------------
# Helper: build pytest params with xfail markers for known issues
# ---------------------------------------------------------------------------


def _make_params(query_keys):
    """Create pytest.param entries for query keys, with xfail for known issues."""
    params = []
    for key in query_keys:
        if key not in ADMINER_QUERIES:
            continue
        marks = []
        if key in KNOWN_ISSUES:
            reasons = "; ".join(KNOWN_ISSUES[key])
            marks.append(pytest.mark.xfail(reason=f"Known: {reasons}", strict=False))
        params.append(pytest.param(key, id=key, marks=marks))
    return params


# All non-template, non-GDS query keys for parse testing
ALL_QUERY_KEYS = [k for k in ADMINER_QUERIES]

GDS_SKIP_KEYS = {k for k in ADMINER_QUERIES if ADMINER_QUERIES[k].get("is_gds")}


# ---------------------------------------------------------------------------
# Parse tests: verify kglite can parse each query without error
# ---------------------------------------------------------------------------


class TestAdMinerQueryParsing:
    """Test that kglite can parse all AD_Miner Cypher queries.

    These tests verify parsing only, not execution. Queries that use
    unsupported features (GDS, SHOW PROCEDURES) are marked as xfail.
    """

    @pytest.mark.parametrize("query_key", _make_params(ALL_QUERY_KEYS))
    def test_parse(self, query_key):
        query = substitute_adminer_templates(ADMINER_QUERIES[query_key]["request"])
        g = rg.KnowledgeGraph()
        g.cypher(query)


# ---------------------------------------------------------------------------
# Execution tests: run queries on a minimal BloodHound graph
# ---------------------------------------------------------------------------


class TestAdMinerPhase0Cleanup:
    """Phase 0: Cleanup/setup queries (DETACH DELETE, REMOVE)."""

    @pytest.mark.parametrize("query_key", _make_params(PHASE_0_CLEANUP))
    def test_exec(self, query_key, bloodhound_graph):
        query = substitute_adminer_templates(ADMINER_QUERIES[query_key]["request"])
        bloodhound_graph.cypher(query)


class TestAdMinerPhase1SetQueries:
    """Phase 1: SET queries that create temporary properties.

    These are ordered by dependency. Each test runs the query on a fresh
    bloodhound_graph fixture.
    """

    @pytest.mark.parametrize("query_key", _make_params(PHASE_1_SET_QUERIES))
    def test_exec(self, query_key, bloodhound_graph):
        query = substitute_adminer_templates(ADMINER_QUERIES[query_key]["request"])
        bloodhound_graph.cypher(query)


class TestAdMinerPhase2ReadQueries:
    """Phase 2: Read queries that consume properties set in Phase 1."""

    @pytest.mark.parametrize("query_key", _make_params(PHASE_2_READ_QUERIES))
    def test_exec(self, query_key, bloodhound_graph):
        query = substitute_adminer_templates(ADMINER_QUERIES[query_key]["request"])
        bloodhound_graph.cypher(query)


class TestAdMinerGDSQueries:
    """GDS queries -- skipped as kglite does not support Neo4j Graph Data Science."""

    @pytest.mark.parametrize(
        "query_key",
        [pytest.param(k, id=k, marks=pytest.mark.skip(reason="GDS not supported")) for k in GDS_QUERIES],
    )
    def test_exec(self, query_key, bloodhound_graph):
        query = substitute_adminer_templates(ADMINER_QUERIES[query_key]["request"])
        bloodhound_graph.cypher(query)
