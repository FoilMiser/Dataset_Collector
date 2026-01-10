"""
collector_core/pipeline_specs_registry.py

Registry of all domain pipeline specifications.
This replaces the need for per-pipeline boilerplate files.
"""
from __future__ import annotations

from collector_core.pipeline_spec import PipelineSpec, register_pipeline


# === Scientific Domains ===

register_pipeline(PipelineSpec(
    domain="chem",
    name="Chemistry Pipeline",
    targets_yaml="targets_chem.yaml",
    routing_keys=["chem_routing", "math_routing"],
    routing_confidence_keys=["chem_routing"],
    default_routing={
        "subject": "chem",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_chem",
))

register_pipeline(PipelineSpec(
    domain="biology",
    name="Biology Pipeline",
    domain_prefix="bio",
    targets_yaml="targets_biology.yaml",
    routing_keys=["bio_routing", "math_routing"],
    routing_confidence_keys=["bio_routing"],
    default_routing={
        "subject": "biology",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="physics",
    name="Physics Pipeline",
    targets_yaml="targets_physics.yaml",
    routing_keys=["physics_routing", "math_routing"],
    routing_confidence_keys=["physics_routing"],
    default_routing={
        "subject": "physics",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="math",
    name="Mathematics Pipeline",
    targets_yaml="targets_math.yaml",
    routing_keys=["math_routing"],
    routing_confidence_keys=["math_routing"],
    default_routing={
        "subject": "math",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="earth",
    name="Earth Science Pipeline",
    targets_yaml="targets_earth.yaml",
    routing_keys=["earth_routing", "math_routing"],
    routing_confidence_keys=["earth_routing"],
    default_routing={
        "subject": "earth",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="materials_science",
    name="Materials Science Pipeline",
    domain_prefix="matsci",
    targets_yaml="targets_materials.yaml",
    routing_keys=["materials_routing", "math_routing"],
    routing_confidence_keys=["materials_routing"],
    default_routing={
        "subject": "materials_science",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))


# === Engineering & Technical Domains ===

register_pipeline(PipelineSpec(
    domain="engineering",
    name="Engineering Pipeline",
    targets_yaml="targets_engineering.yaml",
    routing_keys=["engineering_routing"],
    routing_confidence_keys=["engineering_routing"],
    default_routing={
        "subject": "engineering",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="code",
    name="Code Pipeline",
    targets_yaml="targets_code.yaml",
    routing_keys=["code_routing", "math_routing"],
    routing_confidence_keys=["code_routing", "math_routing"],
    default_routing={
        "subject": "code",
        "domain": "multi",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    custom_workers={"code_worker": "code_worker.py"},
))

register_pipeline(PipelineSpec(
    domain="cyber",
    name="Cybersecurity Pipeline",
    targets_yaml="targets_cyber.yaml",
    routing_keys=["cyber_routing", "math_routing"],
    routing_confidence_keys=["cyber_routing"],
    default_routing={
        "subject": "cyber",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    custom_workers={
        "nvd_worker": "nvd_worker.py",
        "stix_worker": "stix_worker.py",
        "advisory_worker": "advisory_worker.py",
    },
))

register_pipeline(PipelineSpec(
    domain="3d_modeling",
    name="3D Modeling Pipeline",
    domain_prefix="3d",
    targets_yaml="targets_3d.yaml",
    routing_keys=["three_d_routing"],
    routing_confidence_keys=["three_d_routing"],
    default_routing={
        "subject": "3d",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    custom_workers={"mesh_worker": "mesh_worker.py"},
))

register_pipeline(PipelineSpec(
    domain="metrology",
    name="Metrology Pipeline",
    targets_yaml="targets_metrology.yaml",
    routing_keys=["metrology_routing", "math_routing"],
    routing_confidence_keys=["metrology_routing"],
    default_routing={
        "subject": "metrology",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    include_routing_dict_in_row=True,
))


# === NLP & Knowledge Domains ===

register_pipeline(PipelineSpec(
    domain="nlp",
    name="NLP Pipeline",
    targets_yaml="targets_nlp.yaml",
    routing_keys=["nlp_routing"],
    routing_confidence_keys=["nlp_routing"],
    default_routing={
        "subject": "nlp",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_nlp",
))

register_pipeline(PipelineSpec(
    domain="logic",
    name="Logic Pipeline",
    targets_yaml="targets_logic.yaml",
    routing_keys=["logic_routing"],
    routing_confidence_keys=["logic_routing"],
    default_routing={
        "subject": "logic",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))

register_pipeline(PipelineSpec(
    domain="kg_nav",
    name="Knowledge Graph & Navigation Pipeline",
    targets_yaml="targets_kg_nav.yaml",
    routing_keys=["kg_routing", "math_routing"],
    routing_confidence_keys=["kg_routing"],
    default_routing={
        "subject": "kg_nav",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_kg_nav",
))


# === Economics & Policy Domains ===

register_pipeline(PipelineSpec(
    domain="econ_stats_decision_adaptation",
    name="Economics, Statistics, Decision & Adaptation Pipeline",
    domain_prefix="econ",
    targets_yaml="targets_econ_stats_decision_v2.yaml",
    routing_keys=["econ_routing", "math_routing"],
    routing_confidence_keys=["econ_routing"],
    default_routing={
        "subject": "econ",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_econ",
    include_routing_dict_in_row=True,
))

register_pipeline(PipelineSpec(
    domain="regcomp",
    name="Regulatory Compliance Pipeline",
    targets_yaml="targets_regcomp.yaml",
    routing_keys=["regcomp_routing"],
    routing_confidence_keys=["regcomp_routing"],
    default_routing={
        "subject": "regcomp",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))


# === Safety & Agriculture Domains ===

register_pipeline(PipelineSpec(
    domain="safety_incident",
    name="Safety Incident Pipeline",
    domain_prefix="safety",
    targets_yaml="targets_safety_incident.yaml",
    routing_keys=["safety_routing"],
    routing_confidence_keys=["safety_routing"],
    default_routing={
        "subject": "safety_incident",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
    yellow_screen_module="yellow_screen_safety",
))

register_pipeline(PipelineSpec(
    domain="agri_circular",
    name="Agriculture & Circular Economy Pipeline",
    targets_yaml="targets_agri_circular.yaml",
    routing_keys=["agri_routing", "math_routing"],
    routing_confidence_keys=["agri_routing"],
    default_routing={
        "subject": "agri_circular",
        "domain": "misc",
        "category": "misc",
        "level": 5,
        "granularity": "target"
    },
))
