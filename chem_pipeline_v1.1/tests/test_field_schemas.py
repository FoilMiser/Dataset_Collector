"""
test_field_schemas.py

Unit tests for field schema definitions and validation.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import yaml


def load_field_schemas():
    """Load the field_schemas.yaml for testing."""
    schemas_path = Path(__file__).parent.parent / "field_schemas.yaml"
    return yaml.safe_load(schemas_path.read_text(encoding="utf-8"))


class TestSchemaStructure:
    """Tests for field schema file structure."""

    def test_schema_version(self):
        """Field schemas should have a version."""
        fs = load_field_schemas()
        assert "schema_version" in fs

    def test_schemas_section_exists(self):
        """Schemas section should exist."""
        fs = load_field_schemas()
        assert "schemas" in fs
        assert isinstance(fs["schemas"], dict)


class TestPubChemSchema:
    """Tests for PubChem computed-only schema."""

    def test_pubchem_schema_exists(self):
        """PubChem computed-only schema should exist."""
        fs = load_field_schemas()
        assert "pubchem_computed_only_v1.0.0" in fs["schemas"]

    def test_pubchem_required_fields(self):
        """PubChem schema should have key computed fields."""
        fs = load_field_schemas()
        pubchem = fs["schemas"]["pubchem_computed_only_v1.0.0"]
        fields = pubchem.get("fields", {})

        # These are the essential computed fields
        assert "PUBCHEM_COMPOUND_CID" in fields
        assert "PUBCHEM_CACTVS_CANONICAL_SMILES" in fields
        assert "PUBCHEM_IUPAC_INCHI" in fields
        assert "PUBCHEM_IUPAC_INCHIKEY" in fields
        assert "PUBCHEM_MOLECULAR_FORMULA" in fields
        assert "PUBCHEM_MOLECULAR_WEIGHT" in fields

    def test_pubchem_cid_validation(self):
        """PubChem CID field should have proper validation."""
        fs = load_field_schemas()
        pubchem = fs["schemas"]["pubchem_computed_only_v1.0.0"]
        cid_field = pubchem["fields"]["PUBCHEM_COMPOUND_CID"]

        assert cid_field["type"] == "integer"
        assert cid_field["required"] == True
        assert cid_field["nullable"] == False
        assert cid_field["validation"]["min"] >= 1

    def test_pubchem_smiles_validation(self):
        """PubChem SMILES field should have proper validation."""
        fs = load_field_schemas()
        pubchem = fs["schemas"]["pubchem_computed_only_v1.0.0"]
        smiles_field = pubchem["fields"]["PUBCHEM_CACTVS_CANONICAL_SMILES"]

        assert smiles_field["type"] == "string"
        assert "validation" in smiles_field
        assert smiles_field["validation"]["max_length"] > 0

    def test_pubchem_inchikey_validation(self):
        """PubChem InChIKey field should have proper validation."""
        fs = load_field_schemas()
        pubchem = fs["schemas"]["pubchem_computed_only_v1.0.0"]
        inchikey_field = pubchem["fields"]["PUBCHEM_IUPAC_INCHIKEY"]

        assert inchikey_field["type"] == "string"
        assert "validation" in inchikey_field
        # InChIKey is always 27 characters
        assert inchikey_field["validation"].get("exact_length") == 27


class TestPMCSchema:
    """Tests for PMC chunk schema."""

    def test_pmc_schema_exists(self):
        """PMC chunk schema should exist."""
        fs = load_field_schemas()
        assert "pmc_chunk_v1.0.0" in fs["schemas"]

    def test_pmc_required_fields(self):
        """PMC schema should have key fields."""
        fs = load_field_schemas()
        pmc = fs["schemas"]["pmc_chunk_v1.0.0"]
        fields = pmc.get("fields", {})

        assert "pmcid" in fields
        assert "license_spdx" in fields
        assert "text" in fields
        assert "chunk_id" in fields

    def test_pmc_pmcid_validation(self):
        """PMC ID field should have proper validation."""
        fs = load_field_schemas()
        pmc = fs["schemas"]["pmc_chunk_v1.0.0"]
        pmcid_field = pmc["fields"]["pmcid"]

        assert pmcid_field["type"] == "string"
        assert pmcid_field["required"] == True
        # Should match PMC format
        assert "pattern" in pmcid_field["validation"]


class TestTypeConverters:
    """Tests for type converter documentation."""

    def test_type_converters_exist(self):
        """Type converter documentation should exist."""
        fs = load_field_schemas()
        assert "type_converters" in fs

        converters = fs["type_converters"]
        assert "integer" in converters
        assert "float" in converters
        assert "string" in converters
        assert "boolean" in converters


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
