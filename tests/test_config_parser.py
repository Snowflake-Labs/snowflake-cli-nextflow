import tempfile
import os
import pytest
from snowflakecli.nextflow.parser import NextflowConfigParser


class TestNextflowConfigParser:
    """Test suite for NextflowConfigParser."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.parser = NextflowConfigParser()

    def test_parse_block_format(self):
        """Test parsing snowflake config in block format."""
        config_text = """
snowflake {
    computePool = 'test_pool'
    workDirStage = 'test_stage'
    stageMounts = 'input:/data/input,output:/data/output'
    enableStageMountV2 = true
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)

            assert result["computePool"] == "test_pool"
            assert result["workDirStage"] == "test_stage"
            assert result["stageMounts"] == "input:/data/input,output:/data/output"
            assert result["enableStageMountV2"] is True

    def test_parse_dot_notation(self):
        """Test parsing snowflake config in dot notation format."""
        config_text = """
snowflake.computePool = 'test_pool'
snowflake.workDirStage = 'test_stage'
snowflake.stageMounts = 'input:/data/input,output:/data/output'
snowflake.enableStageMountV2 = true
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)

            assert result["computePool"] == "test_pool"
            assert result["workDirStage"] == "test_stage"
            assert result["stageMounts"] == "input:/data/input,output:/data/output"
            assert result["enableStageMountV2"] is True

    def test_parse_profile_format_global(self):
        """Test parsing global config when profiles are present."""
        config_text = """
profiles {
    dev {
        snowflake {
            computePool = 'dev_pool'
            workDirStage = 'dev_stage'
            stageMounts = 'input:/data/input'
            enableStageMountV2 = false
        }
    }
    prod {
        snowflake.computePool = 'prod_pool'
        snowflake.workDirStage = 'prod_stage'
    }
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            # Global config should be empty when only profiles are defined
            result = self.parser.parse(temp_dir)
            assert result == {}

    def test_parse_profile_format_with_profile(self):
        """Test parsing specific profile configuration."""
        config_text = """
profiles {
    dev {
        snowflake {
            computePool = 'dev_pool'
            workDirStage = 'dev_stage'
            stageMounts = 'input:/data/input'
            enableStageMountV2 = false
        }
    }
    prod {
        snowflake.computePool = 'prod_pool'
        snowflake.workDirStage = 'prod_stage'
    }
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            # Test dev profile
            dev_result = self.parser.parse(temp_dir, "dev")
            assert dev_result["computePool"] == "dev_pool"
            assert dev_result["workDirStage"] == "dev_stage"
            assert dev_result["stageMounts"] == "input:/data/input"
            assert dev_result["enableStageMountV2"] is False

            # Test prod profile
            prod_result = self.parser.parse(temp_dir, "prod")
            assert prod_result["computePool"] == "prod_pool"
            assert prod_result["workDirStage"] == "prod_stage"
            # These should not be present in prod profile
            assert "stageMounts" not in prod_result
            assert "enableStageMountV2" not in prod_result

    def test_parse_mixed_format(self):
        """Test parsing config with mixed global and profile configurations."""
        config_text = """
// Global config
snowflake {
    computePool = 'default_pool'
    workDirStage = 'default_stage'
}

snowflake.stageMounts = 'input:/data/input'

profiles {
    test {
        snowflake.computePool = 'test_pool'
        snowflake.enableStageMountV2 = false
    }
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            # Test global config
            global_result = self.parser.parse(temp_dir)
            assert global_result["computePool"] == "default_pool"
            assert global_result["workDirStage"] == "default_stage"
            assert global_result["stageMounts"] == "input:/data/input"

            # Test profile config (should override global values)
            test_result = self.parser.parse(temp_dir, "test")
            assert test_result["computePool"] == "test_pool"  # Overridden
            assert test_result["workDirStage"] == "default_stage"  # From global
            assert test_result["stageMounts"] == "input:/data/input"  # From global
            assert test_result["enableStageMountV2"] is False  # Profile-specific

    def test_parse_boolean_values(self):
        """Test parsing boolean values correctly."""
        config_text = """
snowflake {
    enableStageMountV2 = true
}
snowflake.anotherFlag = false
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)
            assert result["enableStageMountV2"] is True
            assert result["anotherFlag"] is False

    def test_parse_plugins_config(self):
        """Test parsing plugins configuration."""
        config_text = """
plugins {
    id 'nf-snowflake@0.8.0'
    id 'nf-amazon@2.0.0'
}

snowflake {
    computePool = 'test_pool'
    workDirStage = 'test_stage'
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)

            assert "plugins" in result
            assert len(result["plugins"]) == 2

            # Find nf-snowflake plugin
            nf_snowflake_plugin = next((p for p in result["plugins"] if p["name"] == "nf-snowflake"), None)
            assert nf_snowflake_plugin is not None
            assert nf_snowflake_plugin["version"] == "0.8.0"

            # Find nf-amazon plugin
            nf_amazon_plugin = next((p for p in result["plugins"] if p["name"] == "nf-amazon"), None)
            assert nf_amazon_plugin is not None
            assert nf_amazon_plugin["version"] == "2.0.0"

    def test_parse_plugins_without_version(self):
        """Test parsing plugins configuration without version."""
        config_text = """
plugins {
    id 'nf-snowflake'
}

snowflake {
    computePool = 'test_pool'
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)

            assert "plugins" in result
            assert len(result["plugins"]) == 1

            plugin = result["plugins"][0]
            assert plugin["name"] == "nf-snowflake"
            assert plugin["version"] is None

    def test_parse_no_plugins(self):
        """Test parsing config without plugins section."""
        config_text = """
snowflake {
    computePool = 'test_pool'
    workDirStage = 'test_stage'
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)

            assert "plugins" not in result or len(result.get("plugins", [])) == 0
            assert result["computePool"] == "test_pool"
            assert result["workDirStage"] == "test_stage"

    def test_parse_missing_config_file(self):
        """Test handling of missing nextflow.config file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Don't create the config file
            with pytest.raises(FileNotFoundError, match="nextflow.config file not found"):
                self.parser.parse(temp_dir)

    def test_parse_empty_config(self):
        """Test parsing empty config file."""
        config_text = ""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)
            assert result == {}

    def test_parse_config_without_snowflake(self):
        """Test parsing config file without snowflake configuration."""
        config_text = """
params {
    input = 'data/input'
    output = 'data/output'
}

process {
    executor = 'slurm'
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            result = self.parser.parse(temp_dir)
            assert result == {}

    def test_parse_nonexistent_profile(self):
        """Test parsing with a profile that doesn't exist."""
        config_text = """
profiles {
    dev {
        snowflake.computePool = 'dev_pool'
    }
}
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "nextflow.config")
            with open(config_path, "w") as f:
                f.write(config_text)

            # Request non-existent profile should return empty config
            result = self.parser.parse(temp_dir, "nonexistent")
            assert result == {}
